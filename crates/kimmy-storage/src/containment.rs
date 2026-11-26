//! D13 containment (ADR-274): the newest epoch of each node this member has heard of, and the halt
//! of a host that is told a newer epoch of its own node exists.
//!
//! The table of newest epochs is here ([`tables::EPOCH_NEWEST`]); the halt is still the scaffold of
//! the tests commits (`halt` and `halted` do nothing), and the code commits replace them.

use kimmy_core::{Epoch, NodeId};
use redb::{ReadableDatabase, ReadableTable};

use crate::{Engine, Result, StorageError, tables};

/// What a halted host knows of why: the epoch it halts (its own), and the newer epoch of its node
/// it was told of. `told` is `None` for a halt by the mistaken-restore guard, which has no newer
/// epoch to name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Halt {
    pub halted: Epoch,
    pub told: Option<Epoch>,
}

/// The epoch in a row's value.
fn epoch_of(bytes: &[u8]) -> Result<Epoch> {
    <[u8; Epoch::LEN]>::try_from(bytes)
        .map(Epoch)
        .map_err(|_| StorageError::Corrupt("an epoch_newest row is not 18 bytes".into()))
}

/// Raise `node`'s row to `epoch` inside `txn`, if `epoch` is above what the row holds. Returns
/// whether it rose.
pub(crate) fn raise_row(txn: &redb::WriteTransaction, node: NodeId, epoch: Epoch) -> Result<bool> {
    let mut table = txn.open_table(tables::EPOCH_NEWEST)?;
    let key = node.to_bytes();
    let held = match table.get(key.as_slice())? {
        Some(value) => epoch_of(value.value())?,
        None => Epoch::ZERO,
    };
    if epoch <= held {
        return Ok(false);
    }
    table.insert(key.as_slice(), epoch.0.as_slice())?;
    Ok(true)
}

/// Write the rows [`Engine::note_epoch`] raised and has not yet written into `txn`, in the
/// transaction of the next write this member commits. Called by `WriteTxn::commit`, so a belief
/// never needs a transaction of its own. The flag is cleared **first**, then the rows are taken: a
/// row noted in between sets the flag again and is written by the next commit, where the other
/// order could clear a flag that a concurrent note had just set. If a row cannot be written it is
/// put back; if the commit itself fails after this returns, the rows are not (memory still holds
/// them, and a restart relearns them from a handshake).
pub(crate) fn flush_pending_epochs(engine: &Engine, txn: &redb::WriteTransaction) -> Result<()> {
    if !engine.epochs_pending_flag.swap(false, std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    let pending = std::mem::take(&mut *engine.epochs_pending.lock());
    for (node, epoch) in &pending {
        if let Err(e) = raise_row(txn, *node, *epoch) {
            let mut put_back = engine.epochs_pending.lock();
            for (node, epoch) in pending {
                let held = put_back.entry(node).or_insert(Epoch::ZERO);
                *held = (*held).max(epoch);
            }
            engine.epochs_pending_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(e);
        }
    }
    Ok(())
}

impl Engine {
    /// The newest epoch of `node` this member has recorded, from memory: `Epoch::ZERO` for a node
    /// it has none for.
    fn newest_epoch_row(&self, node: NodeId) -> Epoch {
        self.newest_epochs.lock().get(&node).copied().unwrap_or(Epoch::ZERO)
    }

    /// Load the table into memory at the end of an open.
    pub(crate) fn load_newest_epochs(&self) -> Result<()> {
        let txn = self.db().begin_read()?;
        let mut loaded = std::collections::BTreeMap::new();
        // The table is created by the first row written to it, never at the open: a store that has
        // not met another epoch has the file layout it had before the table existed, and this
        // member's own epoch is known from its epoch row and held in memory, never written here.
        match txn.open_table(tables::EPOCH_NEWEST) {
            Ok(table) => {
                for row in table.iter()? {
                    let (key, value) = row?;
                    let node = <[u8; 16]>::try_from(key.value()).map(NodeId::from_bytes).map_err(
                        |_| StorageError::Corrupt("an epoch_newest key is not 16 bytes".into()),
                    )?;
                    loaded.insert(node, epoch_of(value.value())?);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }
        let own = loaded.entry(self.node_id()).or_insert(Epoch::ZERO);
        *own = (*own).max(self.epoch());
        *self.newest_epochs.lock() = loaded;
        Ok(())
    }

    /// The newest epoch of `node` this member knows: the larger of the recorded row and the node's
    /// own origins in the version vectors, read at each check (finding 7: a peer's entries of an
    /// epoch this member never saw in a handshake still count). `Epoch::ZERO` for a node it has
    /// never heard of.
    pub fn newest_epoch(&self, node: NodeId) -> Result<Epoch> {
        let mut newest = self.newest_epoch_row(node);
        for vector in [self.version_vector()?, self.witnessed_vector()?] {
            for (origin, _) in vector.iter() {
                if origin.node == node && origin.epoch > newest {
                    newest = origin.epoch;
                }
            }
        }
        Ok(newest)
    }

    /// Record that `epoch` is an epoch of `node`: the row becomes the larger of the two, and never
    /// falls. The caller has believed the claim already (a verified handshake, within the clock
    /// bound); nothing here bounds it.
    ///
    /// **It opens no transaction and never waits for the writer** (a handshake must not stall
    /// behind a held writer, and under `coalesced` an ordinary commit would wait at the barrier,
    /// whose leader takes the gate): the row is raised in memory and in a pending set, and the
    /// next write transaction this member commits, of any holder, writes it. A member that
    /// restarts before its next write forgets an epoch it had only heard in a handshake and
    /// relearns it at its next contact. A claim that does not raise the row does nothing.
    pub fn note_epoch(&self, node: NodeId, epoch: Epoch) -> Result<()> {
        {
            let mut rows = self.newest_epochs.lock();
            let held = rows.entry(node).or_insert(Epoch::ZERO);
            if epoch <= *held {
                return Ok(());
            }
            *held = epoch;
        }
        {
            let mut pending = self.epochs_pending.lock();
            let held = pending.entry(node).or_insert(Epoch::ZERO);
            *held = (*held).max(epoch);
        }
        self.epochs_pending_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// The clock a claimed epoch's start is held against: the later of the physical clock and the
    /// logical clock's last wall time (a member whose wall clock is behind its own logical clock
    /// must not refuse every legitimate new epoch, gap 4).
    pub fn bound_clock_ms(&self) -> u64 {
        crate::physical_now_ms().max(self.logical_clock_wall_ms())
    }

    /// Halt this host: it was told that `told`, a newer epoch of its own node, exists. The mirror
    /// is set first, then the writer gate is taken, then the halt row (naming this member's own
    /// epoch) is committed durably. **Scaffold: does nothing yet.**
    pub fn halt(&self, _told: Epoch) -> Result<()> {
        Ok(())
    }

    /// The halt this store holds for its own epoch, if any: `None` when it was never halted, and
    /// `None` when the row names another epoch (a backup of a halted host, restored). **Scaffold:
    /// always `None` yet.**
    pub fn halted(&self) -> Option<Halt> {
        None
    }
}
