//! D13 containment (ADR-274): the newest epoch of each node this member has heard of, and the halt
//! of a host that is told a newer epoch of its own node exists.
//!
//! The table of newest epochs is here ([`tables::EPOCH_NEWEST`]), and the halt's row and text.
//! The halt itself (`Engine::halt`, `Engine::halted`) is in `engine.rs`, beside the gate it takes.

use kimmy_core::{Epoch, NodeId};
use redb::{ReadableDatabase, ReadableTable};

use crate::{Engine, Result, StorageError, WriterHolder, tables};

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

    /// The rows this member relays to a peer on its vectors (ADR-274): the newest epoch it holds
    /// for each node, the newest by epoch start first, at most `cap` of them. The second value says
    /// whether the cap cut any. Read from memory, where the pending rows are as well as the
    /// durable ones, and this member's own epoch.
    pub fn newest_epoch_rows(&self, cap: usize) -> (Vec<(NodeId, Epoch)>, bool) {
        let mut rows: Vec<(NodeId, Epoch)> = self
            .newest_epochs
            .lock()
            .iter()
            .filter(|(_, epoch)| **epoch != Epoch::ZERO)
            .map(|(node, epoch)| (*node, *epoch))
            .collect();
        rows.sort_by(|a, b| {
            b.1.start_hlc().cmp(&a.1.start_hlc()).then_with(|| b.1.cmp(&a.1)).then(a.0.cmp(&b.0))
        });
        let cut = rows.len() > cap;
        rows.truncate(cap);
        (rows, cut)
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

    /// Whether this epoch's host has completed a handshake with another node (ADR-274): the durable
    /// bit that tells an epoch past the mistaken-restore guard, which has joined, from one that has
    /// not (see [`Engine::needs_restore_guard`]). Held in memory and loaded at the end of an open
    /// from a row that names this store's own epoch, so a restore, which mints a new epoch, starts
    /// without it.
    pub fn has_met_a_peer(&self) -> bool {
        self.met_peer.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether this epoch is the one the store was founded with: minted by the open that generated
    /// the node id, so no other host has ever run this node. A restore and a rewind are not.
    pub fn is_founding_epoch(&self) -> bool {
        self.founding_epoch
    }

    /// Whether the mistaken-restore guard (ADR-274) has to run before this epoch's host accepts or
    /// dials: it has completed no handshake and it is not the epoch the store was founded with.
    pub fn needs_restore_guard(&self) -> bool {
        !self.is_founding_epoch() && !self.has_met_a_peer()
    }

    /// Run `first` at this epoch's first handshake with another node, before that contact goes on
    /// and before [`Engine::has_met_a_peer`] says anyone has been met (ADR-274): contacts that
    /// arrive while it runs wait for it. If it says it failed (`false`), the epoch stays unmet and
    /// it runs again at the next contact. For an epoch that has met a peer already, never.
    pub fn before_first_met_a_peer(&self, first: impl Fn() -> bool + Send + 'static) {
        *self.before_met.lock() = Some(Box::new(first));
    }

    /// Note that this epoch's host completed a handshake with another node. Sets the flag and
    /// writes nothing, so a handshake never waits on the writer. The first one waits for what
    /// [`Engine::before_first_met_a_peer`] was given, if anything was.
    pub fn note_met_a_peer(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        if self.met_peer.load(SeqCst) {
            return;
        }
        crate::blocking(|| {
            let first = self.before_met.lock();
            // A contact that waited for the lock finds it done.
            if self.met_peer.load(SeqCst) {
                return;
            }
            if first.as_ref().is_none_or(|first| first()) {
                self.met_peer.store(true, SeqCst);
            }
        });
    }

    /// Write the bit durably, once, from outside a contact (it takes the writer). A bit that was
    /// never written is lost with the process, and the guard runs again at the next start, which is
    /// harmless.
    pub fn persist_met_a_peer(&self) -> Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        if !self.met_peer.load(SeqCst) || self.met_peer_written.load(SeqCst) {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Ddl)?;
        txn.open_table(tables::META)?.insert(tables::META_MET_PEER, self.epoch().0.as_slice())?;
        txn.commit()?;
        self.met_peer_written.store(true, SeqCst);
        Ok(())
    }

    /// Load the bit at the end of an open: in force only when the row names this store's own epoch.
    pub(crate) fn load_met_peer(&self) -> Result<()> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(tables::META)?;
        let held = meta.get(tables::META_MET_PEER)?.map(|v| v.value().to_vec());
        if held.as_deref() == Some(self.epoch().0.as_slice()) {
            self.met_peer.store(true, std::sync::atomic::Ordering::SeqCst);
            self.met_peer_written.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    /// The clock a claimed epoch's start is held against: the later of the physical clock and the
    /// logical clock's last wall time (a member whose wall clock is behind its own logical clock
    /// must not refuse every legitimate new epoch, gap 4).
    pub fn bound_clock_ms(&self) -> u64 {
        crate::physical_now_ms().max(self.logical_clock_wall_ms())
    }
}

impl Halt {
    /// The row [`tables::META_HALT`] holds: the epoch halted, a byte for whether a newer one was
    /// told of, and that epoch.
    pub(crate) fn to_row(self) -> Vec<u8> {
        let mut row = self.halted.0.to_vec();
        match self.told {
            Some(told) => {
                row.push(1);
                row.extend_from_slice(&told.0);
            }
            None => {
                row.push(0);
                row.extend_from_slice(&[0; Epoch::LEN]);
            }
        }
        row
    }

    /// The inverse of [`Self::to_row`]; `None` for a row that is not 37 bytes.
    pub(crate) fn from_row(row: &[u8]) -> Option<Self> {
        if row.len() != 2 * Epoch::LEN + 1 {
            return None;
        }
        let halted = Epoch(row[..Epoch::LEN].try_into().ok()?);
        let told = (row[Epoch::LEN] == 1)
            .then(|| row[Epoch::LEN + 1..].try_into().ok().map(Epoch))
            .flatten();
        Some(Self { halted, told })
    }

    /// What an operator reads: why this host halted and what to do. One text, shared by the refusal
    /// of a request, readiness and the log line.
    pub fn reason(&self) -> String {
        let recovery = "delete the store file, keep the containment file, and run \
                        `kimmyd restore`; the recovered host wins only with `--replace`";
        match self.told {
            Some(told) => format!(
                "it was told that a newer epoch of its node exists ({}); a host of that node now \
                 runs elsewhere, and this one, on epoch {}, serves nothing; to recover: {recovery}",
                told.to_hex(),
                self.halted.to_hex()
            ),
            None => format!(
                "the mistaken-restore guard found its node live at another address, and this host, \
                 on epoch {}, serves nothing; to recover: {recovery}",
                self.halted.to_hex()
            ),
        }
    }
}
