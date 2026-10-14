//! Refusal hints (B3 half 2, design 3.5): a definition this member refused is
//! remembered, so that a difference between two members' schemas that comes of
//! the refusal is **explained** and never silent.
//!
//! A refusal today is a counter and a log line (ADR-123 and ADR-139: a
//! definition this build cannot apply, a rival it cannot arbitrate), and nothing
//! records which definition was refused. A definition digest finding for it is
//! correct and unrepairable by design; the hint makes it *explained*.
//!
//! **A hint, never a mask.** The hint is a row in `META`, one per
//! `(collection, kind, name)`, holding the stamp of the refused entry, the build
//! that refused it and the hash of the refused definition. It is **never
//! trusted without being re-derived**: at use it is valid only if the
//! definitions it explains still stand as it says (the other member's current
//! hash for the name equals the stored one, the stamp is above the standing
//! life's floor). A build that can now apply the definition, a rollback, a
//! restore and a recreated collection therefore cannot leave a stale row
//! masking anything.
//!
//! **Kinds.** A hint is of one of four kinds: a refused index, a refused vector
//! configuration, and the two opposite shapes (a peer's drop of an index, or a
//! turning-off of the vector configuration, that this member holds and could not
//! apply). The kind says which definition it is about, so an index named like
//! anything cannot be mistaken for the configuration, and which shape its
//! validity takes.
//!
//! `META` and not a table of its own, so a backup carries it (`META` rows are
//! copied whatever their key) and a build that does not know it never reads the
//! rows.

use kimmy_core::{CollectionId, Stamp};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, WriteTransaction};

use crate::codec;
use crate::definition_digest::{DefinitionHash, DefinitionKey, DefinitionKind};
use crate::engine::{Engine, WriterHolder};
use crate::error::Result;
use crate::tables;

/// The key prefix of a collection's hints is `ddl_refused:<id>:`.
const PREFIX: &str = "ddl_refused:";

/// What was refused, and in which shape.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HintKind {
    /// An index definition this member could not apply.
    Index,
    /// A vector configuration this member could not apply.
    Vector,
    /// A peer's drop of an index this member holds and could not apply: the
    /// opposite shape, where this member is the one still holding the
    /// definition.
    DropIndex,
    /// The same for a peer's turning off of the vector configuration.
    DropVector,
}

impl HintKind {
    /// Which kind of definition this is about.
    pub fn definition(self) -> DefinitionKind {
        match self {
            Self::Index | Self::DropIndex => DefinitionKind::Index,
            Self::Vector | Self::DropVector => DefinitionKind::Vector,
        }
    }

    /// Whether this is the opposite shape: this member holds what a peer dropped.
    pub fn is_drop(self) -> bool {
        matches!(self, Self::DropIndex | Self::DropVector)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Vector => "vector",
            Self::DropIndex => "drop_index",
            Self::DropVector => "drop_vector",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "index" => Some(Self::Index),
            "vector" => Some(Self::Vector),
            "drop_index" => Some(Self::DropIndex),
            "drop_vector" => Some(Self::DropVector),
            _ => None,
        }
    }
}

/// One remembered refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefusalHint {
    pub collection: CollectionId,
    pub kind: HintKind,
    /// The index's name; empty for the vector configuration.
    pub name: String,
    /// The stamp of the entry that was refused.
    pub stamp: Stamp,
    /// The build that refused it (`CARGO_PKG_VERSION`), for the operator.
    pub version: String,
    /// The hash of the refused definition (of the held one, for a `Drop`).
    pub hash: DefinitionHash,
}

impl RefusalHint {
    /// A hint for `name`, made by this build.
    pub fn new(
        collection: CollectionId,
        kind: HintKind,
        name: impl Into<String>,
        stamp: Stamp,
        hash: DefinitionHash,
    ) -> Self {
        let name = match kind.definition() {
            DefinitionKind::Index => name.into(),
            DefinitionKind::Vector => String::new(),
        };
        Self { collection, kind, name, stamp, version: env!("CARGO_PKG_VERSION").to_string(), hash }
    }

    /// The definition this hint is about.
    pub fn key(&self) -> DefinitionKey {
        DefinitionKey { kind: self.kind.definition(), name: self.name.clone() }
    }

    /// The `META` key: the id in decimal, the kind, then the name (empty for a
    /// vector configuration), which may contain `:` and so is parsed last.
    fn row_key(&self) -> String {
        format!("{PREFIX}{}:{}:{}", self.collection.0, self.kind.as_str(), self.name)
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(codec::STAMP_LEN + 16 + self.version.len());
        out.extend_from_slice(&codec::oplog_key(&self.stamp));
        out.extend_from_slice(&self.hash.to_be_bytes());
        out.extend_from_slice(self.version.as_bytes());
        out
    }

    fn decode(collection: CollectionId, key: &str, value: &[u8]) -> Option<Self> {
        let rest = key.strip_prefix(PREFIX)?.strip_prefix(&format!("{}:", collection.0))?;
        let (kind, name) = rest.split_once(':')?;
        let kind = HintKind::parse(kind)?;
        if value.len() < codec::STAMP_LEN + 16 {
            return None;
        }
        let stamp = codec::decode_oplog_key(&value[..codec::STAMP_LEN]).ok()?;
        let hash = u128::from_be_bytes(
            value[codec::STAMP_LEN..codec::STAMP_LEN + 16].try_into().expect("16 bytes"),
        );
        let version = String::from_utf8_lossy(&value[codec::STAMP_LEN + 16..]).into_owned();
        Some(Self { collection, kind, name: name.to_string(), stamp, version, hash })
    }
}

/// At most one warning per ten minutes for a hint row that cannot be read.
fn warn_of_an_unreadable_hint(key: &str) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |since| since.as_secs().max(1));
    let last = LAST.load(Ordering::Relaxed);
    if (last == 0 || now.saturating_sub(last) >= 600)
        && LAST.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok()
    {
        tracing::warn!(
            key,
            "a refusal hint row could not be read and is ignored: it explains nothing, and a \
             hint written under the same key replaces it"
        );
    }
}

/// The range of a collection's hint keys: `ddl_refused:<id>:` up to the first
/// key after it (`;` follows `:`).
fn range_of(collection: CollectionId) -> (String, String) {
    (format!("{PREFIX}{}:", collection.0), format!("{PREFIX}{};", collection.0))
}

impl Engine {
    /// Remember a refusal, in a transaction of its own. A newer stamp replaces
    /// an older one for the same `(collection, kind, name)`, an older never
    /// replaces a newer, and a hint identical to the one held (stamp, build and
    /// hash) is no write at all.
    ///
    /// A failure is the caller's to treat as the batch's: the entry is not
    /// witnessed and is served again, never swallowed.
    pub fn record_refusal_hint(&self, hint: &RefusalHint) -> Result<()> {
        #[cfg(any(test, feature = "test-hooks"))]
        if self.hint_write_fails.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(crate::StorageError::Corrupt(
                "a hint write failure injected by a test".into(),
            ));
        }
        // A refusal met again (a permanent one is re-met on every repair or
        // catch-up round) finds the same row already there: nothing is committed,
        // and the writer is not taken, so a refusal is not a commit trickle
        // (ADR-125). Two writers that both miss it write the same row twice,
        // which is harmless.
        if self.refusal_hints(hint.collection)?.contains(hint) {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let wrote = Self::record_refusal_hint_in(&txn, hint)?;
        if wrote {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        Ok(())
    }

    /// Make the next [`Self::record_refusal_hint`] fail (a test hook: a refusal
    /// that cannot be remembered must fail the batch and be served again).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_next_hint_write_for_test(&self) {
        self.hint_write_fails.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// [`Self::record_refusal_hint`] in a transaction the caller holds.
    /// Whether it wrote.
    pub(crate) fn record_refusal_hint_in(
        txn: &WriteTransaction,
        hint: &RefusalHint,
    ) -> Result<bool> {
        let key = hint.row_key();
        let mut meta = txn.open_table(tables::META)?;
        if let Some(held) = meta.get(key.as_str())?
            && let Some(existing) = RefusalHint::decode(hint.collection, &key, held.value())
            && existing.stamp > hint.stamp
        {
            return Ok(false);
        }
        meta.insert(key.as_str(), hint.encode().as_slice())?;
        Ok(true)
    }

    /// The hints held for `collection`, in a read transaction the caller holds.
    ///
    /// A row this build cannot decode (a kind a later build wrote and the store
    /// was rolled back past, or damage) is **skipped and warned about**, never an
    /// error: a hint is only ever a reason to explain a difference, so losing one
    /// costs an explanation, where failing here would fail every definition
    /// answer and every comparison on this member, and the replication batch
    /// whose refusal wants to write a hint.
    pub(crate) fn refusal_hints_in(
        txn: &ReadTransaction,
        collection: CollectionId,
    ) -> Result<Vec<RefusalHint>> {
        let meta = txn.open_table(tables::META)?;
        let (start, end) = range_of(collection);
        let mut out = Vec::new();
        for row in meta.range(start.as_str()..end.as_str())? {
            let (key, value) = row?;
            match RefusalHint::decode(collection, key.value(), value.value()) {
                Some(hint) => out.push(hint),
                None => warn_of_an_unreadable_hint(key.value()),
            }
        }
        Ok(out)
    }

    /// The hints held for `collection`.
    pub fn refusal_hints(&self, collection: CollectionId) -> Result<Vec<RefusalHint>> {
        let txn = self.db().begin_read()?;
        Self::refusal_hints_in(&txn, collection)
    }

    /// Remove every hint of `collection`, in the caller's transaction: the
    /// single place a collection's rows go (the purge) and a `DropIndex` call
    /// this. Returns how many went.
    pub(crate) fn clear_refusal_hints_in(
        txn: &WriteTransaction,
        collection: CollectionId,
    ) -> Result<usize> {
        let mut meta = txn.open_table(tables::META)?;
        let (start, end) = range_of(collection);
        let keys: Vec<String> = meta
            .range(start.as_str()..end.as_str())?
            .map(|row| row.map(|(key, _)| key.value().to_string()))
            .collect::<std::result::Result<_, _>>()?;
        for key in &keys {
            meta.remove(key.as_str())?;
        }
        Ok(keys.len())
    }
}

impl Engine {
    /// Remember that this member refused to apply an index definition a peer
    /// sent (a replicated `CreateIndex` or a snapshot's index it cannot build).
    /// The hash is of the definition as the peer sent it, which is what the
    /// peer's own stored index hashes to: both are `as_stored` before they are
    /// serialized, and the JSON round trip of the stored form keeps every type.
    pub(crate) fn note_refused_index(
        &self,
        collection: CollectionId,
        index: &kimmy_core::IndexMeta,
        stamp: Stamp,
    ) -> Result<()> {
        self.record_refusal_hint(&RefusalHint::new(
            collection,
            HintKind::Index,
            index.name.clone(),
            stamp,
            crate::definition_digest::index_hash(index),
        ))
    }

    /// The same for a vector configuration this member refused. `config` is the
    /// configuration refused; for a refused **turning off** it is `None`, and the
    /// hint is of the opposite shape: this member holds a configuration the peer
    /// turned off and could not turn off here.
    pub(crate) fn note_refused_vector(
        &self,
        db: &str,
        name: &str,
        config: Option<&kimmy_core::VectorConfig>,
        stamp: Stamp,
    ) -> Result<()> {
        let collection = CollectionId::derive(db, name);
        let (kind, hash) = match config {
            Some(config) => (HintKind::Vector, crate::definition_digest::vector_hash(config)),
            None => match self.get_collection(db, name).ok().and_then(|meta| meta.vector) {
                Some(held) => (HintKind::DropVector, crate::definition_digest::vector_hash(&held)),
                // Nothing held, so nothing to explain.
                None => return Ok(()),
            },
        };
        self.record_refusal_hint(&RefusalHint::new(collection, kind, "", stamp, hash))
    }

    /// The same for a peer's drop of an index this member holds and could not
    /// drop: the opposite shape, and the hash is of the index this member holds.
    pub(crate) fn note_refused_index_drop(
        &self,
        db: &str,
        collection: &str,
        index: &str,
        stamp: Stamp,
    ) -> Result<()> {
        let Some(held) =
            self.get_collection(db, collection).ok().and_then(|meta| meta.index(index).cloned())
        else {
            return Ok(());
        };
        self.record_refusal_hint(&RefusalHint::new(
            CollectionId::derive(db, collection),
            HintKind::DropIndex,
            index,
            stamp,
            crate::definition_digest::index_hash(&held),
        ))
    }

    /// Remove the hints of `collection` about one definition, of both shapes, in
    /// the caller's transaction. A definition this member now holds, has applied
    /// again or has dropped is no longer one it refused or holds under a refusal,
    /// and a row that outlived the change would explain a later, different loss
    /// of the same definition as the refusal that is long since mended.
    pub(crate) fn clear_refusal_hints_for_in(
        txn: &WriteTransaction,
        collection: CollectionId,
        definition: &DefinitionKey,
    ) -> Result<usize> {
        let mut meta = txn.open_table(tables::META)?;
        let mut cleared = 0usize;
        let kinds = match definition.kind {
            DefinitionKind::Index => [HintKind::Index, HintKind::DropIndex],
            DefinitionKind::Vector => [HintKind::Vector, HintKind::DropVector],
        };
        for kind in kinds {
            let key = format!("{PREFIX}{}:{}:{}", collection.0, kind.as_str(), definition.name);
            if meta.remove(key.as_str())?.is_some() {
                cleared += 1;
            }
        }
        Ok(cleared)
    }
}
