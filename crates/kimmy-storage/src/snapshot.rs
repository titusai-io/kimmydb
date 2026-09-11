//! Catching up a peer whose history has been collected, and repairing one
//! collection on a peer that holds a hole in it.
//!
//! Anti-entropy works by replaying oplog entries, which only reaches back as
//! far as `oplog_retention_secs`. A node joining a cluster older than that
//! window asks for history nobody still has: it receives nothing it can apply,
//! its version vector never advances, and it retries forever. With the default
//! retention that is *any* node added to a cluster more than a day old — which
//! is to say, adding a node to a running cluster.
//!
//! So when a peer asks from below the horizon, it is sent **current state**
//! instead of history: the sender's collection tombstones first, then the
//! collection definitions, then documents in pages, then the sender's coverage.
//! The receiver is caught up when it has all of them.
//!
//! The tombstones come first and that order is load-bearing — a page carrying
//! both a drop and the definition of the life that replaced it must bury before
//! it creates, or the definition is a no-op against the name still standing and
//! the drop then takes it (ADR-162).
//!
//! # Why documents arrive as oplog entries
//!
//! Each snapshot document is applied through the same `apply_remote_in_txn`
//! that replication uses, carrying the stamp the document actually has. That
//! is not a trick to save code — it is what makes the result *correct*:
//!
//! - last-writer-wins still decides, so a receiver that already holds a newer
//!   version of a document keeps it;
//! - secondary indexes are maintained, so the new node can answer index-backed
//!   queries;
//! - unique violations are detected and reported, rather than being smuggled in
//!   through a side door that skips the check.
//!
//! What that path does *not* decide is whether a document belongs to a life of
//! its collection this node has already buried: `apply_remote_in_txn` compares
//! the document against the one standing under its id, and a document from a
//! collection that was dropped has nothing there to lose to. So a page's
//! documents are put to [`Engine::is_history`] first — the predicate the
//! entries path applies, in the one place it is written — and a definition is
//! not recreated over a tombstone this node holds. A snapshot is the sender's
//! current state, not an instruction, and a sender that has not applied a drop
//! yet still serves the collection.
//!
//! Collection definitions are *not* logged. One travels with the incarnation
//! it began at (`CollectionState::created`), which is what a drop is judged
//! against and what this node advertises once it holds it — but not with a
//! place in this node's own history, which it has none of. Inventing one would
//! be worse than omitting it.
//!
//! # A page is one transaction
//!
//! Every document of a page goes into one write transaction, committed once,
//! and not at all when every document on the page lost last-writer-wins here
//! — a page a member already holds must not cost an fsync (ADR-152). The
//! first form of this restore applied one document per transaction, 512 per
//! page, which on a member that already held the documents was hundreds of
//! thousands of transactions taken and released against the single writer for
//! a repair that changed nothing. What has to follow the commit — recording
//! unique violations, publishing to change streams — is done after it, in
//! document order, exactly as a replicated batch's run does (ADR-119).
//! Collection definitions still go through the DDL path with transactions of
//! their own, for the reason ADR-119 gives for a run ending at a schema change.
//!
//! # A snapshot can be of one collection
//!
//! A repair (ADR-148) is planned for *one* collection, and a snapshot scoped
//! to it walks only that collection's key range and sends only its definition
//! — or, when the sender has since dropped it, the drop's stamp, on every page
//! and not the first alone, because a repair runs for as long as the
//! collection takes and the drop lands where it lands. The receiver records
//! the tombstone and the entries it was stopped at become history rather than
//! a stop that repeats. Where the receiver still holds the
//! incarnation that stamp names, it goes with the tombstone: keeping it would
//! leave this node advertising a collection the cluster has agreed is deleted,
//! and re-seeding it onto the members that applied the drop. The
//! whole-database snapshot remains what a member below a peer's retention
//! horizon is served; a scoped one grants no coverage, because coverage is per
//! origin and cannot be scoped to a collection.
//!
//! # A snapshot resumes, and the coverage it grants
//!
//! [`SnapshotProgress`] is what the receiver carries between pages and, since
//! ADR-152, between rounds: a page applied stays applied, and the next round
//! asks for the page after it rather than page one. The coverage a completed
//! whole-database snapshot grants is the sender's vector **as served with the
//! first page**, which the sender reads before that page's documents, and it
//! is recorded only when the final page lands. The first page's, not the
//! last's: a document the sender wrote *behind* the cursor while the snapshot
//! ran is not in the snapshot, and the final page's vector covers its entry —
//! adopting that would witness past an entry this node was never served, the
//! hole ADR-148 exists to close. Every document committed before the first
//! page's vector was read is at or ahead of the cursor when its range is
//! walked, so the first page's vector never claims a document the snapshot
//! did not carry; whatever the sender wrote after it is above that vector and
//! the next round pulls it from the oplog, which is where it still is.

use std::fmt;

use kimmy_core::{
    CollectionId, DocId, Hlc, IndexMeta, NodeId, OpKind, OplogEntry, Stamp, VectorConfig,
    VersionVector,
};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::codec;
use crate::docs::RemoteApplied;
use crate::engine::{Engine, Position, WriterHolder};
use crate::error::Result;
use crate::meta::CollectionMeta;
use crate::sync::{Memo, Pending};
use crate::tables;

/// Documents per page.
///
/// Bounded so a large collection crosses the wire in several frames rather than
/// one the receiver may not have the memory to hold.
pub const SNAPSHOT_PAGE: usize = 512;

/// Where a snapshot left off, so it can resume rather than restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCursor {
    /// `CollectionId`, not a bare `u64`: BSON has no unsigned 64-bit type, and
    /// half of all derived ids sit above `i64::MAX`. A bare integer here made
    /// every snapshot page naming such a collection unencodable, so a node
    /// beyond a peer's retention horizon — the only case a snapshot serves —
    /// could never catch up. Same defect as ADR-031's, on the other wire type.
    pub collection: CollectionId,
    /// Encoded document key; the next page starts strictly after it.
    pub after_key: Vec<u8>,
}

/// `collection/hex-key`, for a log line that says where a snapshot stands.
impl fmt::Display for SnapshotCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.collection)?;
        for byte in &self.after_key {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A collection's definition, without its documents.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CollectionState {
    pub db: String,
    pub name: String,
    pub indexes: Vec<IndexMeta>,
    pub vector: Option<VectorConfig>,
    /// The stamp of the create that produced this incarnation *at its
    /// origin* (`CollectionMeta::created`), which the receiver restores it
    /// under and judges its own tombstone against.
    ///
    /// Without it a restored collection's `created` was the receiver's clock
    /// at apply time — later than the incarnation truly began, so a drop that
    /// legitimately followed the create was judged to predate it and ignored
    /// (`sync::apply_ddl`), and the incarnation advertised to a peer's
    /// divergence check looked newer than the tombstone that peer holds for
    /// it. Both are the same resurrection through different doors.
    ///
    /// An `Option`, and optional on the wire, because a sender that predates
    /// the field sends no stamp at all: that is a fact the receiver has to be
    /// able to see, and `Hlc::ZERO` would make it look like an incarnation
    /// older than every tombstone rather than like an absence. What the
    /// receiver does with the absence is on `Engine::restore_collection`.
    #[serde(default)]
    pub created: Option<Hlc>,
}

impl From<CollectionMeta> for CollectionState {
    fn from(meta: CollectionMeta) -> Self {
        CollectionState {
            db: meta.db,
            name: meta.name,
            indexes: meta.indexes,
            vector: meta.vector,
            created: Some(meta.created),
        }
    }
}

/// One document, as it currently stands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDoc {
    /// See [`SnapshotCursor::collection`] for why this is not a `u64`.
    pub collection: CollectionId,
    pub id: DocId,
    pub stamp: Stamp,
    /// `None` for a tombstone, which travels so a delete is not undone by a
    /// peer that still holds the document.
    ///
    /// Binary rather than serde's default array-of-int32s, for the reason on
    /// [`kimmy_core::OplogEntry::body`]. It matters at least as much here: a
    /// snapshot is a whole collection by definition, so this is the largest
    /// thing the cluster ever sends.
    #[serde(with = "serde_bytes")]
    pub body: Option<Vec<u8>>,
}

/// What applying one snapshot page did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotApplied {
    /// Documents the page wrote — those that won last-writer-wins here.
    pub applied: usize,
    /// Index definitions on the page this node's documents refused and it
    /// skipped (ADR-123). Folded into `SyncOutcome::ddl_refused` by the
    /// transport, so a refusal reached through a snapshot is counted where
    /// one reached through the oplog is.
    pub ddl_refused: usize,
    /// Documents on the page that address a life of their collection this
    /// node has buried — `Engine::is_history`, the predicate the entries
    /// path applies — plus those whose collection the page tried and failed
    /// to recreate over a tombstone held here.
    ///
    /// Logged by the transport rather than folded into the round's outcome:
    /// it is what tells a repair that wrote nothing because everything on it
    /// was already buried from one that is stalling, and reading it as the
    /// round's `superseded` would count on this route only part of what that
    /// name counts on the entries path (a last-writer-wins loser is not
    /// counted here — a page a member already holds is the ordinary case for
    /// a repair, and it is not news).
    pub superseded: usize,
}

/// One page of a snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPage {
    /// Collection definitions. Sent with the first page only: every
    /// collection for a whole-database snapshot, the one collection for a
    /// scoped one — or none, when the sender no longer holds it.
    pub collections: Vec<CollectionState>,
    pub documents: Vec<SnapshotDoc>,
    /// Where to resume, or `None` when the snapshot is complete.
    pub next: Option<SnapshotCursor>,
    /// The sender's coverage, read before this page's documents. A
    /// whole-database snapshot grants the vector served with its *first*
    /// page, and only once the final page has landed — see the module docs
    /// for why that vector and not the last one. A scoped snapshot grants
    /// none.
    pub versions: VersionVector,
    /// For a snapshot of one collection the sender no longer holds: the
    /// stamp of its drop, when a tombstone still records it.
    ///
    /// On **every** page of a scoped snapshot, not the first alone: the drop
    /// can land between two pages of a repair that runs for minutes, and a
    /// resumed page that could not carry it left the receiver holding a
    /// partial copy of the dropped incarnation and calling the pull complete.
    /// A whole-database snapshot carries its drops in `dropped_collections`
    /// instead: this field names one collection, and that route has to deny
    /// many.
    ///
    /// Optional on the wire, because a sender that predates the field never
    /// writes it (ADR-152). Nothing about the field changed to carry it on
    /// later pages; what changed is when a sender fills it, and a receiver
    /// that predates *that* reads a resumed page's drop exactly as it reads a
    /// first page's, because its own gate is on the snapshot being scoped and
    /// never was on the page being the first.
    #[serde(default)]
    pub dropped: Option<Stamp>,
    /// For a whole-database snapshot: every collection tombstone the sender
    /// holds, so the page conveys **absence** and not only presence.
    ///
    /// `collections` says what the sender still has. Nothing on the page used
    /// to say what it had and deleted, so a collection the receiver held and
    /// the sender had dropped survived the snapshot — and, because completing
    /// one grants the receiver coverage of the sender's history, the
    /// `DropCollection` entry was never served to it afterwards either. The
    /// collection stayed live, served and writable, on one member only, with
    /// nothing reporting the disagreement. See ADR-162.
    ///
    /// On **every** page, not the first alone, for the reason ADR-152 found
    /// for the scoped route: a drop can land between two pages of a snapshot
    /// that runs for minutes, and a page that could not carry it would leave
    /// the receiver holding a collection the sender deleted while the transfer
    /// was in flight. The list is bounded by `tombstone_retention_secs`, which
    /// is what makes re-sending it affordable.
    ///
    /// Defaulted on the wire, so a sender that predates the field writes
    /// nothing and a receiver that predates it ignores what it cannot read —
    /// no negotiation, and a mixed-version cluster simply keeps the old
    /// behaviour until both ends have rolled.
    ///
    /// **A receiver honours an entry only where `versions` covers its stamp**
    /// — a peer may only deny what its own coverage names (ADR-163). Both
    /// halves of a pair come off the wire here, so without that gate one
    /// malformed entry destroys an arbitrary collection unrecoverably.
    #[serde(default)]
    pub dropped_collections: Vec<(CollectionId, Stamp)>,
}

/// Where a snapshot pull stands on the receiver.
///
/// Carried between pages and, since ADR-152, between rounds: a page applied
/// stays applied, and the next round asks for the page after it rather than
/// page one. Holds the coverage a completed whole-database snapshot grants —
/// the sender's vector as served with the first page — so that a snapshot
/// resumed rounds later still adopts the one vector every document it carried
/// is at or below.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotProgress {
    /// The one collection the snapshot is of, or `None` for the whole
    /// database.
    scope: Option<CollectionId>,
    /// Where the next page begins. `None` before the first page has been
    /// applied, and again — with `complete` set — after the last.
    after: Option<SnapshotCursor>,
    /// The sender's vector as served with the first page; what the snapshot
    /// grants once complete. `None` for a scoped snapshot, which grants
    /// nothing, and before the first page.
    granted: Option<VersionVector>,
    /// Pages applied so far, across every round.
    pages: usize,
    /// Documents written so far — those that won last-writer-wins here.
    documents: usize,
    /// Whether the final page has been applied.
    complete: bool,
}

impl SnapshotProgress {
    /// A whole-database snapshot, not yet started: what a member below a
    /// peer's retention horizon pulls.
    pub fn whole_database() -> Self {
        Self::new(None)
    }

    /// A snapshot of one collection, not yet started: what a repair pulls
    /// (ADR-148).
    pub fn of_collection(id: CollectionId) -> Self {
        Self::new(Some(id))
    }

    fn new(scope: Option<CollectionId>) -> Self {
        SnapshotProgress {
            scope,
            after: None,
            granted: None,
            pages: 0,
            documents: 0,
            complete: false,
        }
    }

    /// This progress as it stands after `page` has been applied, `applied`
    /// documents of it having been written here.
    ///
    /// Split out because the record persisted in the page's own transaction
    /// has to be the state the page LEAVES, and that transaction commits
    /// before the caller's `progress` is advanced. One function so the two
    /// cannot drift: a persisted cursor that disagreed with the in-memory one
    /// would resume somewhere neither had been.
    fn advanced(&self, page: &SnapshotPage, applied: usize) -> Self {
        let mut next = self.clone();
        next.after = page.next.clone();
        next.pages += 1;
        next.documents += applied;
        next.complete = page.next.is_none();
        next
    }

    /// The collection the snapshot is scoped to, if any.
    pub fn scope(&self) -> Option<CollectionId> {
        self.scope
    }

    /// Where the next page begins; what to ask the sender for.
    pub fn after(&self) -> Option<&SnapshotCursor> {
        self.after.as_ref()
    }

    /// Pages applied so far.
    pub fn pages(&self) -> usize {
        self.pages
    }

    /// Documents written so far.
    pub fn documents(&self) -> usize {
        self.documents
    }

    /// Whether the final page has been applied.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

impl Engine {
    /// Produce one page of a snapshot of current state: the whole database,
    /// or — for a repair — the one collection `scope` names.
    pub fn snapshot_page(
        &self,
        after: Option<SnapshotCursor>,
        scope: Option<CollectionId>,
    ) -> Result<SnapshotPage> {
        // Read before the documents, on every page. The receiver adopts the
        // first page's vector once the snapshot completes, and the order is
        // what makes that safe: a document committed before this read is at
        // or ahead of the cursor when its range is walked, so the vector
        // never claims a document the snapshot does not carry. Read after
        // the documents, as it was, it could name an entry for a document
        // written behind the cursor in between.
        let versions = self.version_vector()?;

        // Definitions ride the first page, so the receiver can create the
        // collections before any document needs one. For a scoped snapshot
        // that is the one collection — or, when it is gone here, its drop,
        // and **that rides every page, resumed pages included**.
        //
        // A repair runs for as long as the collection takes, and a drop
        // lands where it lands. Sent on the first page alone, a drop between
        // pages reached the receiver as an empty page with no definition, no
        // documents, no drop and no cursor — indistinguishable from a
        // snapshot that had simply run out — so the pull reported itself
        // complete and the receiver kept a partial copy of the incarnation
        // the cluster had just agreed to delete, with no tombstone of its
        // own. Measured on a live-shaped fixture, not inferred: 512 documents
        // applied, sender drops, resumed page empty, receiver still holding
        // 512. That receiver is then the member that re-seeds the collection
        // onto the ones that applied the drop, which is the finding, and the
        // window is permanent exactly when the drop entry has already aged
        // out of the oplog — which is when a repair is running at all.
        //
        // The tombstone is not read on its own: a collection recreated after
        // a drop keeps the tombstone that floored it, so "a tombstone exists"
        // does not mean "the collection is gone", and a page that carried one
        // regardless would tell a receiver holding an older incarnation to
        // destroy the copy this very snapshot is filling.
        let (collections, dropped) = match scope {
            Some(id) => match self.collection_by_id(id)? {
                Some(meta) if after.is_none() => (vec![CollectionState::from(meta)], None),
                Some(_) => (Vec::new(), None),
                None => (Vec::new(), self.collection_dropped_at(id)?),
            },
            None if after.is_none() => (self.collection_states()?, None),
            None => (Vec::new(), None),
        };
        // What the sender has DELETED, for the whole-database route only: the
        // scoped route names its one collection in `dropped` above. On every
        // page, so a drop that lands mid-transfer is carried (ADR-152's lesson,
        // ADR-162's application of it).
        let dropped_collections =
            if scope.is_none() { self.collections_dropped()? } else { Vec::new() };

        let (documents, next) = self.snapshot_documents(after, scope)?;
        Ok(SnapshotPage { collections, documents, next, versions, dropped, dropped_collections })
    }

    fn collection_states(&self) -> Result<Vec<CollectionState>> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;

        let mut out = Vec::new();
        for row in collections.iter()? {
            let (_, value) = row?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            out.push(CollectionState::from(meta));
        }
        Ok(out)
    }

    /// Read up to [`SNAPSHOT_PAGE`] documents, resuming after `cursor`.
    ///
    /// Walks the `docs` table in key order, which is `(collection, id)` — so a
    /// single cursor covers every collection without needing to track which one
    /// is in progress, and a scoped snapshot is the same walk bounded to one
    /// collection's key range.
    fn snapshot_documents(
        &self,
        after: Option<SnapshotCursor>,
        scope: Option<CollectionId>,
    ) -> Result<(Vec<SnapshotDoc>, Option<SnapshotCursor>)> {
        use std::ops::Bound;

        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;

        let mut out = Vec::new();
        let mut cursor = None;
        let empty: &[u8] = &[];

        // `Excluded` on the resume point, so the document that ended the last
        // page is not sent twice; from the scoped collection's first key
        // otherwise, and from the beginning for the whole database.
        let start = match (&after, scope) {
            (Some(c), _) => Bound::Excluded((c.collection.0, c.after_key.as_slice())),
            (None, Some(id)) => Bound::Included((id.0, empty)),
            (None, None) => Bound::Unbounded,
        };
        // A scoped walk ends where the next collection's keys begin. The
        // top id has no successor, and an unbounded end reads the same for
        // it: nothing sorts after its keys.
        let end = match scope.and_then(|id| id.0.checked_add(1)) {
            Some(next_collection) => Bound::Excluded((next_collection, empty)),
            None => Bound::Unbounded,
        };

        // The key that ended a full page, kept only once the page is full.
        let mut last_sent: Option<(u64, Vec<u8>)> = None;
        for row in docs.range::<(u64, &[u8])>((start, end))? {
            if out.len() >= SNAPSHOT_PAGE {
                // Another row exists past a full page, so there is a next
                // page; the cursor names the last document sent. Looking
                // one row ahead is what lets a snapshot of exactly a page's
                // worth end here rather than with an empty page after it.
                cursor = last_sent.take().map(|(collection, after_key)| SnapshotCursor {
                    collection: CollectionId(collection),
                    after_key,
                });
                break;
            }

            let (key, value) = row?;
            let (collection, doc_key) = key.value();
            let record = codec::decode_doc_record(value.value())?;

            // A tombstone has no `_id` to recover from a body, so its id comes
            // from decoding the key is impossible — keyenc is one-way. Skipping
            // tombstones here means a delete does not travel in a snapshot;
            // that is safe, because the receiver never had the document.
            let Some(document) = record.document()? else {
                continue;
            };
            let id = match document.get(crate::ID_FIELD) {
                Some(value) => DocId::try_from_bson(value)?,
                None => continue,
            };

            out.push(SnapshotDoc {
                collection: CollectionId(collection),
                id,
                stamp: record.stamp,
                body: Some(record.body),
            });
            if out.len() == SNAPSHOT_PAGE {
                last_sent = Some((collection, doc_key.to_vec()));
            }
        }

        Ok((out, cursor))
    }

    /// Apply one page of a peer's snapshot, and move `progress` past it.
    ///
    /// The page's documents are one transaction: every one that wins
    /// last-writer-wins is written into it through `apply_remote_in_txn`,
    /// it is committed once if anything was written and aborted if nothing
    /// was, and what has to follow the commit — unique-violation records,
    /// change-stream publishes — is done after it in document order, the
    /// shape `commit_run` gives a replicated batch (ADR-119, ADR-152). The
    /// final page of a whole-database snapshot records the coverage the
    /// snapshot grants in that same transaction. A failure after the commit
    /// still advances `progress`: the page is durable, and asking for it
    /// again would only re-apply it as superseded.
    ///
    /// `progress` is the receiver's, carried between pages and rounds; a
    /// page applied under it is never asked for again.
    pub fn apply_snapshot_page(
        &self,
        peer: NodeId,
        progress: &mut SnapshotProgress,
        page: &SnapshotPage,
    ) -> Result<SnapshotApplied> {
        // Definitions first, through the DDL path with transactions of its
        // own — the reason a replicated run ends at a schema change
        // (ADR-119) — so every document below has a collection to land in.
        // DENIALS FIRST, THEN DEFINITIONS. The order is load-bearing and the
        // reverse loses data.
        //
        // A collection the sender dropped and recreated arrives on the page as
        // a definition of the NEW life and a tombstone for the OLD one. If the
        // receiver holds the old life and definitions are restored first,
        // `restore_collection` sees the name already present and does nothing
        // -- it does not compare incarnations, which is ADR-155's second
        // residual -- and the tombstone then drops the life it found. The
        // receiver ends with NO collection at all, and on a completed
        // whole-database snapshot that is permanent, because the coverage
        // grant means the `CreateCollection` entry is never served to it
        // either.
        //
        // Applied first, the tombstone takes the old life, `restore_collection`
        // then finds the name absent, checks the tombstone it has just written
        // -- the new life's `created` is above it -- and creates the new life.
        //
        // The reverse order is safe in the other direction too: a receiver
        // holding a life NEWER than the tombstone keeps it, because
        // `aims_at_a_previous_incarnation` judges the tombstone against what
        // stands here whenever it runs.
        if let (Some(dropped), Some(id)) = (page.dropped, progress.scope) {
            self.restore_collection_drop(id, dropped)?;
        }
        // The whole-database route's own denials, through the same predicate
        // the scoped one uses -- `restore_collection_drop` asks
        // `aims_at_a_previous_incarnation`, so a tombstone below the
        // incarnation standing here is ignored and one aimed at it takes it.
        //
        // Each is first held to what the sender's own coverage names (ADR-163).
        // The scoped route above cannot deny anything the pull was not about,
        // because it takes the id from `progress.scope` and never from the
        // page; this route takes both the id and the stamp from the wire, so
        // without a gate an arbitrary pair destroys an arbitrary collection.
        if progress.scope.is_none() {
            self.apply_carried_drops(page)?;
        }
        let mut ddl_refused = 0usize;
        for state in &page.collections {
            ddl_refused += self.restore_collection(state)?;
        }

        // The coverage a whole-database snapshot grants is the first page's
        // vector (module docs), remembered here and recorded with the last.
        if progress.scope.is_none() && progress.pages == 0 {
            progress.granted = Some(page.versions.clone());
        }
        let complete = page.next.is_none();
        // Cloned rather than borrowed: the borrow would outlive the page's
        // transaction and keep `progress` frozen past the point it is
        // advanced. It is one vector, on the last page only.
        let grant = if complete { progress.granted.clone() } else { None };

        let mut applied = 0usize;
        let mut superseded = 0usize;
        let mut failed = None;
        if !page.documents.is_empty() || grant.is_some() {
            // Resolved per distinct collection rather than per document —
            // a page is one collection's worth of documents, usually — and
            // outside the writer, from the state the definitions above left.
            // The same memo a batch carries, for the same two questions
            // (`sync::Memo`); nothing below is a schema change, so it stands
            // for the whole page.
            let mut memo = Memo::default();
            let txn = self.begin_write(WriterHolder::Repair)?;
            let mut pending = Vec::new();
            for document in &page.documents {
                let collection = self.memo_collection(&mut memo, document.collection)?;

                // The predicate the entries path applies, applied here too:
                // `apply_remote_in_txn` below decides last-writer-wins on the
                // document's own stamp and consults neither the tombstone nor
                // the incarnation floor, so a repair pulled from a peer that
                // has not applied the drop yet wrote the buried life straight
                // back in.
                if self.is_history(
                    &mut memo,
                    collection.as_deref(),
                    document.collection,
                    document.stamp,
                )? {
                    superseded += 1;
                    continue;
                }

                let Some(collection) = collection else {
                    // Gone here with a tombstone to say so — including the
                    // definition on this very page that `restore_collection`
                    // refused to recreate over one — is history, exactly as
                    // it is in `sync::apply_one`. Without one it means a
                    // truncated or reordered snapshot: the definition should
                    // have arrived on the first page.
                    if self.memo_dropped_at(&mut memo, document.collection)?.is_some() {
                        superseded += 1;
                        continue;
                    }
                    debug!(
                        collection = document.collection.0,
                        "snapshot document has no collection"
                    );
                    continue;
                };

                // Reconstructed as an ordinary replicated write, so
                // last-writer-wins decides, indexes are maintained, and a
                // unique violation is reported rather than smuggled past the
                // check.
                let entry = OplogEntry {
                    stamp: document.stamp,
                    kind: OpKind::Replace,
                    collection: collection.id,
                    doc_id: Some(document.id.clone()),
                    body: document.body.clone(),
                };
                // `Hold`: a snapshot document must not move the vectors —
                // the coverage is granted once, below, and the reason is on
                // `Position::Hold`.
                if let RemoteApplied::Applied { id, violations } =
                    self.apply_remote_in_txn(&txn, &collection, &entry, Position::Hold)?
                {
                    pending.push(Pending { collection, entry, id, violations });
                }
            }

            // Only a *completed* snapshot grants coverage: adopting it
            // earlier would stop the receiver asking for pages it has not
            // been sent. It rides in the last page's transaction, so a
            // snapshot costs no commit of bookkeeping (ADR-119's rule for a
            // batch's vector).
            let mut wrote = !pending.is_empty();
            if let Some(granted) = &grant {
                wrote |= Engine::absorb_version_vector_in_txn(&txn, granted)?;
            }
            // Where the pull has got to, in the page's own transaction so a
            // snapshot still costs no commit of bookkeeping (ADR-152's rule,
            // which ADR-161 preserves rather than trades away).
            //
            // Only on a page that wrote something. A page this node already
            // holds must not cost an fsync, so the cursor is not persisted for
            // one -- which bounds the resume: a restart resumes at the last
            // page that WROTE something, and the no-op pages after it are
            // re-pulled. They are cheap to redo precisely because they wrote
            // nothing.
            //
            // A completed snapshot has no cursor to leave behind, so the
            // record goes; whether one was there decides whether that is a
            // write at all.
            if complete {
                wrote |= Engine::forget_snapshot_progress_in_txn(&txn, peer, progress.scope)?;
            } else if wrote {
                Engine::persist_snapshot_progress_in_txn(
                    &txn,
                    peer,
                    &progress.advanced(page, pending.len()),
                )?;
            }
            if wrote {
                txn.commit()?;
            } else {
                // Nothing was written, so nothing is committed — a page
                // this node already holds must not cost an fsync.
                txn.abort()?;
            }

            // What had to wait for the commit, for every applied document
            // whether or not an earlier report failed, and the first error
            // afterwards: a report skipped for an entry that is already
            // durable would never publish and never mint its violation
            // (`commit_run`, ADR-029).
            applied = pending.len();
            let mut published = Vec::with_capacity(pending.len());
            for Pending { collection, entry, id, violations } in pending {
                match self.report_remote_write(
                    WriterHolder::Repair,
                    &collection,
                    &entry,
                    &id,
                    &violations,
                ) {
                    Ok(entries) => published.extend(entries),
                    Err(e) => {
                        failed.get_or_insert(e);
                    }
                }
            }
            self.publish(published);
        }

        *progress = progress.advanced(page, applied);
        // The one completion with no page transaction to ride in: a final page
        // that wrote nothing, which cannot clear a record an earlier page
        // left. Checked before it is opened, so a snapshot that never
        // persisted a cursor costs nothing here. ADR-161.
        if complete
            && page.documents.is_empty()
            && grant.is_none()
            && self.snapshot_progress_recorded(peer, progress.scope)?
        {
            self.forget_snapshot_progress(peer, progress.scope)?;
        }
        if complete {
            match progress.scope {
                None => info!(
                    pages = progress.pages,
                    documents = progress.documents,
                    "snapshot complete; the coverage it grants is recorded"
                ),
                Some(collection) => info!(
                    %collection,
                    pages = progress.pages,
                    documents = progress.documents,
                    "snapshot of the collection complete"
                ),
            }
        }
        match failed {
            Some(e) => Err(e),
            None => Ok(SnapshotApplied { applied, ddl_refused, superseded }),
        }
    }

    /// Apply the tombstones a whole-database page carries (ADR-162), held to
    /// what the sender's own coverage names (ADR-163).
    ///
    /// # Why this is not `restore_collection_drop` in a loop
    ///
    /// It was, and that cost the receiver `O(tombstones × collections)` per
    /// page. `restore_collection_drop` asks `collection_by_id`, which is a
    /// **full walk of `COLLECTIONS` with a `serde_json` deserialise per row**,
    /// and `record_collection_drop` opens a second read transaction after it.
    /// Per tombstone, on every page. `sync::Memo` does not help: it caches by
    /// id, and a page's tombstones are hundreds of thousands of *distinct*
    /// ids, so every lookup missed and paid the walk.
    ///
    /// Measured at 600 tombstones against 600 local collections, release
    /// build: 161 ms per page with **nothing to do**, against 0.5 ms here.
    /// That is a cost ADR-152's rule says a page a member already holds must
    /// not pay, and it lands on the node least able to absorb it, since a node
    /// taking a whole-database snapshot is by definition the one that fell
    /// behind.
    ///
    /// So the two questions are asked once for the whole list instead of once
    /// per tombstone, and the tombstones to record are written in one
    /// transaction rather than one each. The map is used **only to skip**: an
    /// id it reports live falls through to `restore_collection_drop`, which
    /// re-reads authoritatively, so the expensive branch is unchanged.
    ///
    /// # Why a stale `live` set cannot skip a drop that mattered
    ///
    /// Not because nothing in this loop creates a collection — that is true
    /// but it is the weak reason, and it would stop being true the moment
    /// someone added one. **The skip is equivalent to doing the work, and the
    /// tombstone comparison is what makes it so**, which holds against a
    /// concurrent writer this loop does not control.
    ///
    /// Take the bad case: a collection is created between the prefetch and the
    /// lookup, so `live` wrongly says absent and the re-read is skipped.
    /// Reaching the skip at all requires `dropped <= held[id]`, and:
    ///
    /// - a collection standing under that id must have `created` **above**
    ///   `held[id].hlc`, because `restore_collection` and `sync::apply_ddl`
    ///   both refuse a create with `created <= dropped.hlc`;
    /// - so `dropped.hlc <= held[id].hlc < current.created`, which is exactly
    ///   `aims_at_a_previous_incarnation`'s `predates_create` — the drop would
    ///   have been **ignored** by the branch that was skipped;
    /// - and `record_collection_drop` would have left the higher tombstone
    ///   standing, since `dropped <= existing` writes nothing.
    ///
    /// Both halves are no-ops, so the skip loses nothing.
    ///
    /// A stale `held` goes the harmless way in both directions too: a
    /// tombstone recorded concurrently costs at worst a redundant batch write,
    /// which is idempotent, and one collected by GC costs at worst
    /// re-recording something already past retention.
    fn apply_carried_drops(&self, page: &SnapshotPage) -> Result<()> {
        if page.dropped_collections.is_empty() {
            return Ok(());
        }
        let live: std::collections::HashSet<CollectionId> = self
            .collections(crate::engine::PairedShadows::Included)?
            .into_iter()
            .map(|meta| meta.id)
            .collect();
        let held: std::collections::HashMap<CollectionId, Stamp> =
            self.collections_dropped()?.into_iter().collect();

        let mut to_record = Vec::new();
        for (id, dropped) in &page.dropped_collections {
            // A peer may only deny what its own coverage names (ADR-163).
            if dropped.hlc > page.versions.get(dropped.node) {
                warn!(
                    collection = %id,
                    stamp = ?dropped,
                    covered_to = ?page.versions.get(dropped.node),
                    "a whole-database page denied a collection with a stamp its sender's own \
                     coverage does not name; ignored"
                );
                continue;
            }
            if live.contains(id) {
                self.restore_collection_drop(*id, *dropped)?;
                continue;
            }
            // The receiver never held this collection, which ADR-162 notes is
            // the ordinary case for most of a sender's list. Nothing to do at
            // all once a tombstone at or above this one is recorded — which is
            // every page after the one that recorded it.
            if held.get(id).is_some_and(|existing| dropped <= existing) {
                continue;
            }
            debug!(
                collection = %id,
                stamp = ?dropped,
                "recording a peer's collection tombstone; entries addressed to it are history"
            );
            to_record.push((*id, *dropped));
        }
        // One transaction for the list rather than one per tombstone. The
        // first page of a catch-up records the sender's whole list, and
        // `record_collection_drop` takes the writer and fsyncs for each.
        self.record_collection_drops(&to_record)
    }

    /// Apply a drop the sender carried: the one collection a scoped snapshot
    /// is of (`page.dropped`), or one of the tombstones a whole-database page
    /// carries (`page.dropped_collections`, ADR-162). In both cases so that
    /// the entries this node was stopped at for it become history
    /// (ADR-148's tombstone rule) rather than a stop that repeats for the
    /// life of the process — and, when this node still holds the very
    /// incarnation that was dropped, apply the drop here as well.
    ///
    /// Which incarnation it is aimed at decides it, and that question is
    /// asked in one place — `sync::aims_at_a_previous_incarnation`, which the
    /// replicated `DropCollection` arm asks too. A drop aimed at the copy
    /// standing here takes it: leaving it is the resurrection the tombstone
    /// exists to stop, since this node would go on advertising a collection
    /// the cluster has agreed is deleted, and the peers that applied the drop
    /// would pull it back from here. A drop aimed at a life that has already
    /// ended is ignored outright, tombstone and all: the sender is simply
    /// behind, and a tombstone below the incarnation standing here says
    /// nothing that incarnation's own floor does not.
    fn restore_collection_drop(&self, id: CollectionId, dropped: Stamp) -> Result<()> {
        if let Some(current) = self.collection_by_id(id)? {
            if crate::sync::aims_at_a_previous_incarnation(&current, dropped.hlc) {
                debug!(collection = %id, "a snapshot carried a drop of a life of this collection that has already ended; ignored");
                return Ok(());
            }
            // What is removed is a partial copy of a life that has ended
            // everywhere else, which on a collection a repair was part-way
            // through can be a large removal. It is chunked, as every drop is
            // since ADR-158, so the receiver's writer is held for one chunk of
            // it at a time; the tombstone is recorded before the first of
            // them, so this node stops serving and re-seeding that copy at the
            // drop's first commit rather than at its last.
            warn!(
                db = %current.db,
                collection = %current.name,
                stamp = ?dropped,
                "the peer has dropped the collection this snapshot was to repair, and the copy \
                 held here is of the incarnation it dropped; dropping it too"
            );
            self.drop_collection_inner(&current.db, &current.name, Some(dropped))?;
        } else {
            // `debug`, not `warn`. On the scoped route this is one line for the
            // one collection the pull is about. On the whole-database route
            // (ADR-162) it is the ORDINARY case for most of the sender's
            // tombstones -- the receiver never held those collections -- and
            // the list replays on every page, so at `warn` a single catch-up
            // writes hundreds of thousands of lines saying nothing happened.
            debug!(
                collection = %id,
                stamp = ?dropped,
                "recording a peer's collection tombstone; entries addressed to it are history"
            );
        }
        // Recorded on both paths that reach here, and after the drop rather
        // than instead of it: `drop_collection_inner` writes the tombstone at
        // the sender's stamp in its own transaction, and a node that never
        // held the collection still needs one. The pair `sync::apply_ddl`'s
        // drop arm writes, for its reasons.
        self.record_collection_drop(id, dropped)
    }

    /// Recreate a collection and its indexes from a snapshot.
    ///
    /// Returns how many of its index definitions this node refused. A
    /// snapshot is served to exactly the peer most likely to hold documents
    /// a definition does not fit — one that was away long enough to write on
    /// its own past the origin's retention — and a bare error here failed the
    /// whole snapshot round, the same wedge as a replayed `CreateIndex`
    /// reached through the other route. Such documents are filed unkeyed
    /// now (ADR-139); what is still refused is a definition this build cannot
    /// apply. So the
    /// definitions go through the same classification as replicated DDL
    /// (`sync::settle`, ADR-123): a refused one is warned and counted, and
    /// the restore goes on to the documents; any other error still fails
    /// the page.
    fn restore_collection(&self, state: &CollectionState) -> Result<usize> {
        if self.get_collection(&state.db, &state.name).is_err() {
            // A snapshot must not recreate a life this node has buried. The
            // page is current state on its sender, not an instruction: a peer
            // that has not applied the drop yet still serves the collection,
            // and creating it here would put it back on every member that had
            // it right. The same rule `sync::apply_ddl` applies to a replayed
            // `CreateCollection`, in the one place a collection can otherwise
            // arrive without an entry behind it.
            //
            // A page carrying no stamp comes from a sender that predates the
            // field, and reads as the incarnation this node dropped rather
            // than as a later one — the mixed-version choice, made this way
            // because a genuine recreation still arrives through the entries
            // path, where a resurrection cannot be undone.
            let id = CollectionId::derive(&state.db, &state.name);
            if let Some(dropped) = self.collection_dropped_at(id)?
                && state.created.is_none_or(|created| created <= dropped.hlc)
            {
                debug!(
                    db = %state.db,
                    collection = %state.name,
                    "ignored a creation older than the drop that removed it"
                );
                return Ok(0);
            }
            // `state.created` and not this node's clock: it is the stamp the
            // create carries at its origin, which is what a replayed drop is
            // judged against (`create_collection_inner`) and what this node
            // then advertises as the incarnation it holds.
            self.create_collection_inner(&state.db, &state.name, false, state.created)?;
        }

        let mut refused = 0usize;
        for index in &state.indexes {
            let existing = self.get_collection(&state.db, &state.name)?;
            let created = self.create_index_inner(
                &state.db,
                &state.name,
                index.fields.clone(),
                index.unique,
                index.enforcement,
                Some(index.name.clone()),
                // A restored TTL index keeps its policy: dropping it here
                // would leave a collection that silently stopped expiring.
                index.expire_after_secs,
                index.partial_filter.clone(),
                // The definition's own creation stamp, which is the only
                // ordering fact a snapshot carries — there is no entry behind
                // it. A name already held here by a different definition is
                // resolved against it exactly as a replicated `CreateIndex`
                // is (ADR-132); a definition already here under this name is
                // returned unchanged, which is what makes calling this for
                // every index of the page cheap. A snapshot written before
                // the stamp existed carries none, and such a rival is refused
                // and counted rather than silently skipped as it was.
                crate::index::CreateOrigin::Replicated(index.created),
            );
            match crate::sync::settle(created)? {
                crate::sync::Ddl::Applied((_, violations)) => {
                    // A replicated definition is built over whatever this
                    // node already holds, collisions reported rather than
                    // refused (ADR-020, ADR-123). Usually nothing: the
                    // indexes are restored before the documents.
                    if !violations.is_empty() {
                        self.report_index_backfill_violations(&existing, &violations)?;
                    }
                }
                // The collection was created or found moments ago; gone now
                // means a concurrent local drop, and the rest of its indexes
                // have nowhere to go.
                crate::sync::Ddl::Gone => break,
                crate::sync::Ddl::Refused(reason) => {
                    refused += 1;
                    warn!(
                        db = %state.db,
                        collection = %state.name,
                        index = %index.name,
                        reason = %reason,
                        "skipped an index in a snapshot this node cannot build; the \
                         definition stands on its peers and the divergence is counted in \
                         kimmy_sync_ddl_refused_total"
                    );
                }
            }
        }

        if let Some(config) = &state.vector {
            let existing = self.get_collection(&state.db, &state.name)?;
            if existing.vector.as_ref() != Some(config) {
                self.configure_vectors_inner(&state.db, &state.name, config.clone(), false)?;
            }
        }
        Ok(refused)
    }

    /// Whether a peer asking from `from` can be served from the oplog.
    ///
    /// `false` means the history it needs has been collected, and serving it
    /// incrementally would hand it a silent gap.
    ///
    /// Compared against what retention has actually *removed*, not against the
    /// oldest retained entry: on a node that has never collected anything the
    /// oldest entry is just the first write ever made, and a peer asking from
    /// before it would be sent a full snapshot for no reason.
    pub fn can_serve_from_oplog(&self, from: Hlc) -> Result<bool> {
        Ok(from >= self.oplog_collected_through()?)
    }

    /// Whether a peer that has processed `held` can be served from the oplog.
    ///
    /// The per-origin form of [`Self::can_serve_from_oplog`], for a peer that
    /// sent its vector rather than only the threshold it derived from it.
    /// `false` when, at any origin this node is ahead of the peer, retention
    /// has removed an entry the peer lacks — that is the silent gap. `true`
    /// otherwise, *even when the threshold is below the coarse horizon*:
    /// then every entry the peer lacks is still here to serve, and the gap
    /// between its threshold and the horizon is made of entries it holds.
    ///
    /// The case that needs this is an origin that wrote nothing for longer
    /// than retention and then wrote once. A peer that had everything asks
    /// from the previous write — collected long ago, along with everything
    /// around it — and the coarse horizon can only answer with a snapshot,
    /// for a gap that holds exactly one servable entry (ADR-097).
    pub fn can_serve_peer_holding(&self, held: &VersionVector) -> Result<bool> {
        let mine = self.version_vector()?;
        let collected = self.oplog_collected()?;
        Ok(!crate::sync::lacks_collected(held, &mine, &collected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }

    /// Transfer a snapshot from `from` into `into` under `progress`, page by
    /// page, exactly as the transport does across its rounds.
    fn transfer_under(into: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> usize {
        let mut applied = 0;
        while !progress.is_complete() {
            let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            applied += into.apply_snapshot_page(from.node_id(), progress, &page).unwrap().applied;
        }
        applied
    }

    /// A peer id for a fixture that hand-builds a page and has no sender
    /// engine to take one from. The peer is only a key to
    /// `apply_snapshot_page` -- it is stored and never interpreted -- so any
    /// stable value does, and a named one says that rather than leaving a
    /// reader to wonder whose node it is.
    fn no_sender() -> NodeId {
        NodeId::from_bytes([7; 16])
    }

    /// Transfer a full snapshot from `from` into `into`.
    fn transfer(into: &Engine, from: &Engine) -> usize {
        transfer_under(into, from, &mut SnapshotProgress::whole_database())
    }

    /// A `(db, name)` whose derived id has the top bit set — the half of the
    /// id space BSON cannot carry as an unsigned integer.
    fn high_bit_collection(db: &str) -> String {
        (0u32..)
            .map(|i| format!("orders-{i}"))
            .find(|name| CollectionId::derive(db, name).0 > i64::MAX as u64)
            .unwrap()
    }

    #[test]
    fn a_snapshot_page_naming_a_high_bit_collection_survives_bson() {
        // The in-process transfer above never serialises, which is how a page
        // that BSON refused to encode passed every test here while every real
        // snapshot on a cluster failed with "cannot fit into BSON".
        let (a, _da) = engine();
        let name = high_bit_collection("shop");
        let ca = a.create_collection("shop", &name).unwrap();
        assert!(ca.id.0 > i64::MAX as u64, "the fixture must sit in the unencodable half");
        for i in 0..(SNAPSHOT_PAGE + 1) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        let page = a.snapshot_page(None, None).unwrap();
        assert!(page.next.is_some(), "the cursor must be exercised too");
        let bytes = bson::serialize_to_vec(&page).expect("a page must always encode");
        let back: SnapshotPage = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, page, "and round-trip exactly, cursor included");

        let (b, _db) = engine();
        assert_eq!(transfer(&b, &a), SNAPSHOT_PAGE + 1);
        let cb = b.get_collection("shop", &name).unwrap();
        assert_eq!(cb.id, ca.id);
        assert_eq!(b.count(&cb).unwrap() as usize, SNAPSHOT_PAGE + 1);
    }

    #[test]
    fn a_snapshot_carries_collections_indexes_and_documents() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("item")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        for i in 0..10i64 {
            a.insert(&ca, doc! { "_id": i, "item": format!("item-{i}") }).unwrap();
        }

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").expect("the collection must arrive");
        assert_eq!(b.count(&cb).unwrap(), 10);
        let index = cb.indexes.iter().find(|i| i.name == "item_1").expect("the index must arrive");
        assert!(index.unique);
    }

    #[test]
    fn a_snapshot_index_over_a_document_this_node_cannot_key_builds_and_files_it_unkeyed() {
        // The route served to exactly the peer most likely to hold divergent
        // documents. B wrote a two-array document while away; A's snapshot
        // carries a compound index over those two fields. ADR-123 refused
        // the definition here and counted it; under ADR-139 it builds, B's
        // document is filed unkeyed under it, and every document restores.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("tags"), field("cats")], false, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a-1", "tags": ["x"] }).unwrap();
        a.insert(&ca, doc! { "_id": "a-2", "cats": ["p"] }).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

        let page = a.snapshot_page(None, None).unwrap();
        let outcome = b
            .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
            .expect("the page applies");
        assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
        assert_eq!(outcome.applied, 2, "the documents restore: {outcome:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        let index = cb.index("tags_1_cats_1").expect("the definition builds here too");
        assert_eq!(b.unkeyed_count(&cb, index.id).unwrap(), 1, "B's own document, filed unkeyed");
        for id in ["a-1", "a-2", "both"] {
            assert!(b.get(&cb, &DocId::String(id.into())).unwrap().is_some(), "{id}");
        }
    }

    #[test]
    fn a_snapshot_index_this_node_cannot_apply_is_skipped_and_the_documents_restore() {
        // The refusal class ADR-123 keeps, reached through the snapshot
        // route: a definition this build cannot apply — a TTL over two
        // fields, which no member can mint but a page written by another
        // build could carry — is warned, counted, and skipped, and the
        // page's documents still restore.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("tags"), field("cats")], false, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a-1", "tags": ["x"] }).unwrap();
        b.create_collection("shop", "orders").unwrap();

        let mut page = a.snapshot_page(None, None).unwrap();
        for state in &mut page.collections {
            for index in &mut state.indexes {
                index.expire_after_secs = Some(60);
            }
        }
        let outcome = b
            .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
            .expect("a refused index must not fail the page");
        assert_eq!(outcome.ddl_refused, 1, "{outcome:?}");
        assert_eq!(outcome.applied, 1, "the documents restore: {outcome:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        assert!(cb.index("tags_1_cats_1").is_none(), "a definition this build cannot apply");
        assert!(b.get(&cb, &DocId::String("a-1".into())).unwrap().is_some());
    }

    /// Rewrite an index's creation stamp in place, so a test can put the
    /// receiver's definition in a known order against the snapshot's without
    /// racing the wall clock.
    fn restamp_index(engine: &Engine, db: &str, collection: &str, name: &str, created: Stamp) {
        let mut meta = engine.get_collection(db, collection).unwrap();
        meta.indexes.iter_mut().find(|i| i.name == name).expect("the index is here").created =
            Some(created);
        let db = engine.db();
        let txn = db.begin_write().unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn a_snapshot_definition_under_a_taken_name_settles_on_the_later_stamp() {
        // The snapshot route follows the rule the replicated `CreateIndex`
        // route follows (ADR-132), where before it silently kept whatever the
        // name already held. B's definition is older than anything A can
        // mint, so A's replaces it; C's is newer, so C keeps its own and the
        // arrival is history.
        let node = |n: u8| kimmy_core::NodeId::from_bytes([n; 16]);
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("item")], true, Some("by_item".into()))
            .unwrap();

        let (b, _db) = engine();
        b.create_collection("shop", "orders").unwrap();
        b.create_index("shop", "orders", vec![field("item")], false, Some("by_item".into()))
            .unwrap();
        restamp_index(&b, "shop", "orders", "by_item", Stamp::new(Hlc::new(1, 0), node(1)));

        let (c, _dc) = engine();
        c.create_collection("shop", "orders").unwrap();
        c.create_index("shop", "orders", vec![field("item")], false, Some("by_item".into()))
            .unwrap();
        // Far ahead of any clock, but still an encodable stamp: `Hlc::MAX`
        // holds a `wall_ms` above `i64::MAX`, which BSON cannot carry, and a
        // fixture must not pin a value the wire would refuse.
        restamp_index(
            &c,
            "shop",
            "orders",
            "by_item",
            Stamp::new(Hlc::new(i64::MAX as u64, 0), node(1)),
        );

        let page = a.snapshot_page(None, None).unwrap();
        let into_b = b
            .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
            .unwrap();
        assert_eq!(into_b.ddl_refused, 0, "the later definition is not a refusal: {into_b:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("by_item").unwrap().unique,
            "the snapshot's definition is the later one and replaces B's"
        );

        let into_c = c
            .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
            .unwrap();
        assert_eq!(
            into_c.ddl_refused, 0,
            "an older definition is history, not a refusal: {into_c:?}"
        );
        assert!(
            !c.get_collection("shop", "orders").unwrap().index("by_item").unwrap().unique,
            "C's definition is the later one and stands"
        );
    }

    #[test]
    fn a_snapshot_pages_a_collection_larger_than_one_page() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let total = SNAPSHOT_PAGE * 2 + 7;
        for i in 0..total as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap(), total as u64, "every page must arrive exactly once");
    }

    /// The transaction rule (ADR-152): a page of `SNAPSHOT_PAGE` documents is
    /// one commit on the receiver — the final page's coverage included, so a
    /// snapshot that fits one page is one commit in all — where it was one
    /// per document. Fails on the per-document form with a delta of 512.
    #[test]
    fn a_snapshot_page_is_one_commit_however_many_documents_it_holds() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..SNAPSHOT_PAGE as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        // The collection is here already, so the page's definitions cost no
        // DDL commit and what is measured is the documents alone.
        b.create_collection("shop", "orders").unwrap();

        let page = a.snapshot_page(None, None).unwrap();
        assert_eq!(page.documents.len(), SNAPSHOT_PAGE);
        assert!(page.next.is_none(), "exactly a page's worth ends the snapshot without a trailer");

        let before = b.commits();
        let mut progress = SnapshotProgress::whole_database();
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert_eq!(outcome.applied, SNAPSHOT_PAGE);
        assert_eq!(b.commits() - before, 1, "one page, one commit, coverage included");
        assert!(progress.is_complete());
        assert!(
            b.version_vector().unwrap().covers(&page.versions),
            "and the coverage rode in that commit"
        );
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap() as usize, SNAPSHOT_PAGE);
    }

    /// The other half of the rule: a page whose every document this node
    /// already holds — the ordinary case for a repair, which re-sends a
    /// collection the member mostly has — writes nothing and commits
    /// nothing. Fails on the per-document form, which aborted 512 times and
    /// committed none, but only because it never opened the page's own
    /// transaction; here the page's transaction is opened and let go.
    #[test]
    fn a_page_whose_documents_are_all_superseded_commits_nothing() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 1) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        transfer(&b, &a);

        // Again, scoped: a repair's shape, and the shape with no coverage to
        // record on the final page either.
        let mut progress = SnapshotProgress::of_collection(ca.id);
        let before = b.commits();
        let first = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert!(first.next.is_some());
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap();
        assert_eq!(outcome.applied, 0, "every document already here: {outcome:?}");
        let last = a.snapshot_page(progress.after().cloned(), Some(ca.id)).unwrap();
        assert!(last.next.is_none());
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &last).unwrap();
        assert_eq!(outcome.applied, 0, "{outcome:?}");
        assert_eq!(b.commits() - before, 0, "a superseded page must not cost a commit");
        assert!(progress.is_complete());
        assert_eq!(progress.documents(), 0);
        assert_eq!(progress.pages(), 2);
    }

    /// A scoped snapshot (ADR-152) carries one collection: its definition on
    /// the first page, its documents and nothing else's, a cursor that
    /// resumes inside it, and an end at the collection's end. It grants no
    /// coverage, because coverage is per origin and cannot be scoped.
    #[test]
    fn a_snapshot_scoped_to_one_collection_carries_only_that_collection() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        // Three collections, so the scoped one has neighbours on both sides
        // of its key range whichever ids the names derive.
        let mut ids = Vec::new();
        for name in ["alpha", "orders", "zeta"] {
            let c = a.create_collection("shop", name).unwrap();
            for i in 0..10i64 {
                a.insert(&c, doc! { "_id": i, "in": name }).unwrap();
            }
            ids.push(c.id);
        }
        let orders = a.get_collection("shop", "orders").unwrap();
        for i in 10..(SNAPSHOT_PAGE as i64 + 5) {
            a.insert(&orders, doc! { "_id": i, "in": "orders" }).unwrap();
        }
        let b_before = b.version_vector().unwrap();

        let first = a.snapshot_page(None, Some(orders.id)).unwrap();
        assert_eq!(first.collections.len(), 1, "the one definition: {:?}", first.collections);
        assert_eq!(first.collections[0].name, "orders");
        assert_eq!(first.documents.len(), SNAPSHOT_PAGE);
        assert!(first.documents.iter().all(|d| d.collection == orders.id));
        let cursor = first.next.clone().expect("more of the collection follows");
        assert_eq!(cursor.collection, orders.id, "the cursor is inside the collection");
        assert_eq!(first.dropped, None);

        let last = a.snapshot_page(Some(cursor), Some(orders.id)).unwrap();
        assert!(last.collections.is_empty(), "definitions ride the first page only");
        assert_eq!(last.documents.len(), 5, "the rest of the collection and nothing else");
        assert!(last.documents.iter().all(|d| d.collection == orders.id));
        assert!(last.next.is_none(), "ends at the collection's end");

        let mut progress = SnapshotProgress::of_collection(orders.id);
        assert_eq!(
            b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap().applied,
            SNAPSHOT_PAGE
        );
        assert_eq!(b.apply_snapshot_page(a.node_id(), &mut progress, &last).unwrap().applied, 5);
        assert!(progress.is_complete());
        assert_eq!(progress.documents(), SNAPSHOT_PAGE + 5);

        let ob = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&ob).unwrap() as usize, SNAPSHOT_PAGE + 5);
        assert!(b.get_collection("shop", "alpha").is_err(), "a scoped snapshot brings one");
        assert!(b.get_collection("shop", "zeta").is_err());
        // B's own origin moved — creating the collection here minted an
        // entry — and A's did not: a scoped snapshot grants no coverage, and
        // its documents move no vector either. The position carries the rest.
        assert_eq!(b.version_vector().unwrap().get(a.node_id()), b_before.get(a.node_id()));
        assert_eq!(b.witnessed_vector().unwrap().get(a.node_id()), Hlc::ZERO);

        // The whole-database snapshot is what it was: every collection,
        // every document, coverage at the end.
        let (c, _dc) = engine();
        assert_eq!(transfer(&c, &a), 30 + SNAPSHOT_PAGE - 5);
        for name in ["alpha", "orders", "zeta"] {
            assert!(c.get_collection("shop", name).is_ok(), "{name}");
        }
        assert!(c.version_vector().unwrap().covers(&a.version_vector().unwrap()));
    }

    /// The last collection in id order is scoped by a range with no upper
    /// bound, and a scope on an id with no successor must not panic or
    /// overrun; both are the same walk bounded correctly.
    #[test]
    fn a_scoped_snapshot_of_the_top_id_walks_to_the_end_of_the_table() {
        let (a, _da) = engine();
        let names = ["alpha", "beta", "gamma", "delta"];
        for name in names {
            let c = a.create_collection("shop", name).unwrap();
            a.insert(&c, doc! { "_id": 1, "in": name }).unwrap();
        }
        let top = names
            .iter()
            .map(|name| a.get_collection("shop", name).unwrap())
            .max_by_key(|c| c.id.0)
            .unwrap();
        let page = a.snapshot_page(None, Some(top.id)).unwrap();
        assert_eq!(page.documents.len(), 1, "{:?}", page.documents);
        assert_eq!(page.documents[0].collection, top.id);
        assert!(page.next.is_none());

        // And an id nothing derives, at the very top: an empty snapshot,
        // not a panic on the successor.
        let page = a.snapshot_page(None, Some(CollectionId(u64::MAX))).unwrap();
        assert!(page.collections.is_empty() && page.documents.is_empty() && page.next.is_none());
    }

    /// The coverage a snapshot grants is the vector served with its first
    /// page (ADR-152). A document the sender writes behind the cursor while
    /// the snapshot runs is not carried, and its entry sits above that vector
    /// — so the receiver still asks for it — where the final page's vector
    /// covers it and would have left a hole nothing re-serves. A document
    /// written ahead of the cursor is carried, and is above the vector too,
    /// which only costs a superseded re-delivery — and applying it must not
    /// move the vector to its stamp either, which is what appending a
    /// replicated entry does for a window and what the first form of this
    /// restore did per document: that carried the position over the
    /// document behind the cursor just the same.
    #[test]
    fn the_coverage_a_snapshot_grants_is_the_vector_served_with_its_first_page() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 1) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        let mut progress = SnapshotProgress::whole_database();
        let first = a.snapshot_page(None, None).unwrap();
        let first_vector = first.versions.clone();
        b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap();
        assert!(!progress.is_complete());

        // Between pages: one document behind the cursor, one ahead of it.
        // Int64 keys sort numerically, so -1 is behind every id sent.
        a.insert(&ca, doc! { "_id": -1, "written": "behind" }).unwrap();
        a.insert(&ca, doc! { "_id": 100_000, "written": "ahead" }).unwrap();
        let last = a.snapshot_page(progress.after().cloned(), None).unwrap();
        assert!(last.next.is_none());
        assert!(last.versions != first_vector, "the final page's vector names the new writes");
        b.apply_snapshot_page(a.node_id(), &mut progress, &last).unwrap();
        assert!(progress.is_complete());

        let cb = b.get_collection("shop", "orders").unwrap();
        assert!(b.get(&cb, &DocId::Int64(100_000)).unwrap().is_some(), "ahead: carried");
        assert!(b.get(&cb, &DocId::Int64(-1)).unwrap().is_none(), "behind: not carried");
        let granted = b.witnessed_vector().unwrap();
        assert!(granted.covers(&first_vector), "the first page's vector was adopted");
        assert!(
            !granted.covers(&last.versions),
            "and not the last page's: the receiver still asks for what was written behind \
             the cursor"
        );
        assert_eq!(
            granted.behind(&a.version_vector().unwrap()),
            Some(first_vector.get(a.node_id())),
            "the next round asks from where the snapshot's vector left it"
        );
    }

    /// A scoped snapshot of a collection the sender has since dropped
    /// carries the drop (ADR-152): the receiver, which was stopped at
    /// entries for a collection it has no record of, records the tombstone
    /// and those entries are history on the next batch rather than a stop
    /// that repeats.
    #[test]
    fn a_scoped_snapshot_of_a_dropped_collection_carries_its_tombstone() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let entry = a.entries_for_peer(Hlc::ZERO, 10).unwrap().entries.pop().unwrap();
        assert_eq!(entry.collection, ca.id, "the insert, which B is about to be stopped at");
        a.drop_collection("shop", "orders").unwrap();
        let dropped_at = a.collection_dropped_at(ca.id).unwrap().expect("a tombstone");

        // B has no record of the collection: the entry stops its batch.
        let stopped = b.apply_batch(std::slice::from_ref(&entry)).unwrap();
        assert_eq!(stopped.unknown_collection, 1, "{stopped:?}");

        let page = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert!(page.collections.is_empty() && page.documents.is_empty(), "{page:?}");
        assert_eq!(page.dropped, Some(dropped_at), "the drop travels in its place");
        assert!(page.next.is_none());
        let mut progress = SnapshotProgress::of_collection(ca.id);
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert!(progress.is_complete());
        assert_eq!(b.collection_dropped_at(ca.id).unwrap(), Some(dropped_at));

        let history = b.apply_batch(std::slice::from_ref(&entry)).unwrap();
        assert_eq!(history.unknown_collection, 0, "history now: {history:?}");
        assert_eq!(history.superseded, 1, "{history:?}");
        assert!(b.get_collection("shop", "orders").is_err(), "and nothing was resurrected");

        // The whole-database route names no single collection in `dropped` --
        // that field is the scoped route's -- but it does deny, in
        // `dropped_collections`, and that is ADR-162.
        let whole = a.snapshot_page(None, None).unwrap();
        assert_eq!(whole.dropped, None);
        assert!(
            whole.dropped_collections.iter().any(|(id, _)| *id == ca.id),
            "a whole-database page must carry the sender's tombstones: {:?}",
            whole.dropped_collections
        );
    }

    /// A moment, so an engine's clock separates what happens on either side
    /// of it. The stamps these tests compare are minted from physical time,
    /// and a fixture must not race the millisecond it is pinning.
    fn a_moment() {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    /// A snapshot is current state on its sender, not an instruction: a peer
    /// that has not applied the drop yet still holds the collection and still
    /// serves it. Recreating it here would put it back on every member that
    /// had it right — the finding, reached through the repair rather than
    /// through a replayed `CreateCollection`, which `sync::apply_ddl` has
    /// refused since ADR-034.
    #[test]
    fn a_snapshot_does_not_recreate_a_collection_this_node_has_dropped() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        b.create_collection("shop", "orders").unwrap();
        a_moment();
        b.drop_collection("shop", "orders").unwrap();
        let dropped = b.collection_dropped_at(ca.id).unwrap().expect("a tombstone");
        assert!(ca.created <= dropped.hlc, "the fixture must drop after the sender's create");
        // A second document, written on A *after* the drop it has not heard
        // about: not history by its own stamp, and history only because the
        // collection it names is one this node buried. The two documents are
        // the two ways a page's document is turned away.
        a_moment();
        a.insert(&ca, doc! { "_id": 2 }).unwrap();

        let outcome = b
            .apply_snapshot_page(
                a.node_id(),
                &mut SnapshotProgress::whole_database(),
                &a.snapshot_page(None, None).unwrap(),
            )
            .unwrap();
        assert!(b.get_collection("shop", "orders").is_err(), "the drop stands");
        assert_eq!(outcome.applied, 0, "{outcome:?}");
        assert_eq!(
            outcome.superseded, 2,
            "both documents are history, counted rather than passed over: {outcome:?}"
        );
    }

    /// The same rule under a **chunked** drop (ADR-158). A drop clears what
    /// the collection held a chunk at a time, and a repair from a peer that
    /// has not applied the drop yet can arrive during any of them — the
    /// window that chunking widens and this is the guard on it. The tombstone
    /// is recorded in the drop's first commit, so the refusal is in place for
    /// the whole of the purge rather than from its end: neither the
    /// definition nor a document of the buried life comes back.
    #[test]
    fn a_snapshot_does_not_recreate_a_collection_whose_drop_is_still_purging() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": 1 }).unwrap();
        a_moment();
        // B's drop, stopped after its first commit: definition gone,
        // tombstone written, every document still filed under the id.
        b.bury_collection("shop", "orders", None).unwrap().expect("dropped");
        let dropped = b.collection_dropped_at(cb.id).unwrap().expect("a tombstone");
        assert!(ca.created <= dropped.hlc, "the fixture must drop after the sender's create");
        a_moment();
        a.insert(&ca, doc! { "_id": 2 }).unwrap();

        let outcome = b
            .apply_snapshot_page(
                a.node_id(),
                &mut SnapshotProgress::whole_database(),
                &a.snapshot_page(None, None).unwrap(),
            )
            .unwrap();

        assert!(b.get_collection("shop", "orders").is_err(), "the drop stands mid-purge");
        assert_eq!(outcome.applied, 0, "{outcome:?}");
        assert_eq!(outcome.superseded, 2, "both documents are history: {outcome:?}");
        // And the purge finishes over the top of the refused page.
        assert_eq!(b.purge_dropped_collection(cb.id).unwrap(), 1, "the one document it held");
        assert!(b.get_collection("shop", "orders").is_err());
    }

    /// The other side of the same rule, and the reason it is a comparison
    /// rather than a refusal: a collection genuinely recreated after the drop
    /// is restored, under the sender's incarnation.
    #[test]
    fn a_snapshot_recreates_a_collection_created_after_the_drop_held_here() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        b.create_collection("shop", "orders").unwrap();
        b.drop_collection("shop", "orders").unwrap();
        a_moment();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let dropped = b.collection_dropped_at(ca.id).unwrap().expect("a tombstone");
        assert!(ca.created > dropped.hlc, "the fixture must create after the drop");

        let outcome = b
            .apply_snapshot_page(
                a.node_id(),
                &mut SnapshotProgress::whole_database(),
                &a.snapshot_page(None, None).unwrap(),
            )
            .unwrap();
        assert_eq!(outcome.applied, 1, "{outcome:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(cb.created, ca.created, "restored under the sender's incarnation");
        assert_eq!(b.count(&cb).unwrap(), 1);
    }

    /// The mixed-version rule at the restore's door: a sender that predates
    /// `CollectionState::created` names no incarnation, and an absent stamp
    /// reads as the life this node dropped rather than as a later one. The
    /// collection is not recreated for the minutes a roll is under way; a
    /// genuine recreation still arrives through the entries path, where a
    /// resurrection could not be undone.
    #[test]
    fn a_snapshot_that_names_no_incarnation_does_not_recreate_a_dropped_collection() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        b.create_collection("shop", "orders").unwrap();
        a_moment();
        b.drop_collection("shop", "orders").unwrap();

        let mut page = a.snapshot_page(None, None).unwrap();
        for state in &mut page.collections {
            state.created = None;
        }
        let outcome = b
            .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
            .unwrap();
        assert!(b.get_collection("shop", "orders").is_err(), "the drop stands");
        assert_eq!((outcome.applied, outcome.superseded), (0, 1), "{outcome:?}");
    }

    /// A restored collection carries the **sender's** `created`, not this
    /// node's clock at apply time.
    ///
    /// Not tidying. `create_collection_inner` states what `created` is for: a
    /// replayed drop is judged against it, and the local apply clock sits
    /// after the whole catch-up backlog, so a drop that legitimately followed
    /// the create reads as older than the incarnation and is ignored. It is
    /// also the incarnation this node then advertises to a peer's divergence
    /// check, which would read a local clock as a recreation newer than the
    /// tombstone that peer holds — and pull the collection back.
    #[test]
    fn a_whole_database_snapshot_restores_every_collection_under_the_senders_incarnation() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let mut sent = Vec::new();
        for name in ["alpha", "orders", "zeta"] {
            let c = a.create_collection("shop", name).unwrap();
            a.insert(&c, doc! { "_id": 1 }).unwrap();
            sent.push((name, c.created));
        }
        // So that a `created` taken from this node's clock could not pass
        // for the sender's.
        a_moment();

        transfer(&b, &a);

        for (name, created) in sent {
            assert_eq!(
                b.get_collection("shop", name).unwrap().created,
                created,
                "collection {name} must keep the incarnation it began at"
            );
        }
    }

    /// The document half of the rule (`Engine::is_history`, the predicate the
    /// entries path applies). Every snapshot document went straight to
    /// `apply_remote_in_txn`, which decides last-writer-wins on the
    /// document's own stamp and consults neither the tombstone nor the
    /// incarnation floor — so a repair pulled from a peer that had not
    /// applied the drop wrote the buried life back into its replacement.
    #[test]
    fn a_snapshot_document_from_a_life_buried_here_is_history_and_is_counted() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1, "life": "the first" }).unwrap();

        // Dropped and created again here: the same derived id, and an
        // incarnation floor at the drop.
        b.create_collection("shop", "orders").unwrap();
        a_moment();
        b.drop_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        assert!(cb.incarnation_floor.is_some(), "the replacement must carry a floor");

        let mut progress = SnapshotProgress::of_collection(ca.id);
        let page = a.snapshot_page(None, Some(ca.id)).unwrap();
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert_eq!(outcome.applied, 0, "{outcome:?}");
        assert_eq!(outcome.superseded, 1, "{outcome:?}");
        assert_eq!(b.count(&cb).unwrap(), 0, "the previous life must not enter the replacement");

        // And the same page's next document, written after the drop, is not
        // history and lands: this is a floor, not a gate on the collection.
        a_moment();
        a.insert(&ca, doc! { "_id": 2, "life": "after the drop" }).unwrap();
        let mut progress = SnapshotProgress::of_collection(ca.id);
        let page = a.snapshot_page(None, Some(ca.id)).unwrap();
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert_eq!((outcome.applied, outcome.superseded), (1, 1), "{outcome:?}");
        assert!(b.get(&cb, &DocId::Int64(2)).unwrap().is_some());
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_none());
    }

    /// A scoped snapshot answering with a drop of the very incarnation this
    /// node holds applies it here too.
    ///
    /// Recording the tombstone and keeping the copy leaves this node
    /// advertising a collection the cluster has agreed is deleted, and every
    /// peer that applied the drop then sees a divergence against it and
    /// repairs the copy back out — after this node has re-seeded them from
    /// it. It is a replicated drop: the sender's stamp on the tombstone, and
    /// no entry of this node's own.
    #[test]
    fn a_snapshot_carrying_a_drop_of_the_incarnation_held_here_drops_it_here_too() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().created,
            ca.created,
            "B holds the incarnation A is about to drop"
        );
        let b_own = b.version_vector().unwrap().get(b.node_id());

        a_moment();
        a.drop_collection("shop", "orders").unwrap();
        let dropped = a.collection_dropped_at(ca.id).unwrap().expect("a tombstone");

        let page = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert_eq!(page.dropped, Some(dropped), "the drop travels in the collection's place");
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(ca.id), &page)
            .unwrap();

        assert!(b.get_collection("shop", "orders").is_err(), "the copy held here went with it");
        assert_eq!(b.count_by_id(ca.id).unwrap(), None);
        assert_eq!(
            b.collection_dropped_at(ca.id).unwrap(),
            Some(dropped),
            "at the sender's stamp, so a recreation that followed it is still usable"
        );
        assert_eq!(
            b.version_vector().unwrap().get(b.node_id()),
            b_own,
            "a replicated drop mints no entry of this node's own"
        );
    }

    /// The sender is the one that is behind: it dropped a life this node has
    /// already replaced. The collection standing here is not the one the drop
    /// names, so it stays — the rule `sync::apply_ddl` applies to a replayed
    /// `DropCollection`, for the same reason.
    #[test]
    fn a_snapshot_carrying_a_drop_older_than_the_incarnation_held_here_is_ignored() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let dropped = a.collection_dropped_at(ca.id).unwrap().expect("a tombstone");

        a_moment();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": 1 }).unwrap();
        assert!(cb.created > dropped.hlc, "the fixture must create after the sender's drop");

        let page = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert_eq!(page.dropped, Some(dropped));
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(ca.id), &page)
            .unwrap();

        assert_eq!(b.get_collection("shop", "orders").unwrap().created, cb.created);
        assert_eq!(b.count(&cb).unwrap(), 1, "the newer incarnation and its documents stand");
    }

    /// The drop landing **between two pages** of a repair, which is where it
    /// lands whenever a repair runs long enough to matter.
    ///
    /// The resumed page carries it, so the receiver's partial copy of the
    /// dropped incarnation goes with it. Before this, that page came back with
    /// no definition, no documents, no drop and no cursor — the same thing a
    /// snapshot that had run out looks like — so the pull reported itself
    /// complete and left the receiver advertising a collection the cluster had
    /// agreed to delete, with no tombstone of its own to stop it being served
    /// or re-seeded.
    #[test]
    fn a_scoped_snapshot_resumed_after_its_sender_dropped_the_collection_carries_the_drop() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 88) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        let mut progress = SnapshotProgress::of_collection(ca.id);
        let first = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert!(first.next.is_some(), "the fixture needs a page left to resume");
        let outcome = b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap();
        assert_eq!(outcome.applied, SNAPSHOT_PAGE, "{outcome:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap() as usize, SNAPSHOT_PAGE, "the partial copy is here");
        // Read after the first page, which created the collection: what must
        // not move is B's own coverage, because a replicated drop mints no
        // entry of the receiver's own.
        let b_own = b.version_vector().unwrap().get(b.node_id());

        // Between the pages.
        a.drop_collection("shop", "orders").unwrap();
        let dropped = a.collection_dropped_at(ca.id).unwrap().expect("a tombstone");

        let resumed = a.snapshot_page(progress.after().cloned(), Some(ca.id)).unwrap();
        assert_eq!(resumed.dropped, Some(dropped), "a resumed page carries the drop too");
        assert!(resumed.collections.is_empty(), "definitions still ride the first page alone");
        assert!(resumed.documents.is_empty() && resumed.next.is_none(), "{resumed:?}");
        b.apply_snapshot_page(a.node_id(), &mut progress, &resumed).unwrap();
        assert!(progress.is_complete());

        assert!(
            b.get_collection("shop", "orders").is_err(),
            "the partial copy of the dropped incarnation goes with the drop"
        );
        assert_eq!(b.count_by_id(ca.id).unwrap(), None, "and its documents with it");
        assert_eq!(
            b.collection_dropped_at(ca.id).unwrap(),
            Some(dropped),
            "at the sender's stamp, so a recreation that follows it is still usable"
        );
        assert_eq!(b.version_vector().unwrap().get(b.node_id()), b_own, "no entry of its own");

        // The whole-database route names nothing in `dropped` on any page --
        // that field is the scoped route's -- and carries its denials in
        // `dropped_collections` on every page, resumed ones included
        // (ADR-162). Asked with the first page's cursor rather than
        // `progress.after()`, which is `None` by now: the resumed arm is the
        // one this could regress, and a first page is pinned elsewhere in this
        // file.
        let resumed = a.snapshot_page(first.next.clone(), None).unwrap();
        assert_eq!(resumed.dropped, None);
        assert!(
            resumed.dropped_collections.iter().any(|(id, _)| *id == ca.id),
            "a resumed whole-database page must deny too, or a drop that lands \
             mid-transfer is lost: {:?}",
            resumed.dropped_collections
        );
    }

    /// A tombstone alone cannot stand for "the collection is gone": a
    /// collection recreated after a drop keeps the tombstone that floored it.
    /// A resumed page that read the tombstone without checking would tell a
    /// receiver holding an older incarnation to destroy the very copy this
    /// snapshot is filling.
    #[test]
    fn a_resumed_scoped_page_of_a_collection_recreated_after_a_drop_carries_no_drop() {
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let ca = a.create_collection("shop", "orders").unwrap();
        assert!(ca.incarnation_floor.is_some(), "the tombstone survives the recreation");
        for i in 0..(SNAPSHOT_PAGE + 1) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        let first = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert_eq!(first.dropped, None);
        assert_eq!(first.collections.len(), 1, "the definition rides the first page");
        let resumed = a.snapshot_page(first.next.clone(), Some(ca.id)).unwrap();
        assert_eq!(resumed.dropped, None, "the sender holds it; a floor is not news");
        assert!(resumed.collections.is_empty(), "definitions still ride the first page alone");
        assert_eq!(resumed.documents.len(), 1, "and the page it was asked for still arrives");
    }

    /// Two nodes minting the same `Hlc` in the same millisecond, with the
    /// create's node sorting above the drop's. `low` stamps the drop, `high`
    /// the create.
    fn tied_nodes() -> (kimmy_core::NodeId, kimmy_core::NodeId) {
        let low = kimmy_core::NodeId::from_bytes([1; 16]);
        let high = kimmy_core::NodeId::from_bytes([2; 16]);
        assert!(high > low, "the fixture depends on the node ids breaking the tie this way");
        (low, high)
    }

    /// A collection standing at exactly its own incarnation floor, which is
    /// what a create that beat a same-millisecond drop on the node id leaves:
    /// `apply_ddl`'s creation rule compares `Stamp` strictly, so the create is
    /// not history and is applied, taking `created` from its origin and the
    /// floor from the tombstone that survived it. Both are the same `Hlc`.
    ///
    /// Returned with the drop's stamp, which is the one a peer that applied
    /// the drop and never saw the create still answers a repair with, and
    /// with that same drop as the oplog entry the other route delivers.
    fn at_its_own_floor(b: &Engine, wall: u64) -> (CollectionMeta, Stamp, OplogEntry) {
        let (low, high) = tied_nodes();
        let tie = Hlc::new(wall, 0);
        let dropped = Stamp::new(tie, low);

        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let entries = a.entries_for_peer(Hlc::ZERO, 10).unwrap().entries;
        let of = |kind| {
            entries.iter().find(|e| e.kind == kind).cloned().expect("the entry the drop minted")
        };
        let create = of(OpKind::CreateCollection);
        let drop = OplogEntry { stamp: dropped, ..of(OpKind::DropCollection) };

        b.record_collection_drop(ca.id, dropped).unwrap();
        b.apply_batch(&[OplogEntry { stamp: Stamp::new(tie, high), ..create }]).unwrap();

        let current = b.get_collection("shop", "orders").unwrap();
        assert_eq!(current.created, tie, "the create was applied under its own origin stamp");
        assert_eq!(current.incarnation_floor, Some(tie), "and over the tombstone that survived");
        (current, dropped, drop)
    }

    /// The tie the whole change is built around, at the drop's door: a
    /// collection whose `created` *equals* its floor, and the drop that
    /// produced that floor arriving again through a repair.
    ///
    /// The two routes have to agree, and only the floor clause makes them:
    /// the drop neither predates the incarnation standing here nor is newer
    /// than it, so a comparison against `created` alone reads it as aimed at
    /// this copy and destroys a collection the replicated `DropCollection`
    /// arm defends. Reachable, not merely constructible — a repair planned
    /// while this node lacked the collection runs a round later, after the
    /// drop and the create have both landed.
    #[test]
    fn a_snapshot_carrying_the_drop_a_collection_was_created_over_is_ignored() {
        let (b, _db) = engine();
        let (current, dropped, as_an_entry) = at_its_own_floor(&b, 1_000);
        b.insert(&current, doc! { "_id": 1 }).unwrap();

        let page = SnapshotPage {
            collections: Vec::new(),
            documents: Vec::new(),
            next: None,
            versions: VersionVector::new(),
            dropped: Some(dropped),
            dropped_collections: Vec::new(),
        };
        b.apply_snapshot_page(no_sender(), &mut SnapshotProgress::of_collection(current.id), &page)
            .unwrap();

        assert_eq!(
            b.get_collection("shop", "orders").unwrap().created,
            current.created,
            "the incarnation the drop was already accounted for must stand"
        );
        assert_eq!(b.count(&current).unwrap(), 1, "and its documents with it");

        // The same drop down the other route, on the same state: the two
        // answers are one predicate, so they cannot differ.
        let outcome = b.apply_batch(std::slice::from_ref(&as_an_entry)).unwrap();
        assert_eq!(outcome.ddl, 1, "{outcome:?}");
        assert_eq!(b.get_collection("shop", "orders").unwrap().created, current.created);
        assert_eq!(b.count(&current).unwrap(), 1, "however the drop reaches this node");
    }

    /// The floor half of `Engine::is_history` on the snapshot route, with the
    /// tombstone half unable to account for it: the document is stamped in the
    /// same millisecond as the drop by a node sorting above the one that
    /// stamped it, so it sorts *after* the tombstone as a `Stamp` and only the
    /// floor — `Hlc`, and not strict — turns it away. A last pre-drop write
    /// racing the drop is exactly how it arrives.
    #[test]
    fn a_snapshot_document_at_the_incarnation_floor_is_history_though_it_outranks_the_tombstone() {
        let (b, _db) = engine();
        let (low, high) = tied_nodes();
        let tie = Hlc::new(1, 0);
        let id = CollectionId::derive("shop", "orders");
        b.record_collection_drop(id, Stamp::new(tie, low)).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        assert_eq!(cb.incarnation_floor, Some(tie));
        assert!(Stamp::new(tie, high) > Stamp::new(tie, low), "the tombstone cannot turn it away");

        let document = |wall: u64, key: i64| SnapshotDoc {
            collection: id,
            id: DocId::Int64(key),
            stamp: Stamp::new(Hlc::new(wall, 0), high),
            body: Some(bson::serialize_to_vec(&doc! { "_id": key }).unwrap()),
        };
        let page = SnapshotPage {
            collections: Vec::new(),
            // At the floor, and one above it: the floor is a boundary, not a
            // gate on the collection.
            documents: vec![document(1, 1), document(2, 2)],
            next: None,
            versions: VersionVector::new(),
            dropped: None,
            dropped_collections: Vec::new(),
        };
        let outcome = b
            .apply_snapshot_page(no_sender(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        assert_eq!((outcome.applied, outcome.superseded), (1, 1), "{outcome:?}");
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_none(), "the previous life stays buried");
        assert!(b.get(&cb, &DocId::Int64(2)).unwrap().is_some(), "the one above the floor lands");
    }

    #[test]
    fn a_snapshot_makes_a_node_beyond_the_horizon_able_to_catch_up() {
        // The failure this exists for: A's oplog is collected, so B asking for
        // history receives nothing it can apply and never advances.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..20i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        a.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            crate::gc::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        // Incremental sync cannot help.
        let from = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        assert!(!a.can_serve_from_oplog(from).unwrap(), "a should know it cannot serve this");

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap(), 20);
        assert!(
            b.version_vector().unwrap().covers(&a.version_vector().unwrap()),
            "after a snapshot the receiver must stop asking for collected history"
        );
    }

    #[test]
    fn a_snapshot_does_not_overwrite_a_newer_local_version() {
        // Last-writer-wins still decides. A snapshot is state, not authority.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1, "v": "older" }).unwrap();

        let cb = b.create_collection("shop", "orders").unwrap();
        b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "v": "newer" }, true).unwrap();

        transfer(&b, &a);

        let kept = b.get(&cb, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(kept.get_str("v").unwrap(), "newer", "a snapshot must not undo a newer write");
    }

    #[test]
    fn absorbing_coverage_keeps_the_receivers_own_writes() {
        // The receiver may hold writes the sender never saw; adopting the
        // sender's vector outright would claim it had forgotten them.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "local-only" }).unwrap();
        let b_own = b.version_vector().unwrap().get(b.node_id());

        transfer(&b, &a);

        assert_eq!(
            b.version_vector().unwrap().get(b.node_id()),
            b_own,
            "the receiver's own coverage must survive"
        );
    }

    #[test]
    fn a_completed_snapshot_survives_a_restart() {
        // The version vector was derived from the oplog, which would have
        // recomputed the granted coverage away on the next open and sent the
        // node back to asking for history it cannot be given.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        a.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            crate::gc::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        let granted = {
            let b = Engine::open(&path).unwrap();
            transfer(&b, &a);
            b.version_vector().unwrap()
        };

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(
            reopened.version_vector().unwrap(),
            granted,
            "a restart must not undo coverage a snapshot granted"
        );
    }

    /// A sender with more documents than one snapshot page carries, so a
    /// single `apply_snapshot_page` leaves the transfer genuinely unfinished.
    fn sender_of_two_pages() -> (Engine, tempfile::TempDir) {
        let (a, da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 4) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        (a, da)
    }

    #[test]
    fn a_restart_part_way_through_a_snapshot_does_not_claim_the_coverage() {
        // The defect ADR-160 closes, and the case ADR-152 recorded as untested.
        //
        // A snapshot document is appended under `Position::Hold` precisely
        // because the receiver holds it as STATE and cannot serve a contiguous
        // window containing it. `Engine::open` then rebuilt the vector from the
        // oplog and raised the position over it anyway -- it could not tell a
        // document held as state from one held as history. The node came back
        // up claiming to be able to serve entries it had never seen, and the
        // hole ADR-148 forbids was opened by a restart rather than by a page.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let (a, _da) = sender_of_two_pages();

        // One page only: the snapshot is left in flight, which is what a
        // restart mid-repair leaves behind.
        let mut progress = SnapshotProgress::whole_database();
        let stopped = {
            let b = Engine::open(&path).unwrap();
            let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
            assert!(!progress.is_complete(), "the fixture must leave the snapshot unfinished");
            assert!(b.held_len().unwrap() > 0, "an unfinished snapshot holds entries as state");
            b.version_vector().unwrap()
        };

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(
            reopened.version_vector().unwrap(),
            stopped,
            "a restart must not raise the position over documents held as state"
        );
        assert!(
            reopened.version_vector().unwrap().get(a.node_id())
                < a.version_vector().unwrap().get(a.node_id()),
            "the receiver must still be behind the sender it has not finished copying"
        );
    }

    #[test]
    fn a_completed_snapshot_releases_the_marks_its_grant_covers() {
        // NOT "every mark it made". The grant is the FIRST page's vector, so a
        // document written on the sender after that vector was read and still
        // ahead of the cursor arrives at a stamp above it and keeps its mark
        // through a snapshot that completed perfectly --
        // `a_document_written_during_a_snapshot_keeps_its_mark` below is that
        // case. This fixture has no writer running against the sender, so
        // every stamp is at or below the grant and the table does empty; the
        // name used to claim the general guarantee and the fixture could not
        // have contradicted it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let granted = {
            let b = Engine::open(&path).unwrap();
            transfer(&b, &a);
            assert_eq!(b.held_len().unwrap(), 0, "a completed snapshot leaves nothing held");
            b.version_vector().unwrap()
        };

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(
            reopened.version_vector().unwrap(),
            granted,
            "a restart must not undo coverage a snapshot granted"
        );
    }

    #[test]
    fn a_document_written_during_a_snapshot_keeps_its_mark() {
        // The limit of what a grant releases, and the case the fixture above
        // cannot produce. The coverage a whole-database snapshot grants is the
        // vector served with its FIRST page; `snapshot_page` re-reads live
        // state per page, so a document written after that vector was read and
        // still ahead of the cursor is carried at a stamp ABOVE the grant.
        //
        // Its mark is correctly NOT released: the receiver holds that document
        // as state and the grant says nothing about it. What was wrong was the
        // claim that a completed snapshot empties the table.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();
        let ca = a.get_collection("shop", "orders").unwrap();

        let b = Engine::open(&path).unwrap();
        let mut progress = SnapshotProgress::whole_database();

        // Page one fixes the grant.
        let first = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap();

        // The sender writes while the snapshot is still running, at a stamp
        // above the grant, and beyond the cursor so the next page carries it.
        a.insert(&ca, doc! { "_id": 99_999 }).unwrap();

        while !progress.is_complete() {
            let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        }

        assert!(
            b.held_len().unwrap() > 0,
            "a document written above the grant keeps its mark through a completed snapshot"
        );
        // And the node is honest about it rather than broken by it: it holds
        // the document, and does not claim a position it cannot serve from.
        assert!(b.get(&ca, &DocId::Int64(99_999)).unwrap().is_some());
        assert!(
            b.version_vector().unwrap().get(a.node_id())
                < a.version_vector().unwrap().get(a.node_id()),
            "the grant does not reach the write that came after it"
        );
    }

    #[test]
    fn finishing_an_interrupted_snapshot_releases_what_the_restart_kept() {
        // The two halves together: the marks survive the restart, and the
        // completion that follows clears them. Without this the confinement
        // would be a one-way door.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let mut progress = SnapshotProgress::whole_database();
        {
            let b = Engine::open(&path).unwrap();
            let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
            assert!(b.held_len().unwrap() > 0);
        }

        // The resume carries the same progress, which is what persisting it
        // buys a real node; here it is simply still in hand.
        let b = Engine::open(&path).unwrap();
        transfer_under(&b, &a, &mut progress);
        assert_eq!(b.held_len().unwrap(), 0, "completion must release what the restart kept");
        assert_eq!(
            b.version_vector().unwrap().get(a.node_id()),
            a.version_vector().unwrap().get(a.node_id()),
            "and the receiver is caught up once it is"
        );
    }

    #[test]
    fn re_delivering_documents_a_snapshot_already_applied_does_not_claim_them() {
        // The honest consequence of the confinement, asserted rather than
        // wished away.
        //
        // An unfinished snapshot's documents are held as state. If the entries
        // path then re-delivers the same stamps, last-writer-wins supersedes
        // every one of them -- the document is already at that stamp -- so
        // nothing is appended, the servable vector does not move, and the
        // marks stay. That is correct: this node still cannot serve a
        // contiguous window containing them. Before ADR-160 a restart raised
        // the position over them anyway and the node claimed a window it could
        // not serve.
        //
        // What must NOT happen is the ADR-054 failure: re-requesting forever.
        // The witnessed vector is what "am I behind" is judged on, and the
        // sync path absorbs the window's coverage into it whether or not
        // anything was appended -- so the node goes quiet while staying
        // honest about what it can serve.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let mut progress = SnapshotProgress::whole_database();
        let b = Engine::open(&path).unwrap();
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        let held = b.held_len().unwrap();
        assert!(held > 0, "the fixture must leave something held");

        let window = a.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
        b.apply_peer_batch(&a.version_vector().unwrap(), &window.entries, window.scanned_to, true)
            .unwrap();

        assert_eq!(
            b.held_len().unwrap(),
            held,
            "supersession releases no mark: nothing was appended, so nothing arrived in position"
        );
        // And yet the node is caught up, because the window was contiguous
        // from ZERO and its later entries WERE appended in position. A version
        // vector is a per-origin high-water mark, so raising it to the last
        // entry covers every earlier stamp from that origin -- the marks left
        // behind are stale rather than wrong, which is safe for the reason
        // `a_stale_mark_can_never_lower_a_vector_that_already_covers_it`
        // asserts, and retention collects them with their entries.
        assert_eq!(
            b.version_vector().unwrap().get(a.node_id()),
            a.version_vector().unwrap().get(a.node_id()),
            "a contiguous window from the beginning does catch the node up"
        );
    }

    #[test]
    fn a_stale_mark_can_never_lower_a_vector_that_already_covers_it() {
        // The bound on how much damage a mark can do: the open-time rebuild
        // MERGES into the stored vector and never lowers it, so a mark can only
        // withhold a raise. A stamp the stored vector already covers stays
        // covered however it got there.
        //
        // That is a bound, not a licence. Withholding a raise is exactly what
        // bites in the case the rebuild exists for -- a stored vector lost or
        // disagreeing with the oplog -- which is why `release_held_under` runs
        // on every grant rather than leaving marks to be tidied later.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let covered = {
            let b = Engine::open(&path).unwrap();
            transfer(&b, &a);
            b.version_vector().unwrap()
        };

        // Mark every entry as state after the fact -- a state no release path
        // can produce, which is the point: even then the vector must hold.
        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let oplog = txn.open_table(crate::tables::OPLOG).unwrap();
                let mut held = txn.open_table(crate::tables::OPLOG_HELD).unwrap();
                for row in oplog.iter().unwrap() {
                    let (key, _) = row.unwrap();
                    held.insert(key.value(), ()).unwrap();
                }
            }
            txn.commit().unwrap();
        }

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.version_vector().unwrap(), covered);
    }

    #[test]
    fn a_database_with_no_held_table_opens_as_it_always_did() {
        // An older build, or a restore: the backup does not carry this table,
        // and neither does a database written before it existed. Opening must
        // fall back to counting every entry -- the behaviour ADR-160 narrows,
        // and the safe direction of the two.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let expected = {
            let e = Engine::open(&path).unwrap();
            let c = e.create_collection("db", "c").unwrap();
            e.insert(&c, doc! { "_id": 1 }).unwrap();
            e.version_vector().unwrap()
        };

        // Remove the table entirely, as a database that never had one.
        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.delete_table(crate::tables::OPLOG_HELD).unwrap();
            {
                let mut versions = txn.open_table(crate::tables::OPLOG_VERSIONS).unwrap();
                versions.retain(|_, _| false).unwrap();
            }
            txn.commit().unwrap();
        }

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.version_vector().unwrap(), expected);
    }

    #[test]
    fn retention_collects_a_mark_with_the_entry_it_names() {
        // The third release path, untested until now. A mark on an entry the
        // oplog no longer holds could never be collected afterwards -- the
        // removal is gated on the oplog row being there -- so it would be a
        // permanent orphan.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let b = Engine::open(&path).unwrap();
        let mut progress = SnapshotProgress::whole_database();
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert!(b.held_len().unwrap() > 0, "the fixture must leave marks to collect");

        let before = b.held_len().unwrap();
        b.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            crate::gc::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        assert!(b.held_len().unwrap() < before, "retention must take marks with the entries");
        assert_eq!(
            b.held_orphans().unwrap(),
            0,
            "and must never leave one naming an entry it removed: nothing could collect it after"
        );
    }

    #[test]
    fn a_rewind_discards_marks_with_the_entries_and_does_not_raise_over_the_rest() {
        // Rewind is the only remover of oplog rows outside retention, and it
        // is wrong in two directions at once if it ignores the marks: the ones
        // it strands can never be collected, and
        // `reset_version_vector_to_oplog` REPLACES the vectors rather than
        // merging, so counting a held entry there does not merely fail to
        // withhold a raise -- it writes the claim. ADR-160, entered by another
        // door.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let b = Engine::open(&path).unwrap();
        let mut progress = SnapshotProgress::whole_database();
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        let held = b.held_len().unwrap();
        assert!(held > 0);

        // A snapshot document is a document whose earlier value was never in
        // this node's oplog, so a rewind below it is REFUSED outright rather
        // than discarding it -- which narrows how a held entry can be
        // discarded at all, and is worth knowing.
        let refused = b.rewind_to(Hlc::ZERO);
        assert!(
            refused.is_err(),
            "a rewind below a document that exists only as snapshot state cannot be honoured"
        );

        // What CAN discard one: a document this node already held at an
        // earlier stamp, replaced by a snapshot document, rewound to between
        // the two. The earlier value is in the oplog, so the rewind is
        // allowed, and the mark on the discarded entry must go with it.
        let cb = b.get_collection("shop", "orders").unwrap();
        let own = b.insert(&cb, doc! { "_id": 424_242 }).unwrap();
        let mid = b.version_vector().unwrap().get(b.node_id());
        let page = SnapshotPage {
            collections: Vec::new(),
            documents: vec![SnapshotDoc {
                collection: cb.id,
                id: DocId::Int64(424_242),
                stamp: Stamp::new(a.version_vector().unwrap().get(a.node_id()), a.node_id()),
                body: Some(bson::serialize_to_vec(&doc! { "_id": 424_242, "v": 2 }).unwrap()),
            }],
            next: None,
            versions: VersionVector::new(),
            dropped: None,
            dropped_collections: Vec::new(),
        };
        let _ = own;
        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

        b.rewind_to(mid).unwrap();
        assert_eq!(
            b.held_orphans().unwrap(),
            0,
            "a mark on a discarded entry can never be collected, so it must go with it"
        );
    }

    #[test]
    fn a_rewind_that_keeps_a_held_entry_does_not_claim_it() {
        // The half the test above cannot show, because it discards everything:
        // a rewind whose cut-off leaves held entries standing must still not
        // raise the position over them.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let b = Engine::open(&path).unwrap();
        let mut progress = SnapshotProgress::whole_database();
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        assert!(b.held_len().unwrap() > 0);

        // Far in the future: nothing is discarded, so every mark stands.
        let far = a.version_vector().unwrap().get(a.node_id());
        b.rewind_to(far).unwrap();

        assert!(b.held_len().unwrap() > 0, "nothing was discarded, so nothing was collected");
        assert_eq!(
            b.version_vector().unwrap().get(a.node_id()),
            Hlc::ZERO,
            "the reset must skip held entries exactly as the open-time rebuild does"
        );
    }

    #[test]
    fn an_interrupted_snapshot_is_recorded_and_resumes_at_its_last_page() {
        // Before ADR-161 the progress lived only in a map on the cluster
        // transport, so a member restarted part-way through a large snapshot
        // began again at page one and re-transferred everything it had
        // already applied.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let stopped = {
            let b = Engine::open(&path).unwrap();
            let mut progress = SnapshotProgress::whole_database();
            let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
            assert!(!progress.is_complete());
            progress
        };

        // The process is gone; only what was written survives.
        let b = Engine::open(&path).unwrap();
        let recorded = b.snapshots_to_resume().unwrap();
        assert_eq!(recorded.len(), 1, "the pull must be recorded against its peer");
        let (peer, mut resumed) = recorded.into_iter().next().unwrap();
        assert_eq!(peer, a.node_id());
        assert_eq!(resumed, stopped, "and it must be exactly where the page left it");

        // And it finishes from there rather than from page one.
        let more = transfer_under(&b, &a, &mut resumed);
        assert!(resumed.is_complete());
        assert_eq!(
            resumed.documents(),
            (SNAPSHOT_PAGE + 4),
            "the resumed pull accounts for every document, counted once"
        );
        assert!(more < SNAPSHOT_PAGE, "a resume that re-sent page one would have carried it again");
    }

    #[test]
    fn a_completed_snapshot_leaves_no_record_to_resume() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let b = Engine::open(&path).unwrap();
        transfer(&b, &a);
        assert!(
            b.snapshots_to_resume().unwrap().is_empty(),
            "a finished snapshot has nothing to resume, and a record of one would restart it"
        );
    }

    /// A whole-database pull, one page in and not complete, plus a scoped
    /// repair against the same peer run to completion. The pairing is not
    /// contrived: a node below a peer's retention horizon is exactly the node
    /// whose divergence check is firing repairs at that same peer.
    fn a_whole_database_pull_and_a_scoped_repair_at_one_peer()
    -> (Engine, tempfile::TempDir, Engine, tempfile::TempDir, SnapshotProgress) {
        let (a, da) = engine();
        let (b, db) = engine();
        let big = a.create_collection("shop", "big").unwrap();
        for i in 0..(SNAPSHOT_PAGE * 2) as i64 {
            a.insert(&big, doc! { "_id": i }).unwrap();
        }
        let mut whole = SnapshotProgress::whole_database();
        let first = a.snapshot_page(None, None).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut whole, &first).unwrap();
        assert!(!whole.is_complete(), "the whole-database pull must still be under way");
        assert_eq!(
            b.snapshots_to_resume().unwrap().len(),
            1,
            "and must have recorded a cursor, or there is nothing for the repair to take"
        );
        (a, da, b, db, whole)
    }

    #[test]
    fn a_scoped_repair_completing_leaves_a_whole_database_cursor_alone() {
        // The row is one per peer, because `PeerStalls` holds one pull per
        // peer and `resume_snapshots` inserts by peer. What it must not do is
        // let either pull silently take the other's place: the in-memory half
        // already guarded this -- `snapshot_forgotten` removes a pull only
        // when the scope matches -- and the persisted half did not.
        let (a, _da, b, _db, _whole) = a_whole_database_pull_and_a_scoped_repair_at_one_peer();

        let small = a.create_collection("shop", "small").unwrap();
        a.insert(&small, doc! { "_id": 1 }).unwrap();
        let mut scoped = SnapshotProgress::of_collection(small.id);
        while !scoped.is_complete() {
            let page = a.snapshot_page(scoped.after().cloned(), scoped.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut scoped, &page).unwrap();
        }

        let recorded = b.snapshots_to_resume().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "a repair of one collection must not clear a whole-database pull's cursor; the next \
             start would begin again at page one, which is what ADR-161 exists to prevent"
        );
        assert_eq!(
            recorded[0].1.scope(),
            None,
            "and the surviving cursor is the whole-database one"
        );
        assert_eq!(recorded[0].1.pages(), 1, "standing where the page left it");
    }

    #[test]
    fn a_scoped_repair_does_not_displace_a_whole_database_cursor_by_persisting_over_it() {
        // The other half. Forgetting is not the only way to take the row: a
        // scoped repair that does not complete in one page persists a cursor
        // of its own, and the row holds one.
        let (a, _da, b, _db, _whole) = a_whole_database_pull_and_a_scoped_repair_at_one_peer();

        let small = a.create_collection("shop", "small").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 5) as i64 {
            a.insert(&small, doc! { "_id": i }).unwrap();
        }
        let mut scoped = SnapshotProgress::of_collection(small.id);
        let page = a.snapshot_page(None, Some(small.id)).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut scoped, &page).unwrap();
        assert!(!scoped.is_complete(), "the repair must be mid-pull, or it persists nothing");

        let recorded = b.snapshots_to_resume().unwrap();
        assert_eq!(recorded.len(), 1, "still one row");
        assert_eq!(
            recorded[0].1.scope(),
            None,
            "and it is still the whole-database pull's: the one worth thousands of pages \
             outranks the one worth a few"
        );
    }

    #[test]
    fn a_whole_database_pull_still_clears_its_own_cursor() {
        // The control. Guarding the row by scope must not stop a pull
        // clearing what it owns -- a version that never forgot anything would
        // pass both tests above and leave every completed snapshot behind as a
        // cursor that resumes forever.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let big = a.create_collection("shop", "big").unwrap();
        for i in 0..(SNAPSHOT_PAGE * 2) as i64 {
            a.insert(&big, doc! { "_id": i }).unwrap();
        }
        transfer(&b, &a);
        assert!(
            b.snapshots_to_resume().unwrap().is_empty(),
            "a completed whole-database pull leaves nothing to resume"
        );
    }

    #[test]
    fn a_page_that_wrote_nothing_records_no_cursor() {
        // The bound, asserted: the cursor is persisted in the page's own
        // transaction and only when the page wrote something, so a page this
        // node already holds still costs no fsync (ADR-152's rule). A restart
        // therefore resumes at the last page that WROTE something, and the
        // no-op pages after it are re-pulled -- cheap to redo precisely
        // because they wrote nothing.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = sender_of_two_pages();

        let b = Engine::open(&path).unwrap();
        let mut progress = SnapshotProgress::whole_database();
        let first = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &first).unwrap();
        let after_first = b.snapshots_to_resume().unwrap();
        assert_eq!(after_first.len(), 1);

        // The same page again: every document supersedes, nothing is written.
        let mut replay = SnapshotProgress::whole_database();
        let applied = b.apply_snapshot_page(a.node_id(), &mut replay, &first).unwrap();
        assert_eq!(applied.applied, 0, "the fixture must be a page that writes nothing");
        assert_eq!(
            b.snapshots_to_resume().unwrap(),
            after_first,
            "a page that wrote nothing must leave the record where it was"
        );
    }

    #[test]
    fn a_whole_database_snapshot_takes_a_collection_the_sender_dropped() {
        // The defect ADR-162 closes, and the only open High of the 0.28.0
        // batch. A whole-database page said what the sender HAS and nothing
        // about what it deleted, so a collection the receiver held and the
        // sender had dropped survived the transfer -- and because completing
        // one grants the receiver coverage of the sender's history, the
        // `DropCollection` entry was never served to it afterwards either. The
        // collection stayed live, served and writable, on one member only.
        let (a, _da) = engine();
        let (b, _db) = engine();

        // The receiver takes the collection from the sender first, so it holds
        // the same incarnation the sender is about to drop -- which is the
        // state a real member is in, and the one a tombstone has to be judged
        // against.
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        assert!(b.get_collection("shop", "orders").is_ok(), "the fixture must start with it held");

        // The sender drops it, then the receiver pulls again -- the case a
        // member below a peer's retention horizon is in.
        a.drop_collection("shop", "orders").unwrap();
        transfer(&b, &a);

        assert!(
            b.get_collection("shop", "orders").is_err(),
            "a whole-database snapshot must deny what the sender deleted, not only \
             affirm what it kept"
        );
    }

    #[test]
    fn a_whole_database_snapshot_does_not_destroy_a_newer_incarnation() {
        // The other half, and the reason the denials go through
        // `restore_collection_drop` rather than being applied as they arrive:
        // a tombstone alone cannot stand for "the collection is gone". A
        // collection recreated after a drop keeps the tombstone that floored
        // it, and a receiver holding the NEWER life must not be told to
        // destroy it by a sender still carrying the older one's headstone.
        let (a, _da) = engine();
        let (b, _db) = engine();

        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        a.drop_collection("shop", "orders").unwrap();
        // Recreated on the sender: it now holds both a live incarnation and
        // the tombstone of the life before it.
        let again = a.create_collection("shop", "orders").unwrap();
        a.insert(&again, doc! { "_id": 2 }).unwrap();
        // And the RECEIVER already holds that same newer life, which is the
        // state this test is named for. Taking it from the sender first is the
        // only way to hold it: an empty receiver would gain the collection
        // from the page under test, and `restore_collection` would take the
        // create path -- which is a different case, and the one this fixture
        // used to exercise while claiming to exercise this one.
        transfer(&b, &a);
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().created,
            again.created,
            "the fixture must start with the receiver holding the newer life"
        );
        assert!(
            a.collections_dropped().unwrap().iter().any(|(id, _)| *id == again.id),
            "the fixture must leave the sender holding the older life's tombstone"
        );

        transfer(&b, &a);

        assert!(
            b.get_collection("shop", "orders").is_ok(),
            "the drop is aimed at a life that has already ended and must be ignored"
        );
        assert!(
            b.get(&b.get_collection("shop", "orders").unwrap(), &DocId::Int64(2))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn a_page_that_predates_the_field_denies_nothing_and_breaks_nothing() {
        // Wire compatibility, asserted rather than assumed: the field is
        // defaulted, so a page from a sender that predates it deserialises
        // with an empty list and the receiver keeps what it holds -- the old
        // behaviour, which is what a mixed-version cluster must see until both
        // ends have rolled.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        assert!(b.get_collection("shop", "orders").is_ok());
        a.drop_collection("shop", "orders").unwrap();

        let mut progress = SnapshotProgress::whole_database();
        while !progress.is_complete() {
            let mut page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            // What an older sender puts on the wire.
            page.dropped_collections.clear();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        }

        assert!(
            b.get_collection("shop", "orders").is_ok(),
            "an older sender denies nothing, so the receiver keeps what it has"
        );
    }

    #[test]
    fn a_receiver_holding_the_previous_life_gets_the_one_the_sender_recreated() {
        // The case the order of denials and definitions decides, and the one
        // that makes them inseparable.
        //
        // The sender dropped and recreated the collection, so its page carries
        // a definition of the NEW life and a tombstone for the OLD one. The
        // receiver holds the old life. Restore definitions first and
        // `restore_collection` finds the name present and does nothing -- it
        // does not compare incarnations, which is ADR-155's second residual --
        // and the tombstone then takes the life it found: the receiver ends
        // with NO collection, permanently, because a completed whole-database
        // snapshot grants coverage and the `CreateCollection` entry is never
        // served to it either.
        //
        // Denials first, and it converges: the old life goes, the name is then
        // absent, and the new life is created above the tombstone just written.
        let (a, _da) = engine();
        let (b, _db) = engine();

        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        let life1 = b.get_collection("shop", "orders").unwrap().created;

        a.drop_collection("shop", "orders").unwrap();
        let life2 = a.create_collection("shop", "orders").unwrap();
        a.insert(&life2, doc! { "_id": 2 }).unwrap();
        assert_ne!(life1, life2.created, "the fixture must be two distinct lives");

        transfer(&b, &a);

        let held = b
            .get_collection("shop", "orders")
            .expect("the receiver must end holding the recreated collection, not nothing");
        assert_eq!(held.created, life2.created, "and it must be the life the sender has");
        assert!(
            b.get(&held, &DocId::Int64(1)).unwrap().is_none(),
            "the previous life's document stays buried"
        );
        assert!(
            b.get(&held, &DocId::Int64(2)).unwrap().is_some(),
            "and the new life's document lands"
        );
    }

    #[test]
    fn a_scoped_page_carries_no_whole_database_tombstone_list() {
        // The sender-side gate. A scoped pull is about one collection and says
        // so in `dropped`; carrying the sender's whole tombstone table would
        // hand a repair authority over collections it was never asked about.
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let other = a.create_collection("shop", "other").unwrap();
        a.insert(&other, doc! { "_id": 1 }).unwrap();
        a.drop_collection("shop", "other").unwrap();
        assert!(!a.collections_dropped().unwrap().is_empty(), "the sender must hold one to leak");

        let scoped = a.snapshot_page(None, Some(ca.id)).unwrap();
        assert!(
            scoped.dropped_collections.is_empty(),
            "a scoped page must deny nothing beyond its own collection: {:?}",
            scoped.dropped_collections
        );
    }

    #[test]
    fn a_scoped_pull_ignores_a_tombstone_list_it_is_handed() {
        // The receiver-side gate, which is the half that matters: the sender
        // is not the only thing that can put a list on a page. A hostile or
        // buggy peer must not be able to make a repair drop collections the
        // pull was never about.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);

        let victim = b.get_collection("shop", "orders").unwrap();
        let mut page = a.snapshot_page(None, Some(ca.id)).unwrap();
        // A denial of the very collection the receiver holds, at a stamp above
        // its incarnation -- which on the whole-database route would take it.
        page.dropped_collections =
            vec![(victim.id, Stamp::new(Hlc::new(u64::MAX, 0), a.node_id()))];

        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(ca.id), &page)
            .unwrap();

        assert!(
            b.get_collection("shop", "orders").is_ok(),
            "a scoped pull must ignore a tombstone list, whoever put it on the page"
        );
    }

    #[test]
    fn a_whole_database_page_ignores_a_denial_its_senders_coverage_does_not_name() {
        // The same gate as the scoped route above, on the route that has
        // something far worse to lose. `a_scoped_pull_ignores_a_tombstone_list`
        // states the reason -- the sender is not the only thing that can put a
        // list on a page -- and the scoped route earns it structurally, by
        // taking the id from `progress.scope`. This route takes the id AND the
        // stamp from the wire, so the only thing standing between an arbitrary
        // pair and a purge is what the sender's own coverage vouches for.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        // A collection the sender has never heard of, so the page cannot be
        // read as a legitimate denial of anything it knows.
        let local = b.create_collection("shop", "local-only").unwrap();
        b.insert(&local, doc! { "_id": 1 }).unwrap();

        let victim = b.get_collection("shop", "orders").unwrap();
        let mut page = a.snapshot_page(None, None).unwrap();
        assert!(
            page.collections
                .iter()
                .any(|state| CollectionId::derive(&state.db, &state.name) == victim.id),
            "the fixture must deny a collection the same page defines, which is the case that \
             shows the denial is not a stale echo of something the sender dropped"
        );
        page.dropped_collections = vec![
            (victim.id, Stamp::new(Hlc::new(u64::MAX, 0), a.node_id())),
            (local.id, Stamp::new(Hlc::new(u64::MAX, 0), a.node_id())),
        ];

        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

        assert!(
            b.get_collection("shop", "orders").is_ok(),
            "a denial above the sender's own coverage must not take a collection"
        );
        assert!(
            b.get_collection("shop", "local-only").is_ok(),
            "least of all one the sender has never held"
        );
        // The half that makes it unrecoverable rather than merely wrong: at
        // `Hlc::MAX` no stamp the cluster can ever mint clears the tombstone,
        // and `gc` expires on `stamp.hlc < cutoff`, so it is never collected
        // either. Recording one would be permanent.
        assert!(
            b.collections_dropped().unwrap().is_empty(),
            "and it must not be recorded, or it outlives every recovery and rides every page \
             this node serves afterwards: {:?}",
            b.collections_dropped().unwrap()
        );
    }

    #[test]
    fn a_denial_the_senders_coverage_names_is_still_honoured() {
        // The gate must discriminate. A test that only proves denials are
        // refused is equally passed by refusing all of them -- which would
        // reinstate the defect ADR-162 exists to close, quietly.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let doomed = a.create_collection("shop", "doomed").unwrap();
        a.insert(&doomed, doc! { "_id": 1 }).unwrap();
        transfer(&b, &a);
        assert!(b.get_collection("shop", "doomed").is_ok(), "the receiver must hold it to lose it");

        a.drop_collection("shop", "doomed").unwrap();
        let page = a.snapshot_page(None, None).unwrap();
        let (_, stamp) = page.dropped_collections[0];
        assert!(
            stamp.hlc <= page.versions.get(stamp.node),
            "a sender's own drop is absorbed in position, so its coverage names it: {stamp:?} vs \
             {:?}",
            page.versions.get(stamp.node)
        );

        b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

        assert!(
            b.get_collection("shop", "doomed").is_err(),
            "a denial within the sender's coverage still takes the collection"
        );
    }

    #[test]
    fn a_denial_the_gate_refuses_is_still_delivered_by_the_entries_path() {
        // Why the gate can be this strict without reopening ADR-162's hole.
        //
        // A node relaying a snapshot mid-pull holds tombstones it has not yet
        // absorbed coverage for, so its own page carries denials its `versions`
        // does not name and the gate refuses them. That is not a lost drop:
        // what made ADR-162's defect permanent was the COVERAGE GRANT, and a
        // sender that does not cover the drop does not grant coverage of it
        // either. The receiver stays behind on that stamp, so the
        // `DropCollection` entry is still owed to it -- and arrives the
        // ordinary way.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();

        let doomed = a.create_collection("shop", "doomed").unwrap();
        a.insert(&doomed, doc! { "_id": 1 }).unwrap();
        transfer(&c, &a);
        assert!(c.get_collection("shop", "doomed").is_ok(), "the victim must hold it to lose it");
        a.drop_collection("shop", "doomed").unwrap();

        // B takes a snapshot from A and stops part-way, so it holds A's
        // tombstone with none of A's coverage. More than one page is what
        // makes the stop real: a single-page snapshot completes and grants.
        let keep = a.create_collection("shop", "keep").unwrap();
        for i in 0..(SNAPSHOT_PAGE + 5) as i64 {
            a.insert(&keep, doc! { "_id": i }).unwrap();
        }
        let mut partial = SnapshotProgress::whole_database();
        let first = a.snapshot_page(None, None).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut partial, &first).unwrap();
        assert!(!partial.is_complete(), "the relay must be mid-pull, or it has absorbed the grant");

        let relayed = b.snapshot_page(None, None).unwrap();
        let (_, stamp) = relayed.dropped_collections[0];
        assert!(
            stamp.hlc > relayed.versions.get(stamp.node),
            "the fixture must produce the refused case, or it proves nothing"
        );

        c.apply_snapshot_page(b.node_id(), &mut SnapshotProgress::whole_database(), &relayed)
            .unwrap();
        assert!(
            c.get_collection("shop", "doomed").is_ok(),
            "the gate refused the denial, as the fixture arranged"
        );

        // And now the entries path, which is what the gate is leaning on.
        let mine = c.version_vector().unwrap();
        let theirs = a.version_vector().unwrap();
        let start = mine.behind(&theirs).expect("the refused drop must still be owed to C");
        let entries = a.entries_for_peer(start, usize::MAX).unwrap().entries;
        c.apply_batch(&entries).unwrap();

        assert!(
            c.get_collection("shop", "doomed").is_err(),
            "a drop the gate refused is still delivered, because refusing it left the receiver \
             behind on the stamp that carries it"
        );
    }

    #[test]
    fn a_carried_tombstone_list_costs_one_writer_and_a_replay_costs_none() {
        // The receiver-side cost of ADR-162, pinned where it can be asserted
        // exactly rather than timed. Before the hoist this took the writer
        // once per tombstone on the first page -- N fsyncs, each taking the
        // writer from live traffic -- and on every page after it paid a full
        // `COLLECTIONS` walk per tombstone to conclude there was nothing to
        // do. ADR-152's rule is that a page a member already holds costs no
        // fsync, and ADR-161 restates it.
        // Asserted as "does not grow with the list" rather than against a
        // fixed number: what is wrong with the per-tombstone version is its
        // shape, and a magic constant here would also be satisfied by a
        // version that took one writer too many for an unrelated reason, while
        // breaking whenever something incidental changed.
        fn cost_of(tombstones: usize) -> (u64, u64) {
            let (a, _da) = engine();
            let (b, _db) = engine();
            for i in 0..tombstones {
                let name = format!("gone-{i}");
                let c = a.create_collection("shop", &name).unwrap();
                a.insert(&c, doc! { "_id": 1 }).unwrap();
                a.drop_collection("shop", &name).unwrap();
            }
            let keep = a.create_collection("shop", "keep").unwrap();
            a.insert(&keep, doc! { "_id": 1 }).unwrap();

            let page = a.snapshot_page(None, None).unwrap();
            assert_eq!(
                page.dropped_collections.len(),
                tombstones,
                "the fixture must carry the whole list, or it measures nothing"
            );

            let before = b.writer_wait().count;
            b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
                .unwrap();
            let first = b.writer_wait().count - before;

            let before = b.writer_wait().count;
            b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
                .unwrap();
            let replay = b.writer_wait().count - before;
            (first, replay)
        }

        let (small_first, small_replay) = cost_of(4);
        let (large_first, large_replay) = cost_of(40);

        assert_eq!(
            small_first, large_first,
            "ten times the tombstones must cost the first page the same number of write \
             transactions; it took {small_first} for 4 and {large_first} for 40"
        );
        // Not zero: the page carries a document, and that transaction is
        // opened whatever the list does. What must be zero is the *list's*
        // share of it, which is what holding the two lengths equal says --
        // this is the page ADR-152 rules must not cost an fsync for what it
        // already holds.
        assert_eq!(
            small_replay, large_replay,
            "a replayed list must add nothing to the page's own cost, whatever its length; it \
             took {small_replay} for 4 and {large_replay} for 40"
        );
    }

    #[test]
    fn a_peer_within_the_window_is_still_served_incrementally() {
        // Snapshots are the fallback, not the default: they transfer everything.
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        assert!(a.can_serve_from_oplog(Hlc::ZERO).unwrap(), "nothing has been collected yet");
    }
}
