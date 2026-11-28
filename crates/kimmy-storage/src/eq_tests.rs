//! Unit EQ (ADR-272): after a schedule of deliveries, at quiescence, THREE comparisons on real engines. **Every member against
//! the register function** of all the facts (`kimmy_core::register_function::fold`): members can all agree with one another and
//! not with it. **Every member against the first**: the function says nothing of document bodies, options rows or names rows.
//! **A swapped name's collection tombstones, of what has ended, against what is wanted of each member** ([`Column::Presence`]).
//!
//! **The oracle never reads a member's kept state.** A [`Ledger`] holds each fact as the entry its author's own operation
//! appended, read back from that author at the step. It also holds which burials a database drop made: nothing on the wire says
//! so. At the end, no member may hold the entry of a fact the ledger lacks.
//!
//! **Floors** (rule 3 of EQ's rows). A name that was never swapped has three items: the collection's own tombstone, the
//! database's, and the effective floor, the later of the two, which is the one the function names. For a SWAPPED name, one whose
//! register the FUNCTION says is not empty, the database's tombstone alone is compared between members: the identity's burial
//! stamp is each member's own (D4-2), and the function's floor is not stored apart from it yet.
//!
//! **Three pins**, each a value wanted that is not the one the function or the members would give, so that any other value fails.
//!
//! **PIN B.** A database drop's burial of a collection is today an entry with a stamp of its own, above the drop, and a member
//! that buries by it keeps that stamp as the collection's floor. So where such a burial sorts above the floor the function names,
//! every member must hold the BURIAL's stamp: exactly that, so that any other floor fails, the function's included. The pin goes
//! with the change that stamps those burials at the drop. It is the only difference the floor, measured over the plain steps,
//! showed: every other column of every set of kinds agreed.
//!
//! **PIN P and the stamp pin** are of the collection tombstones a swapped name leaves ([`Column::Presence`]). Of every stage and
//! recreation the function ends, and of the identity where it does not stand, each view is wanted to HOLD one, but as the two
//! pins say. PIN P: where the register's first event is a binding, which bound the identity out, a member that joins after that
//! binding is in effect is wanted NOT to hold the identity's; one that joins while the binding is held parked is wanted, at the
//! end, to hold it, as the others do. The stamp pin is of a stage that was BOUND and whose next event in the register is a drop
//! of the name: it is wanted at that drop's stamp, and where a recreation follows, a member that joins afterwards is wanted at
//! the recreation's. Every other stage and recreation that ended is wanted held at any stamp. Both pins go with the change that
//! keeps these burial marks apart from the collection tombstone and takes whether there is one, and its stamp, from the facts.
//! The identity's stamp is each member's own and is not held to any.
//!
//! **What is drawn.** The generated run draws the plain collection only: no stage, binding or recreation, no delivery out of causal
//! order, no garbage collection (a tombstone one member has collected and another has not is no difference of state), and no shadow
//! ([`SHADOWS`]). Swapped names are drawn by fixed rows ([`swapped`]). Index definitions are not a compared column; the
//! convergence model compares them.
//!
//! **A physical** of a swapped name, a stage or a recreation, has no oracle of its own: its documents, its document tombstones
//! and its rows are compared between members, for every physical the facts name, an ended one included.
//!
//! A child of the convergence model, whose engines, clock, quiescence and generator it uses and does not change.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use kimmy_core::register_function::{
    Answer, FoldEvent, FoldFact, FoldKind, Serves, fold, is_a_touch,
};
use kimmy_core::{
    CollectionId, DocId, Hlc, OpKind, OplogEntry, RegisterEvent, Stamp, VersionVector,
    database_drop_id,
};
use redb::{ReadableDatabase, ReadableTableMetadata};

use super::{
    A, B, C, Cluster, DB, LETTERS, NAMES, Op, Res, Route, Step, Track, base, env_u64, err,
    generate, model_clock, op, parse, show,
};
use crate::register::{Carried, carried_rows, stored_row_count_for_tests, stored_row_for_tests};
use crate::{
    CollectionMeta, Engine, MarkedRange, PeerWindow, ServeVerdict, SnapshotProgress, StorageError,
    SyncOutcome, WalkScope, codec, tables,
};

/// Whether the vector shadows are compared. They come with the commits that build the shadow's rules; no schedule here makes one.
const SHADOWS: bool = false;

/// Swapped names: fixed schedules, and a stage's creator that lost what it made.
#[path = "eq_swapped_tests.rs"]
mod swapped;

// -- Kinds and the gate ----------------------------------------------------------------------

/// A kind of fact of a name. A database drop's own entries of the collections it buries are part of that database drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Create,
    Touch,
    Drop,
    DatabaseDrop,
    Stage,
    Bind,
    Recreate,
}

type Kinds = BTreeSet<Kind>;

fn kinds_of(facts: &[FoldFact]) -> Kinds {
    facts
        .iter()
        .filter_map(|fact| match &fact.kind {
            FoldKind::Create => Some(Kind::Create),
            FoldKind::Touch => Some(Kind::Touch),
            FoldKind::Drop { database_drop: None } => Some(Kind::Drop),
            FoldKind::Drop { database_drop: Some(_) } => None,
            FoldKind::DatabaseDrop => Some(Kind::DatabaseDrop),
            FoldKind::StageCreate { .. } => Some(Kind::Stage),
            FoldKind::Bind { .. } => Some(Kind::Bind),
            FoldKind::Recreate { .. } => Some(Kind::Recreate),
        })
        .collect()
}

/// The kinds whose arrivals are converted: a name whose facts are all of these kinds is held to the function in every column,
/// and no pin applies to it. None yet; a commit that converts a kind adds it here.
const CONVERTED: &[Kind] = &[];

fn converted(kinds: &Kinds) -> bool {
    kinds.iter().all(|kind| CONVERTED.contains(kind))
}

// -- The ledger ------------------------------------------------------------------------------

/// Every fact's entry, by its stamp; a burial a database drop made carries that drop's `hlc`. And for each collection that is no
/// name's own, a stage's or a recreation's physical, the documents written to it.
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<Stamp, (OplogEntry, Option<Hlc>)>,
    elsewhere: BTreeMap<CollectionId, BTreeSet<i64>>,
}

/// What `entry` is a fact of: the logical name, and the kind of fact. `None` for an entry that is no fact of one of the names:
/// among them a database's drop, which is a fact of every name and is told apart by its collection, a database drop's burial of a
/// physical, and anything else of a physical.
fn name_fact(entry: &OplogEntry) -> Option<(&'static str, FoldKind)> {
    let named = |logical: &str| NAMES.into_iter().find(|name| *name == logical);
    let of_identity =
        || NAMES.into_iter().find(|name| entry.collection == CollectionId::derive(DB, name));
    match entry.kind {
        OpKind::CreateCollection => {
            let body = entry.body.as_ref()?;
            let made: kimmy_core::CollectionCreate =
                bson::deserialize_from_slice(body).expect("a creation's body decodes");
            if made.db != DB {
                None
            } else if let Some(name) = named(&made.name) {
                Some((name, FoldKind::Create))
            } else if let Some(stage) = made.stage {
                let name = named(kimmy_core::swap::logical_of_stage(&made.name)?)?;
                Some((name, FoldKind::StageCreate { physical: made.name, stage }))
            } else {
                let name = named(kimmy_core::logical_of_recreation(&made.name)?)?;
                Some((name, FoldKind::Recreate { physical: made.name }))
            }
        }
        OpKind::BindCollection => {
            let name = of_identity()?;
            let body = entry.body.as_ref()?;
            let bind: kimmy_core::CollectionBind =
                bson::deserialize_from_slice(body).expect("a binding's body decodes");
            Some((name, FoldKind::Bind { physical: bind.physical }))
        }
        OpKind::DropCollection => Some((of_identity()?, FoldKind::Drop { database_drop: None })),
        kind if is_a_touch(kind) => Some((of_identity()?, FoldKind::Touch)),
        _ => None,
    }
}

/// Whether `entry` is a fact of one of the names or of their database.
fn is_a_fact(entry: &OplogEntry) -> bool {
    name_fact(entry).is_some()
        || (entry.kind == OpKind::DropCollection && entry.collection == database_drop_id(DB))
}

/// Every entry in `e`'s oplog.
fn entries_held(e: &Engine) -> Res<Vec<OplogEntry>> {
    let window = e
        .entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background)
        .map_err(|e| err("entries_for_peer", e))?;
    Ok(window.entries)
}

/// The facts in `e`'s oplog.
fn facts_held(e: &Engine) -> Res<Vec<OplogEntry>> {
    Ok(entries_held(e)?.into_iter().filter(is_a_fact).collect())
}

/// The entries `e` itself wrote.
fn own_entries(e: &Engine) -> Res<BTreeMap<Stamp, OplogEntry>> {
    let node = e.node_id();
    Ok(entries_held(e)?
        .into_iter()
        .filter(|entry| entry.stamp.node == node)
        .map(|entry| (entry.stamp, entry))
        .collect())
}

impl Ledger {
    /// Apply a step, and for a local one record what its author appended.
    fn step(&mut self, cluster: &mut Cluster, op: Op) -> Res<()> {
        if !op.step.is_local() {
            return cluster.apply(op);
        }
        let author = Cluster::member_of(op.step);
        let database_drop = matches!(op.step, Step::DropDb { .. });
        self.authored(cluster, author, database_drop, |cluster| cluster.apply(op)).map(drop)
    }

    /// Do `act`, by which member `author` writes, and record what it appended: every fact, and every document of a collection that
    /// is no name's own. Answers the entries it appended.
    fn authored(
        &mut self,
        cluster: &mut Cluster,
        author: usize,
        database_drop: bool,
        act: impl FnOnce(&mut Cluster) -> Res<()>,
    ) -> Res<Vec<OplogEntry>> {
        let before = own_entries(cluster.e(author))?;
        act(&mut *cluster)?;
        let appended: Vec<OplogEntry> = own_entries(cluster.e(author))?
            .into_iter()
            .filter(|(stamp, _)| !before.contains_key(stamp))
            .map(|(_, entry)| entry)
            .collect();
        let database = database_drop_id(DB);
        let head = appended.iter().find(|entry| entry.collection == database).map(|e| e.stamp.hlc);
        for entry in &appended {
            if !is_a_fact(entry) {
                if let Some(DocId::Int64(doc)) = &entry.doc_id {
                    self.elsewhere.entry(entry.collection).or_default().insert(*doc);
                }
                continue;
            }
            let burial = database_drop
                && entry.kind == OpKind::DropCollection
                && entry.collection != database;
            let of = if burial {
                Some(head.ok_or("a database drop's burial without the drop's own entry")?)
            } else {
                None
            };
            if self.entries.insert(entry.stamp, (entry.clone(), of)).is_some() {
                return Err("two facts under one stamp".into());
            }
        }
        Ok(appended)
    }

    /// The facts of `name`: its own, and every drop of the database.
    fn facts(&self, name: &str) -> Vec<FoldFact> {
        let database = database_drop_id(DB);
        self.entries
            .values()
            .filter_map(|(entry, of)| {
                let kind = if entry.collection == database {
                    FoldKind::DatabaseDrop
                } else {
                    match name_fact(entry)? {
                        (of_name, _) if of_name != name => return None,
                        (_, FoldKind::Drop { .. }) => FoldKind::Drop { database_drop: *of },
                        (_, kind) => kind,
                    }
                };
                Some(FoldFact { stamp: entry.stamp, kind })
            })
            .collect()
    }

    /// What the function, and for the documents of a plain name the rule "no lost write, above the floor", say of `name`.
    fn expected(&self, name: &str) -> Expected {
        let facts = self.facts(name);
        let kinds = kinds_of(&facts);
        let answer = fold(name, &facts);
        let id = CollectionId::derive(DB, name);
        // each document's latest event: its stamp, and whether it left the document live
        let mut latest: BTreeMap<i64, (Stamp, bool)> = BTreeMap::new();
        for (stamp, (entry, _)) in &self.entries {
            if entry.collection != id {
                continue;
            }
            let Some(doc) = &entry.doc_id else { continue };
            let DocId::Int64(doc) = doc else { panic!("an id the model never writes: {doc:?}") };
            let live = match entry.kind {
                OpKind::Insert | OpKind::Update | OpKind::Replace => true,
                OpKind::Delete => false,
                kind => panic!("a document entry of kind {kind:?}"),
            };
            // the ledger is in stamp order, so the last one stands
            latest.insert(*doc, (*stamp, live));
        }
        let documents = latest
            .iter()
            .filter(|(_, (stamp, live))| {
                *live && answer.floor.is_none_or(|floor| stamp.hlc > floor.hlc)
            })
            .map(|(doc, (stamp, _))| (*doc, *stamp))
            .collect();
        let burial_above_the_floor = self
            .entries
            .iter()
            .filter(|(_, (entry, of))| entry.collection == id && of.is_some())
            .map(|(stamp, _)| *stamp)
            .max()
            .filter(|burial| answer.floor.is_none_or(|floor| *burial > floor));
        let touched = latest.keys().copied().collect();
        let physicals = facts
            .iter()
            .filter_map(|fact| match &fact.kind {
                FoldKind::StageCreate { physical, .. } | FoldKind::Recreate { physical } => {
                    let id = CollectionId::derive(DB, physical);
                    let written = self.elsewhere.get(&id).cloned().unwrap_or_default();
                    Some((physical.clone(), written))
                }
                _ => None,
            })
            .collect();
        Expected { answer, kinds, documents, touched, burial_above_the_floor, physicals }
    }

    /// The entries of facts a member holds and the ledger was never told of.
    fn unknown_to_it(&self, members: &[&Engine]) -> Res<Vec<String>> {
        let mut unknown = Vec::new();
        for (m, e) in members.iter().enumerate() {
            for entry in facts_held(e)? {
                if !self.entries.contains_key(&entry.stamp) {
                    unknown.push(format!("member {m} holds {:?} at {:?}", entry.kind, entry.stamp));
                }
            }
        }
        Ok(unknown)
    }
}

/// What every member must hold of one name.
#[derive(Clone)]
struct Expected {
    answer: Answer,
    kinds: Kinds,
    /// The live documents of a plain name, each with the stamp of the write it stands at.
    documents: BTreeMap<i64, Stamp>,
    /// Every document any fact of the name wrote or deleted.
    touched: BTreeSet<i64>,
    /// The newest burial a database drop made of the name's collection, where it sorts above the function's floor.
    burial_above_the_floor: Option<Stamp>,
    /// Every stage and recreation the facts name, ended ones too, each with the documents written to it.
    physicals: BTreeMap<String, BTreeSet<i64>>,
}

impl Expected {
    /// The stamp PIN B holds this name's floor at, if it applies.
    fn pinned_at(&self) -> Option<Stamp> {
        if converted(&self.kinds) { None } else { self.burial_above_the_floor }
    }

    /// The floor every member must hold for the name today: PIN B's, or the function's.
    fn floor_today(&self) -> Option<Stamp> {
        self.pinned_at().or(self.answer.floor)
    }
}

// -- The pins of a swapped name's collection tombstones -----------------------------------------

/// What a view is of: PIN P and the stamp pin ask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seat {
    /// A member of the schedule.
    Member,
    /// A member that joined by snapshot with every event of the schedule in effect where it joined from.
    JoinerAfterRelease,
    /// A member that joined by snapshot from one that held an event parked.
    JoinerWhileParked,
}

/// What a view is wanted to hold as the collection tombstone of a collection of a swapped name that has ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tombstone {
    /// One, whatever its stamp.
    Held,
    /// One at exactly this stamp.
    At(Stamp),
    /// None.
    NotHeld,
}

impl Tombstone {
    fn is_met_by(self, held: Option<Stamp>) -> bool {
        match self {
            Tombstone::Held => held.is_some(),
            Tombstone::At(stamp) => held == Some(stamp),
            Tombstone::NotHeld => held.is_none(),
        }
    }
}

/// PIN P: whether a view in `seat` is wanted not to hold the identity's tombstone. So where a binding is the register's first
/// event, which bound the identity out, and the view's member joined after it was in effect.
fn pin_p(want: &Expected, seat: Seat) -> bool {
    seat == Seat::JoinerAfterRelease
        && matches!(want.answer.register.first(), Some((_, FoldEvent::Bind { .. })))
}

/// The stamp pin: the stamp a view in `seat` is wanted to hold the tombstone of `stage` at, where a drop of the name ended it:
/// the register's next event after the stage's binding is that drop. The drop's, or, for a member that joined after a recreation
/// above the drop was in effect, the recreation's. `None` for a collection no such drop ended.
fn stamp_pin(want: &Expected, stage: &str, seat: Seat) -> Option<Stamp> {
    let mut from_its_binding = want.answer.register.iter().skip_while(
        |(_, event)| !matches!(event, FoldEvent::Bind { physical } if physical == stage),
    );
    from_its_binding.next()?;
    let Some((dropped, FoldEvent::Drop)) = from_its_binding.next() else { return None };
    let recreated = from_its_binding
        .find(|(_, event)| matches!(event, FoldEvent::Recreate { .. }))
        .map(|(stamp, _)| *stamp);
    match recreated {
        Some(recreated) if seat == Seat::JoinerAfterRelease => Some(recreated),
        _ => Some(*dropped),
    }
}

/// The collections of a swapped name that have ended, the identity as `None`, each with what a view in `seat` is wanted to hold
/// as its tombstone.
fn tombstones_wanted(want: &Expected, seat: Seat) -> Vec<(Option<&str>, Tombstone)> {
    let a = &want.answer;
    let mut wanted: Vec<(Option<&str>, Tombstone)> = a
        .dead
        .keys()
        .map(|physical| {
            let pinned = stamp_pin(want, physical, seat);
            (Some(physical.as_str()), pinned.map_or(Tombstone::Held, Tombstone::At))
        })
        .collect();
    if !a.register.is_empty() && !a.identity_stands {
        let identity = if pin_p(want, seat) { Tombstone::NotHeld } else { Tombstone::Held };
        wanted.push((None, identity));
    }
    wanted
}

// -- What a member holds ----------------------------------------------------------------------

/// One member's answer for one name, read from its store.
#[derive(Clone, Debug)]
struct View {
    /// What this is a view of. [`view`] reads a member; a joiner's is set by who made the joiner.
    seat: Seat,
    serves: Serves,
    /// The collection named like the name is held, whether or not the register shows it.
    identity_stands: bool,
    /// The stages and recreations held, each stage with the `Hlc` of its creation. A recreation's creation is not compared.
    alive: BTreeMap<String, Option<Hlc>>,
    register: Vec<(Stamp, RegisterEvent)>,
    superseded: BTreeMap<String, Option<Stamp>>,
    /// Of the facts the function says are history by the pure rules, those a register row in effect is held for. A parked row fails
    /// quiescence before anything is compared, so none is read here.
    invalid_held: BTreeSet<Stamp>,
    /// Live documents: the stamp and the body. A live row under the id with no collection held over it is one too.
    documents: BTreeMap<i64, (Stamp, String)>,
    /// Document tombstones stamped above the floor wanted of the member today ([`Expected::floor_today`]). One at or below the
    /// floor is residue and not compared: the sweep leaves one, at the stamp of the drop it swept under, for each row it removes,
    /// and a member that never held the row has none.
    tombstones: BTreeMap<i64, Stamp>,
    own_tombstone: Option<Stamp>,
    database_tombstone: Option<Stamp>,
    options: Option<Vec<u8>>,
    names: Option<Vec<u8>>,
    /// What the member holds of each physical the facts name.
    physicals: BTreeMap<String, Physical>,
}

/// What one member holds of one physical of a name, held or ended.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Physical {
    /// Live documents, as [`View::documents`].
    rows: BTreeMap<i64, (Stamp, String)>,
    /// Document tombstones, as [`View::tombstones`].
    tombstones: BTreeMap<i64, Stamp>,
    options: Option<Vec<u8>>,
    names: Option<Vec<u8>>,
    /// The collection tombstone held under its id.
    collection_tombstone: Option<Stamp>,
}

/// A part of what a member holds of its physicals, for those of which it holds that part.
fn part<T>(view: &View, of: impl Fn(&Physical) -> Option<T>) -> BTreeMap<String, T> {
    view.physicals.iter().filter_map(|(name, held)| Some((name.clone(), of(held)?))).collect()
}

/// A member's live documents, and its document tombstones.
type Documents = (BTreeMap<i64, (Stamp, String)>, BTreeMap<i64, Stamp>);

/// What `e` holds of the documents of the collection `id`, held as `meta` or not: the live rows; and of the documents `touched`,
/// the tombstones above `floor` and any live row that no collection is held over.
fn documents_of(
    e: &Engine,
    txn: &redb::ReadTransaction,
    id: CollectionId,
    meta: Option<&CollectionMeta>,
    touched: &BTreeSet<i64>,
    floor: Option<Stamp>,
) -> Res<Documents> {
    let mut live = BTreeMap::new();
    if let Some(meta) = meta {
        e.for_each_record_after(meta, None, WalkScope::Background, |id, stamp, doc| {
            let DocId::Int64(n) = id else { panic!("an id the model never writes") };
            live.insert(n, (stamp, doc.to_string()));
            Ok(true)
        })
        .map_err(|e| err("for_each_record_after", e))?;
    }
    let mut tombstones = BTreeMap::new();
    let records = txn.open_table(tables::DOCS).map_err(|e| err("open_table", e))?;
    for doc in touched {
        let key = crate::docs::doc_key(&DocId::Int64(*doc)).map_err(|e| err("doc_key", e))?;
        let Some(raw) = records.get((id.0, key.as_slice())).map_err(|e| err("get", e))? else {
            continue;
        };
        let record =
            codec::decode_doc_record(raw.value()).map_err(|e| err("decode_doc_record", e))?;
        if record.deleted {
            if floor.is_none_or(|f| record.stamp.hlc > f.hlc) {
                tombstones.insert(*doc, record.stamp);
            }
        } else if meta.is_none() {
            // no read of a collection shows this row, and the member holds it all the same
            let body = record.document().map_err(|e| err("document", e))?;
            live.insert(*doc, (record.stamp, format!("no collection held: {body:?}")));
        }
    }
    Ok((live, tombstones))
}

impl View {
    fn effective_floor(&self) -> Option<Stamp> {
        self.own_tombstone.max(self.database_tombstone)
    }
}

/// The row of the names or the base options table under the collection id, read through the register's module, which alone opens
/// its tables.
fn register_row(
    txn: &redb::ReadTransaction,
    which: Carried,
    id: CollectionId,
) -> Res<Option<Vec<u8>>> {
    stored_row_for_tests(txn, which, id).map_err(|e| err("stored_row_for_tests", e))
}

/// How many rows one of the register's tables holds, read the same way.
fn register_rows(txn: &redb::ReadTransaction, which: Carried) -> Res<u64> {
    stored_row_count_for_tests(txn, which).map_err(|e| err("stored_row_count_for_tests", e))
}

fn rows_held<K: redb::Key + 'static, V: redb::Value + 'static>(
    txn: &redb::ReadTransaction,
    table: redb::TableDefinition<K, V>,
) -> Res<u64> {
    match txn.open_table(table) {
        Ok(table) => table.len().map_err(|e| err("len", e)),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
        Err(e) => Err(err("open_table", e)),
    }
}

fn found<T>(read: crate::error::Result<T>, what: &str) -> Res<Option<T>> {
    match read {
        Ok(found) => Ok(Some(found)),
        Err(StorageError::Core(kimmy_core::Error::CollectionNotFound { .. })) => Ok(None),
        Err(e) => Err(err(what, e)),
    }
}

fn view(e: &Engine, name: &str, want: &Expected) -> Res<View> {
    let id = CollectionId::derive(DB, name);
    let identity = found(e.get_collection(DB, name), "get_collection")?;
    let resolved = found(e.resolve(DB, name), "resolve")?;
    let alive = e
        .all_collections()
        .map_err(|e| err("all_collections", e))?
        .into_iter()
        .filter(|c| c.db == DB && c.name.starts_with(&format!("{name}.")))
        .filter(|c| SHADOWS || !kimmy_core::vector_meta::is_shadow(&c.name))
        .map(|c| {
            let created = kimmy_core::is_stage_name(&c.name).then_some(c.created);
            (c.name, created)
        })
        .collect();
    let own_tombstone = e.collection_dropped_at(id).map_err(|e| err("collection_dropped_at", e))?;
    let database_tombstone =
        e.database_dropped_at(DB).map_err(|e| err("database_dropped_at", e))?;
    let txn = e.db().begin_read().map_err(|e| err("begin_read", e))?;
    let timeline =
        crate::register::timeline_read(&txn, DB, name).map_err(|e| err("timeline_read", e))?;
    let serves = match &resolved {
        None => Serves::Absent,
        Some(resolved) if resolved.physical == name => Serves::Identity,
        Some(resolved) => {
            let by = timeline.events().iter().find(|(s, _)| Some(*s) == resolved.visible);
            match by {
                Some((_, RegisterEvent::Create { .. })) => {
                    Serves::Recreation(resolved.physical.clone())
                }
                _ => Serves::Stage(resolved.physical.clone()),
            }
        }
    };
    let superseded = want
        .answer
        .superseded
        .keys()
        .map(|physical| (physical.clone(), timeline.superseded_at(physical)))
        .collect();
    let invalid_held = timeline
        .events()
        .iter()
        .map(|(stamp, _)| *stamp)
        .filter(|stamp| want.answer.invalid.contains(stamp))
        .collect();
    let (documents, tombstones) =
        documents_of(e, &txn, id, identity.as_ref(), &want.touched, want.floor_today())?;
    let mut physicals = BTreeMap::new();
    for (physical, touched) in &want.physicals {
        let of = CollectionId::derive(DB, physical);
        let meta = found(e.get_collection(DB, physical), "get_collection")?;
        let (rows, tombstones) =
            documents_of(e, &txn, of, meta.as_ref(), touched, want.floor_today())?;
        let options = register_row(&txn, Carried::Options, of)?;
        let names = register_row(&txn, Carried::Names, of)?;
        let collection_tombstone =
            e.collection_dropped_at(of).map_err(|e| err("collection_dropped_at", e))?;
        let held = Physical { rows, tombstones, options, names, collection_tombstone };
        physicals.insert(physical.clone(), held);
    }
    Ok(View {
        seat: Seat::Member,
        serves,
        identity_stands: identity.is_some(),
        alive,
        register: timeline.events().to_vec(),
        superseded,
        invalid_held,
        documents,
        tombstones,
        own_tombstone,
        database_tombstone,
        options: register_row(&txn, Carried::Options, id)?,
        names: register_row(&txn, Carried::Names, id)?,
        physicals,
    })
}

/// What no member may still hold at quiescence, and what it must not hold at all: a purge hold, a staged carry row, a pull stored
/// or registered, an event parked ahead of its position, a collection of no name, and a row of the options or names table whose
/// id is neither one of the names' nor one of the physicals' the facts name.
fn leftovers(e: &Engine, wants: &[(&str, &Expected)]) -> Res<Vec<String>> {
    let mut left = Vec::new();
    let txn = e.db().begin_read().map_err(|e| err("begin_read", e))?;
    for (what, rows) in [
        ("purge holds", rows_held(&txn, tables::PURGE_NOT_BEFORE)?),
        ("staged carry rows", rows_held(&txn, tables::SNAPSHOT_CARRY_STAGE)?),
        ("snapshot progress rows", rows_held(&txn, tables::SNAPSHOT_PROGRESS)?),
        ("parked events", register_rows(&txn, Carried::Info)?),
    ] {
        if rows != 0 {
            left.push(format!("{rows} {what}"));
        }
    }
    let registered = e.pulls_in_flight.lock().len();
    if registered != 0 {
        left.push(format!("{registered} pulls registered"));
    }
    let stored = e.snapshot_pull_peers().map_err(|e| err("snapshot_pull_peers", e))?;
    if !stored.is_empty() {
        left.push(format!("a pull stored with {stored:?}"));
    }
    let known: Vec<[u8; 8]> = wants
        .iter()
        .flat_map(|(name, want)| {
            std::iter::once(*name).chain(want.physicals.keys().map(String::as_str))
        })
        .map(|name| CollectionId::derive(DB, name).0.to_be_bytes())
        .collect();
    for (what, table) in [("options", Carried::Options), ("names", Carried::Names)] {
        for (key, _) in carried_rows(&txn, table).map_err(|e| err("carried_rows", e))? {
            if !known.iter().any(|id| key.starts_with(id)) {
                left.push(format!(
                    "the {what} table: a row under the id of no name and no physical: {key:02x?}"
                ));
            }
        }
    }
    for c in e.all_collections().map_err(|e| err("all_collections", e))? {
        let of_a_name = c.db == DB
            && NAMES.iter().any(|n| c.name == *n || c.name.starts_with(&format!("{n}.")));
        if !of_a_name {
            left.push(format!("a collection of no name: {}.{}", c.db, c.name));
        }
    }
    Ok(left)
}

// -- The two comparisons ---------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Column {
    // against the function
    Serves,
    Identity,
    Floor,
    Alive,
    Register,
    Superseded,
    Invalid,
    Documents,
    // against the first member
    LiveRows,
    Tombstones,
    OwnTombstone,
    DatabaseTombstone,
    EffectiveFloor,
    Options,
    Names,
    // of the physicals, against the first member
    PhysicalRows,
    PhysicalTombstones,
    PhysicalOptions,
    PhysicalNames,
    // of a swapped name's ended collections, against what is wanted of each view ([`tombstones_wanted`])
    Presence,
}

/// The columns compared for a name, `swapped` or not (the module's note on floors; the documents of a swapped name have no oracle
/// of their own and are compared between members; a plain name's own tombstone is [`Column::OwnTombstone`]'s).
fn columns(swapped: bool) -> Vec<Column> {
    use Column::*;
    let all = [
        Serves,
        Identity,
        Floor,
        Alive,
        Register,
        Superseded,
        Invalid,
        Documents,
        LiveRows,
        Tombstones,
        OwnTombstone,
        DatabaseTombstone,
        EffectiveFloor,
        Options,
        Names,
        PhysicalRows,
        PhysicalTombstones,
        PhysicalOptions,
        PhysicalNames,
        Presence,
    ];
    all.into_iter()
        .filter(|c| !(swapped && matches!(c, Floor | Documents | OwnTombstone | EffectiveFloor)))
        .filter(|c| swapped || *c != Presence)
        .collect()
}

/// The function's register as storage keeps one: three kinds of event, a database drop being a logical drop. A binding or a
/// recreation that a database drop made a drop at its own stamp needs a database drop over a stage; no schedule here has one, and
/// this mapping does not make it.
fn as_stored(register: &[(Stamp, FoldEvent)]) -> Vec<(Stamp, RegisterEvent)> {
    register
        .iter()
        .map(|(stamp, event)| {
            let event = match event {
                FoldEvent::Bind { physical } => RegisterEvent::Bind { physical: physical.clone() },
                FoldEvent::Recreate { physical } => {
                    RegisterEvent::Create { physical: physical.clone() }
                }
                FoldEvent::Drop | FoldEvent::DatabaseDrop => RegisterEvent::LogicalDrop,
            };
            (*stamp, event)
        })
        .collect()
}

#[derive(Debug)]
struct Difference {
    column: Column,
    member: usize,
    detail: String,
}

/// A row whose bytes are text, as an options row's are.
fn as_text(row: &Option<Vec<u8>>) -> String {
    row.as_deref()
        .map_or_else(|| "none".into(), |bytes| String::from_utf8_lossy(bytes).into_owned())
}

/// Both comparisons of one name over the members' views: each member against `want`, and each member after the first against the
/// first. [`Column::Presence`] is held to what is wanted of each view's [`Seat`].
fn compare(want: &Expected, views: &[View]) -> Vec<Difference> {
    let columns = columns(!want.answer.register.is_empty());
    let mut differences = Vec::new();
    let first = &views[0];
    for (member, v) in views.iter().enumerate() {
        let mut check = |column: Column, same: bool, detail: &dyn Fn() -> String| {
            if columns.contains(&column) && !same {
                differences.push(Difference { column, member, detail: detail() });
            }
        };
        let a = &want.answer;
        check(Column::Serves, v.serves == a.serves, &|| {
            format!("serves {:?}, the function {:?}", v.serves, a.serves)
        });
        check(Column::Identity, v.identity_stands == a.identity_stands, &|| {
            format!("identity {}, the function {}", v.identity_stands, a.identity_stands)
        });
        check(Column::Floor, v.effective_floor() == want.floor_today(), &|| {
            format!(
                "floor {:?}; wanted {:?} (the function {:?}, PIN B {:?})",
                v.effective_floor(),
                want.floor_today(),
                a.floor,
                want.pinned_at()
            )
        });
        let alive: BTreeMap<String, Option<Hlc>> = a
            .alive
            .iter()
            .map(|(physical, created)| {
                (physical.clone(), kimmy_core::is_stage_name(physical).then_some(*created))
            })
            .collect();
        check(Column::Alive, v.alive == alive, &|| {
            format!("holds {:?}, the function {alive:?}", v.alive)
        });
        let register = as_stored(&a.register);
        check(Column::Register, v.register == register, &|| {
            format!("register {:?}, the function {register:?}", v.register)
        });
        check(Column::Superseded, v.superseded == a.superseded, &|| {
            format!("superseded {:?}, the function {:?}", v.superseded, a.superseded)
        });
        check(Column::Invalid, v.invalid_held.is_empty(), &|| {
            format!("holds a row for an invalid fact: {:?}", v.invalid_held)
        });
        let stamps: BTreeMap<i64, Stamp> =
            v.documents.iter().map(|(doc, (stamp, _))| (*doc, *stamp)).collect();
        check(Column::Documents, stamps == want.documents, &|| {
            format!("documents {stamps:?}, the function {:?}", want.documents)
        });
        check(Column::LiveRows, v.documents == first.documents, &|| {
            format!("live rows {:?}, the first {:?}", v.documents, first.documents)
        });
        check(Column::Tombstones, v.tombstones == first.tombstones, &|| {
            format!("tombstones {:?}, the first {:?}", v.tombstones, first.tombstones)
        });
        check(Column::OwnTombstone, v.own_tombstone == first.own_tombstone, &|| {
            format!("own tombstone {:?}, the first {:?}", v.own_tombstone, first.own_tombstone)
        });
        check(Column::DatabaseTombstone, v.database_tombstone == first.database_tombstone, &|| {
            format!(
                "database tombstone {:?}, the first {:?}",
                v.database_tombstone, first.database_tombstone
            )
        });
        check(Column::EffectiveFloor, v.effective_floor() == first.effective_floor(), &|| {
            format!(
                "effective floor {:?}, the first {:?}",
                v.effective_floor(),
                first.effective_floor()
            )
        });
        check(Column::Options, v.options == first.options, &|| {
            format!("options {}, the first {}", as_text(&v.options), as_text(&first.options))
        });
        check(Column::Names, v.names == first.names, &|| {
            format!("names row {:?}, the first {:?}", v.names, first.names)
        });
        let rows = |v: &View| part(v, |p| (!p.rows.is_empty()).then(|| p.rows.clone()));
        check(Column::PhysicalRows, rows(v) == rows(first), &|| {
            format!("physicals' live rows {:?}, the first {:?}", rows(v), rows(first))
        });
        let dead = |v: &View| part(v, |p| (!p.tombstones.is_empty()).then(|| p.tombstones.clone()));
        check(Column::PhysicalTombstones, dead(v) == dead(first), &|| {
            format!("physicals' tombstones {:?}, the first {:?}", dead(v), dead(first))
        });
        let options = |v: &View| part(v, |p| p.options.clone());
        check(Column::PhysicalOptions, options(v) == options(first), &|| {
            format!("physicals' options rows {:?}, the first {:?}", options(v), options(first))
        });
        let names = |v: &View| part(v, |p| p.names.clone());
        check(Column::PhysicalNames, names(v) == names(first), &|| {
            format!("physicals' names rows {:?}, the first {:?}", names(v), names(first))
        });
        let held = |of: Option<&str>| match of {
            None => v.own_tombstone,
            Some(physical) => v.physicals.get(physical).and_then(|p| p.collection_tombstone),
        };
        let unmet: Vec<String> = tombstones_wanted(want, v.seat)
            .into_iter()
            .filter(|(of, wanted)| !wanted.is_met_by(held(*of)))
            .map(|(of, wanted)| {
                let name = of.unwrap_or("the identity");
                format!("{name} of a {:?} holds {:?}, wanted {wanted:?}", v.seat, held(of))
            })
            .collect();
        check(Column::Presence, unmet.is_empty(), &|| unmet.join("; "));
    }
    differences
}

// -- Deliveries ------------------------------------------------------------------------------

/// One entry to `to`, as a window that carries it alone, and the requirement that `to` took it.
fn deliver(to: &Engine, entry: &OplogEntry) -> Res<()> {
    let mut theirs = VersionVector::default();
    theirs.observe(entry.stamp);
    for _ in 0..4 {
        let mine = to.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_into(
            &theirs,
            Some(&mine),
            std::slice::from_ref(entry),
            PeerWindow {
                scanned_to: entry.stamp.hlc,
                exhausted: true,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .map_err(|e| err("apply_peer_window", e))?;
        to.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        if outcome.purge_pending_at.is_none() {
            if let Some(unknown) = outcome.unknown {
                return Err(format!("stopped at an entry of an unknown collection: {unknown:?}"));
            }
            let taken = to.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
            return if taken.get(entry.stamp.origin()) >= entry.stamp.hlc {
                Ok(())
            } else {
                Err(format!("the entry at {:?} was not taken", entry.stamp))
            };
        }
    }
    Err("an entry that waits for a purge that never finishes".into())
}

/// A pull of snapshot pages from `from` into `to`, to its end. Answers how many pages were applied.
fn pages(to: &Engine, from: &Engine, scope: Option<CollectionId>) -> Res<usize> {
    let mut progress =
        scope.map_or_else(SnapshotProgress::whole_database, SnapshotProgress::of_collection);
    let (mut applied_pages, mut waits) = (0, 0);
    while !progress.is_complete() {
        let page = from
            .snapshot_page(progress.after().cloned(), progress.scope())
            .map_err(|e| err("snapshot_page", e))?;
        let applied = to
            .apply_snapshot_page(from.node_id(), &mut progress, &page)
            .map_err(|e| err("apply_snapshot_page", e))?;
        if let Some(stop) = &applied.clock_refused {
            return Err(format!("a page refused by the clock bound: {stop:?}"));
        }
        if applied.purge_pending.is_none() {
            applied_pages += 1;
            continue;
        }
        waits += 1;
        if waits > 4 {
            return Err("a page that waits for a purge that never finishes".into());
        }
        to.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
    }
    to.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
    Ok(applied_pages)
}

/// Pull `from`'s entries into `to` until nothing of `from`'s is left, with the transport's fallbacks to pages.
fn entries(to: &Engine, from: &Engine) -> Res<()> {
    let mut stopped_at = None;
    for _ in 0..32 {
        let theirs = from.version_vector().map_err(|e| err("version_vector", e))?;
        let mine = to.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        let marked: Vec<MarkedRange> = to
            .held_marks_covered_by(&mine)
            .map_err(|e| err("held_marks_covered_by", e))?
            .into_iter()
            .filter_map(|(span, _)| {
                let through = span.through.min(theirs.get(span.origin));
                (through >= span.from).then_some(MarkedRange { through, ..span })
            })
            .collect();
        let start = mine.behind(&theirs).or_else(|| {
            (!marked.is_empty()).then(|| theirs.iter().map(|(_, h)| h).max().unwrap_or(Hlc::ZERO))
        });
        let Some(start) = start else { return Ok(()) };
        if from.serve_verdict(&mine).map_err(|e| err("serve_verdict", e))? != ServeVerdict::Serve {
            pages(to, from, None)?;
            return Ok(());
        }
        let window = from
            .entries_for_peer_marked(start, 1024, Some(&mine), &marked, WalkScope::Background)
            .map_err(|e| err("entries_for_peer", e))?;
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_into(
            &theirs,
            Some(&mine),
            &window.entries,
            PeerWindow {
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .map_err(|e| err("apply_peer_window", e))?;
        to.finish_purges_now().map_err(|e| err("finish_purges_now", e))?;
        if outcome.purge_pending_at.is_some() {
            continue;
        }
        if let Some(unknown) = outcome.unknown {
            if stopped_at == Some(unknown.stamp) {
                return Err(format!("stuck at an entry of an unknown collection: {unknown:?}"));
            }
            stopped_at = Some(unknown.stamp);
            pages(to, from, Some(unknown.id))?;
            continue;
        }
        if window.exhausted {
            return Ok(());
        }
    }
    Err("a pull that never ended".into())
}

/// A member that joins by snapshot: a fresh engine, in no step of the schedule, so it writes nothing.
struct Joiner {
    engine: Engine,
    /// Where it joined: after the release, unless who made it says it joined while an event was parked.
    seat: Seat,
    _dir: tempfile::TempDir,
}

impl Joiner {
    /// A whole-database pull from the first page to the grant. After it the joiner has taken everything `source` has.
    fn join(base: u64, source: &Engine) -> Res<Joiner> {
        let dir = tempfile::tempdir().map_err(|e| err("tempdir", e))?;
        let engine = Engine::open(&dir.path().join("joiner.redb")).map_err(|e| err("open", e))?;
        model_clock(&engine, base);
        let applied = pages(&engine, source, None)?;
        if applied == 0 {
            return Err("the join applied no page".into());
        }
        let joiner = Joiner { engine, seat: Seat::JoinerAfterRelease, _dir: dir };
        joiner.has_taken(source)?;
        Ok(joiner)
    }

    fn has_taken(&self, member: &Engine) -> Res<()> {
        let theirs = member.version_vector().map_err(|e| err("version_vector", e))?;
        let mine = self.engine.witnessed_vector().map_err(|e| err("witnessed_vector", e))?;
        match mine.behind(&theirs) {
            None => Ok(()),
            Some(from) => Err(format!("the joiner is behind a member from {from:?}")),
        }
    }

    /// Take what the members hold, by the route the cluster quiesced by.
    fn catch_up(&self, cluster: &Cluster, route: Route) -> Res<()> {
        for m in 0..3 {
            match route {
                Route::Pages => {
                    pages(&self.engine, cluster.e(m), None)?;
                }
                Route::Entries | Route::Repaired => entries(&self.engine, cluster.e(m))?,
            }
        }
        for m in 0..3 {
            self.has_taken(cluster.e(m))?;
        }
        Ok(())
    }
}

/// A quiesced run: for each name, what is expected of it and what each member holds (A, B, C, then the joiner).
struct Quiet {
    names: Vec<(&'static str, Expected, Vec<View>)>,
}

/// Quiesce `cluster` by `route`, bring `joiner` level, and read every member. A fact the ledger lacks and anything left over are
/// errors, not differences.
fn quiet(cluster: &mut Cluster, ledger: &Ledger, joiner: &Joiner, route: Route) -> Res<Quiet> {
    if !cluster.quiesce(route)? {
        return Err("still moving after the rounds of quiescence".into());
    }
    joiner.catch_up(cluster, route)?;
    let members = [cluster.e(0), cluster.e(1), cluster.e(2), &joiner.engine];
    let unknown = ledger.unknown_to_it(&members)?;
    if !unknown.is_empty() {
        return Err(format!("facts the ledger was not told of: {unknown:?}"));
    }
    let mut names = Vec::new();
    for name in NAMES {
        let want = ledger.expected(name);
        let mut views = Vec::new();
        for e in members {
            views.push(view(e, name, &want)?);
        }
        views[3].seat = joiner.seat;
        names.push((name, want, views));
    }
    let wants: Vec<(&str, &Expected)> = names.iter().map(|(name, want, _)| (*name, want)).collect();
    for (m, e) in members.iter().enumerate() {
        let left = leftovers(e, &wants)?;
        if !left.is_empty() {
            return Err(format!("member {m} at quiescence: {left:?}"));
        }
    }
    Ok(Quiet { names })
}

fn member_letter(m: usize) -> char {
    LETTERS.get(m).copied().unwrap_or('J')
}

// -- The documents row -------------------------------------------------------------------------

/// What the member that never heard of the drop writes after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    Write,
    Delete,
    Definition,
}

/// Who takes what, one entry at a time, before the join; the joiner joins from the member that took it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delivery {
    /// A, which holds the collection, takes the drop and then what was written after it.
    ATakesTheDropFirst,
    ATakesTheWriteFirst,
    /// B, the dropper, which holds only the tombstone.
    BTakesTheWrite,
    /// C, the writer.
    CTakesTheDrop,
    /// The joiner joins from B before B takes anything, so it holds only the tombstone and never held the collection; then it
    /// takes the write.
    AJoinerHoldingOnlyTheTombstoneTakesTheWrite,
}

const VARIANTS: [Variant; 3] = [Variant::Write, Variant::Delete, Variant::Definition];
const DELIVERIES: [Delivery; 5] = [
    Delivery::ATakesTheDropFirst,
    Delivery::ATakesTheWriteFirst,
    Delivery::BTakesTheWrite,
    Delivery::CTakesTheDrop,
    Delivery::AJoinerHoldingOnlyTheTombstoneTakesTheWrite,
];

/// A plain collection; a drop; and, stamped after the drop, a write, a delete or a definition by a member that had not heard of
/// it; no creation after. Answers the run, the drop and what was made after it.
fn documents_run(variant: Variant, delivery: Delivery, route: Route) -> Res<(Quiet, Stamp, Stamp)> {
    let base = base();
    let mut cluster = Cluster::new(base);
    let mut ledger = Ledger::default();
    let after = match variant {
        Variant::Write => Step::Write { m: C, c: 0, d: 2 },
        Variant::Delete => Step::Delete { m: C, c: 0, d: 1 },
        Variant::Definition => Step::Index { m: C, c: 0, ix: 2 },
    };
    for step in [
        op(Step::Create { m: A, c: 0 }, 10),
        op(Step::Write { m: A, c: 0, d: 1 }, 20),
        op(Step::Pull { to: B, from: A }, 0),
        op(Step::Pull { to: C, from: A }, 0),
        op(Step::Drop { m: B, c: 0 }, 30),
        op(after, 40),
    ] {
        ledger.step(&mut cluster, step)?;
        cluster.finish_purges()?;
    }
    let by = |m: u8, cluster: &Cluster| -> Res<OplogEntry> {
        let node = cluster.e(m as usize).node_id();
        ledger
            .entries
            .values()
            .map(|(entry, _)| entry)
            .rfind(|entry| entry.stamp.node == node)
            .cloned()
            .ok_or_else(|| format!("member {m} wrote no fact"))
    };
    let (dropped, later) = (by(B, &cluster)?, by(C, &cluster)?);
    let before = by(A, &cluster)?;
    assert_eq!(dropped.kind, OpKind::DropCollection, "premise: B's fact is its drop");
    let expected_kind = match variant {
        Variant::Write => matches!(later.kind, OpKind::Insert | OpKind::Replace),
        Variant::Delete => later.kind == OpKind::Delete,
        Variant::Definition => later.kind == OpKind::CreateIndex,
    };
    assert!(expected_kind, "premise: C's fact is the variant's: {:?}", later.kind);
    assert!(
        before.stamp.hlc < dropped.stamp.hlc && dropped.stamp.hlc < later.stamp.hlc,
        "premise: A's facts, then the drop, then C's"
    );
    for (m, fact) in [(B, &later), (C, &dropped)] {
        let seen = cluster.e(m as usize).witnessed_vector().map_err(|e| err("witnessed", e))?;
        assert!(seen.get(fact.stamp.origin()) < fact.stamp.hlc, "premise: concurrent");
    }
    let source = match delivery {
        Delivery::ATakesTheDropFirst => {
            deliver(cluster.e(0), &dropped)?;
            deliver(cluster.e(0), &later)?;
            0
        }
        Delivery::ATakesTheWriteFirst => {
            deliver(cluster.e(0), &later)?;
            deliver(cluster.e(0), &dropped)?;
            0
        }
        Delivery::BTakesTheWrite => {
            deliver(cluster.e(1), &later)?;
            1
        }
        Delivery::CTakesTheDrop => {
            deliver(cluster.e(2), &dropped)?;
            2
        }
        Delivery::AJoinerHoldingOnlyTheTombstoneTakesTheWrite => 1,
    };
    let joiner = Joiner::join(base, cluster.e(source))?;
    let holds = |e: &Engine| found(e.get_collection(DB, NAMES[0]), "get_collection");
    assert_eq!(
        holds(&joiner.engine)?.is_some(),
        holds(cluster.e(source))?.is_some(),
        "premise: the joiner holds the collection where its source does"
    );
    if delivery == Delivery::AJoinerHoldingOnlyTheTombstoneTakesTheWrite {
        assert!(holds(&joiner.engine)?.is_none(), "premise: the joiner holds no collection");
        assert_eq!(
            joiner
                .engine
                .collection_dropped_at(CollectionId::derive(DB, NAMES[0]))
                .map_err(|e| err("collection_dropped_at", e))?,
            Some(dropped.stamp),
            "premise: the joiner holds the tombstone"
        );
        deliver(&joiner.engine, &later)?;
    }
    let quiet = quiet(&mut cluster, &ledger, &joiner, route)?;
    if variant == Variant::Definition {
        for (m, e) in
            [cluster.e(0), cluster.e(1), cluster.e(2), &joiner.engine].into_iter().enumerate()
        {
            let held = holds(e)?.is_some_and(|c| c.indexes.iter().any(|i| i.name == "j_1"));
            assert!(held, "{delivery:?} {route:?}: member {} has not the index", member_letter(m));
        }
    }
    Ok((quiet, dropped.stamp, later.stamp))
}

/// EQ's first row: every member, by entries and by a join, a member holding only the tombstone among them, ends holding the
/// collection with what was made after the drop and nothing at or below it, and the same three floor items.
#[test]
fn the_documents_row_holds_by_entries_and_by_a_join() {
    let mut failures = Vec::new();
    for variant in VARIANTS {
        for delivery in DELIVERIES {
            for route in [Route::Entries, Route::Pages] {
                let case = format!("{variant:?}, {delivery:?}, {route:?}");
                let (quiet, dropped, later) = match documents_run(variant, delivery, route) {
                    Ok(run) => run,
                    Err(e) => {
                        failures.push(format!("{case}: {e}"));
                        continue;
                    }
                };
                let (_, want, views) = &quiet.names[0];
                // the oracle's answer for the row, written out
                assert_eq!(want.answer.serves, Serves::Identity, "{case}: the function");
                assert!(want.answer.identity_stands, "{case}: the function");
                assert_eq!(want.answer.floor, Some(dropped), "{case}: the function");
                assert!(want.answer.register.is_empty(), "{case}: the function");
                let documents: BTreeMap<i64, Stamp> = match variant {
                    Variant::Write => BTreeMap::from([(2, later)]),
                    Variant::Delete | Variant::Definition => BTreeMap::new(),
                };
                assert_eq!(want.documents, documents, "{case}: the oracle's documents");
                if variant == Variant::Delete {
                    assert_eq!(
                        views[2].tombstones.get(&1),
                        Some(&later),
                        "{case}: premise: the delete's author holds its tombstone"
                    );
                }
                for d in compare(want, views) {
                    failures.push(format!(
                        "{case}: {} {:?}: {}",
                        member_letter(d.member),
                        d.column,
                        d.detail
                    ));
                }
                // the three floor items, written out: the collection's own at the drop, no database's, the effective at the drop
                for (m, v) in views.iter().enumerate() {
                    let items = (v.own_tombstone, v.database_tombstone, v.effective_floor());
                    if items != (Some(dropped), None, Some(dropped)) {
                        failures.push(format!(
                            "{case}: {}: floor items (own, database, effective) {items:?}",
                            member_letter(m)
                        ));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{} failure(s):\n{}", failures.len(), failures.join("\n"));
}

// -- The comparer ------------------------------------------------------------------------------

/// Each column, changed on ONE member, is reported on that member, in that column and in no other it does not carry with it.
#[test]
fn each_column_changed_on_one_member_is_reported() {
    let (quiet, dropped, later) =
        documents_run(Variant::Write, Delivery::ATakesTheDropFirst, Route::Entries).unwrap();
    let (_, want, views) = &quiet.names[0];
    assert!(compare(want, views).is_empty(), "premise: the members agree");
    let held = &views[1];
    assert!(
        held.documents.contains_key(&2)
            && held.own_tombstone == Some(dropped)
            && held.database_tombstone.is_none()
            && held.options.is_some(),
        "premise: the reads found the document, the floor and the options row: {held:?}"
    );
    let above = Stamp { hlc: later.hlc.successor(), ..later };
    let below = Stamp { hlc: Hlc::new(dropped.hlc.wall_ms - 1, 0), ..dropped };
    // every member holds one physical with something in each of its four parts, so that what a change of it must be reported by is
    // its VALUE: which physicals have a part stays the same
    const PHYSICAL: &str = "orders.__stg.x";
    let of_the_physical = Physical {
        rows: BTreeMap::from([(9, (above, "one".to_string()))]),
        tombstones: BTreeMap::from([(8, above)]),
        options: Some(vec![1]),
        names: Some(vec![1]),
        collection_tombstone: Some(above),
    };
    let mut views = views.clone();
    for v in &mut views {
        v.physicals.insert(PHYSICAL.into(), of_the_physical.clone());
    }
    let found = compare(want, &views);
    assert!(found.is_empty(), "premise: the members agree with the physical held: {found:?}");
    type Change<'a> = Box<dyn Fn(&mut View) + 'a>;
    // an array with its type written out, so that each box and each list is coerced where it stands
    let changes: [(&str, Change<'_>, &[Column]); 18] = [
        ("serves", Box::new(|v: &mut View| v.serves = Serves::Absent), &[Column::Serves]),
        ("identity", Box::new(|v: &mut View| v.identity_stands = false), &[Column::Identity]),
        (
            "a held stage",
            Box::new(|v: &mut View| {
                v.alive.insert("orders.__stg.x".into(), None);
            }),
            &[Column::Alive],
        ),
        (
            "a register event",
            Box::new(move |v: &mut View| v.register.push((above, RegisterEvent::LogicalDrop))),
            &[Column::Register],
        ),
        (
            "superseded",
            Box::new(move |v: &mut View| {
                v.superseded.insert("orders".into(), Some(above));
            }),
            &[Column::Superseded],
        ),
        (
            "an invalid fact held",
            Box::new(move |v: &mut View| {
                v.invalid_held.insert(above);
            }),
            &[Column::Invalid],
        ),
        (
            "a document's stamp",
            Box::new(move |v: &mut View| v.documents.get_mut(&2).unwrap().0 = above),
            &[Column::Documents, Column::LiveRows],
        ),
        (
            "a document's body",
            Box::new(|v: &mut View| v.documents.get_mut(&2).unwrap().1.push(' ')),
            &[Column::LiveRows],
        ),
        (
            "a document tombstone",
            Box::new(move |v: &mut View| {
                v.tombstones.insert(9, above);
            }),
            &[Column::Tombstones],
        ),
        (
            "the own tombstone",
            Box::new(move |v: &mut View| v.own_tombstone = Some(above)),
            &[Column::Floor, Column::OwnTombstone, Column::EffectiveFloor],
        ),
        (
            "the database tombstone alone",
            Box::new(move |v: &mut View| v.database_tombstone = Some(below)),
            &[Column::DatabaseTombstone],
        ),
        ("the options row", Box::new(|v: &mut View| v.options = Some(vec![0])), &[Column::Options]),
        ("a names row", Box::new(|v: &mut View| v.names = Some(vec![0])), &[Column::Names]),
        (
            "a physical's row",
            Box::new(move |v: &mut View| {
                let held = v.physicals.get_mut(PHYSICAL).unwrap();
                held.rows.insert(9, (above, "another".to_string()));
            }),
            &[Column::PhysicalRows],
        ),
        (
            "a physical's tombstone",
            Box::new(move |v: &mut View| {
                v.physicals.get_mut(PHYSICAL).unwrap().tombstones.insert(8, below);
            }),
            &[Column::PhysicalTombstones],
        ),
        (
            "a physical's options row",
            Box::new(|v: &mut View| v.physicals.get_mut(PHYSICAL).unwrap().options = Some(vec![0])),
            &[Column::PhysicalOptions],
        ),
        (
            "a physical's names row",
            Box::new(|v: &mut View| v.physicals.get_mut(PHYSICAL).unwrap().names = Some(vec![0])),
            &[Column::PhysicalNames],
        ),
        // compared only where a swapped name ended the collection, so of a plain name nothing
        (
            "a physical's collection tombstone",
            Box::new(|v: &mut View| {
                v.physicals.get_mut(PHYSICAL).unwrap().collection_tombstone = None;
            }),
            &[],
        ),
    ];
    let mut wrong: Vec<String> = Vec::new();
    let mut covered = BTreeSet::new();
    for (what, change, expected) in &changes {
        let mut views = views.clone();
        change(&mut views[1]);
        let found = compare(want, &views);
        let reported: BTreeSet<Column> = found.iter().map(|d| d.column).collect();
        let expected: BTreeSet<Column> = expected.iter().copied().collect();
        if reported != expected || found.iter().any(|d| d.member != 1) {
            wrong.push(format!("{what}: expected {expected:?} on member 1, reported {found:?}"));
        }
        covered.extend(expected);
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(
        covered,
        columns(false).into_iter().collect::<BTreeSet<_>>(),
        "every column is changed once"
    );
    // a held stage's creation stamp: with the stage wanted and held by every member, another stamp on one member is reported
    let stage = "orders.__stg.x".to_string();
    let mut with_a_stage = want.clone();
    with_a_stage.answer.alive.insert(stage.clone(), dropped.hlc);
    let mut holding = views.clone();
    for v in &mut holding {
        v.alive.insert(stage.clone(), Some(dropped.hlc));
    }
    let found = compare(&with_a_stage, &holding);
    assert!(found.is_empty(), "premise: every member holds the stage as wanted: {found:?}");
    holding[1].alive.insert(stage, Some(later.hlc));
    let found = compare(&with_a_stage, &holding);
    assert!(
        matches!(found.as_slice(), [d] if d.column == Column::Alive && d.member == 1),
        "a stage's creation stamp: {found:?}"
    );
    // a swapped name: of its floor neither the identity's own tombstone nor the effective floor is compared and the database's is,
    // and its physicals, which only a swapped name has, are compared in all four; the tombstone of one it ended is wanted held
    assert!(columns(true).contains(&Column::Presence), "a swapped name is held to its tombstones");
    assert!(!columns(false).contains(&Column::Presence), "and a plain name is not");
    let mut swapped = want.clone();
    swapped.answer.register.push((above, FoldEvent::Drop));
    swapped.answer.dead.insert(PHYSICAL.into(), above);
    let found: Vec<Difference> =
        compare(&swapped, &views).into_iter().filter(|d| d.column != Column::Register).collect();
    assert!(found.is_empty(), "premise: the members agree on the swapped name: {found:?}");
    let mut of_physicals = BTreeSet::new();
    for (what, change, of_a_plain_name) in &changes {
        let expected: BTreeSet<Column> = match *what {
            "the own tombstone" => BTreeSet::new(),
            "the database tombstone alone" => BTreeSet::from([Column::DatabaseTombstone]),
            "a physical's collection tombstone" => BTreeSet::from([Column::Presence]),
            "a physical's row"
            | "a physical's tombstone"
            | "a physical's options row"
            | "a physical's names row" => {
                of_physicals.extend(of_a_plain_name.iter().copied());
                of_a_plain_name.iter().copied().collect()
            }
            _ => continue,
        };
        let mut views = views.clone();
        change(&mut views[1]);
        let reported: BTreeSet<Column> = compare(&swapped, &views)
            .iter()
            .map(|d| d.column)
            .filter(|c| *c != Column::Register)
            .collect();
        assert_eq!(reported, expected, "a swapped name, {what}");
    }
    assert_eq!(of_physicals.len(), 4, "premise: each physical column is changed on a swapped name");
    // PIN B, both ways: with a burial above the function's floor the burial is the floor wanted, so a member that holds it is not
    // reported, and one that holds the function's floor is
    let mut buried = want.clone();
    buried.burial_above_the_floor = Some(above);
    let mut views = views.clone();
    views[1].own_tombstone = Some(above);
    let reported: BTreeSet<usize> = compare(&buried, &views)
        .iter()
        .filter(|d| d.column == Column::Floor)
        .map(|d| d.member)
        .collect();
    assert_eq!(reported, BTreeSet::from([0, 2, 3]), "PIN B");
    // PIN P and the stamp pin, both ways: a stage a binding put in the identity's place, then a drop and a recreation. A, B, C,
    // a joiner after the release and one from a member that held the binding parked, each holding what is wanted of it, are not
    // reported; and each that holds anything else is reported, alone
    let [bound_at, dropped_at, recreated_at, other] = [0, 1, 2, 3].map(|n| {
        let hlc = Hlc::new(above.hlc.wall_ms + 1 + n, 0);
        Stamp { hlc, ..above }
    });
    let mut bound = want.clone();
    bound.answer.identity_stands = false;
    bound.answer.register = vec![
        (bound_at, FoldEvent::Bind { physical: PHYSICAL.into() }),
        (dropped_at, FoldEvent::Drop),
        (recreated_at, FoldEvent::Recreate { physical: "orders.recreated".into() }),
    ];
    bound.answer.dead = BTreeMap::from([(PHYSICAL.to_string(), dropped_at)]);
    let seats = [
        Seat::Member,
        Seat::Member,
        Seat::Member,
        Seat::JoinerAfterRelease,
        Seat::JoinerWhileParked,
    ];
    let pinned: Vec<View> = seats
        .into_iter()
        .map(|seat| {
            let mut v = views[0].clone();
            v.seat = seat;
            let later = seat == Seat::JoinerAfterRelease;
            v.own_tombstone = (!later).then_some(bound_at);
            v.physicals.get_mut(PHYSICAL).unwrap().collection_tombstone =
                Some(if later { recreated_at } else { dropped_at });
            v
        })
        .collect();
    let reported_under = |want: &Expected, views: &[View]| -> BTreeSet<usize> {
        compare(want, views)
            .iter()
            .filter(|d| d.column == Column::Presence)
            .map(|d| d.member)
            .collect()
    };
    let reported = |views: &[View]| reported_under(&bound, views);
    assert_eq!(reported(&pinned), BTreeSet::new(), "the pinned shape");
    let identity = |held: Option<Stamp>| -> Change<'static> {
        Box::new(move |v: &mut View| v.own_tombstone = held)
    };
    let stage = |held: Option<Stamp>| -> Change<'static> {
        Box::new(move |v: &mut View| {
            v.physicals.get_mut(PHYSICAL).unwrap().collection_tombstone = held;
        })
    };
    let cases: [(&str, usize, Change<'_>); 12] = [
        // PIN P
        ("A without the identity's", 0, identity(None)),
        ("C without the identity's", 2, identity(None)),
        ("a joiner without a stage's", 3, stage(None)),
        ("a joiner after the release with the identity's", 3, identity(Some(bound_at))),
        ("a joiner while parked without the identity's", 4, identity(None)),
        // the stamp pin
        ("a joiner after the release at the drop's", 3, stage(Some(dropped_at))),
        ("a member at the recreation's", 0, stage(Some(recreated_at))),
        ("a joiner while parked at the recreation's", 4, stage(Some(recreated_at))),
        ("a third stamp on a member", 1, stage(Some(other))),
        ("a third stamp on a joiner", 3, stage(Some(other))),
        ("a member with no stage tombstone", 1, stage(None)),
        ("a joiner while parked with no stage tombstone", 4, stage(None)),
    ];
    let mut wrong = Vec::new();
    for (what, member, change) in &cases {
        let mut views = pinned.clone();
        change(&mut views[*member]);
        let found = reported(&views);
        if found != BTreeSet::from([*member]) {
            wrong.push(format!("{what}: expected view {member} alone, reported {found:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // where a pin's condition does not hold, it asks for nothing. Every view's stage tombstone is put at a stamp of no event,
    // which the stamp pin sees on all five where it applies
    let mut at_no_event = pinned.clone();
    for v in &mut at_no_event {
        stage(Some(other))(v);
    }
    assert_eq!(reported(&at_no_event), BTreeSet::from([0, 1, 2, 3, 4]), "premise: the pin applies");
    let binding = (bound_at, FoldEvent::Bind { physical: PHYSICAL.into() });
    let unmet_conditions = [
        // no binding bound the name out, so the joiner after the release is reported, alone, for the identity it lacks; and
        // no binding bound the stage, so no stamp is asked of it
        ("a drop alone", vec![(dropped_at, FoldEvent::Drop)], BTreeSet::<usize>::from([3])),
        // the drop is not the next event after the stage's binding
        (
            "another binding next, then the drop",
            vec![
                binding.clone(),
                (dropped_at, FoldEvent::Bind { physical: "orders.__stg.y".into() }),
                (recreated_at, FoldEvent::Drop),
            ],
            BTreeSet::new(),
        ),
        // the next event is a drop of the database, not of the name
        (
            "a drop of the database next",
            vec![
                binding,
                (dropped_at, FoldEvent::DatabaseDrop),
                (recreated_at, FoldEvent::Recreate { physical: "orders.recreated".into() }),
            ],
            BTreeSet::new(),
        ),
    ];
    for (what, register, wanted) in unmet_conditions {
        let mut want = bound.clone();
        want.answer.register = register;
        assert_eq!(reported_under(&want, &at_no_event), wanted, "{what}");
    }
}

// -- The generated run: the measured floor ------------------------------------------------------

/// The schedules of the plain collection: the convergence model's, without its garbage collections.
fn plain_schedule(seed: u64) -> Vec<Op> {
    generate(seed, 8 + (seed % 5) as usize, Track::Full)
        .into_iter()
        .filter(|op| !matches!(op.step, Step::Gc { .. }))
        .collect()
}

/// A schedule by `route`, with a member that joins from member `from` where the schedule ends.
fn plain_run(ops: &[Op], route: Route, from: usize) -> Res<Quiet> {
    let base = base();
    let mut cluster = Cluster::new(base);
    let mut ledger = Ledger::default();
    for (i, op) in ops.iter().enumerate() {
        cluster.step = i;
        ledger.step(&mut cluster, *op).map_err(|e| format!("step {i} ({op}): {e}"))?;
        if !cluster.defer_purges {
            cluster.finish_purges()?;
        } else if let Step::Pull { to, from }
        | Step::PullShort { to, from }
        | Step::Scoped { to, from, .. }
        | Step::Whole { to, from } = op.step
        {
            // The model leaves a pull whose page waits for the purger where it is and never resumes it. The transport forgets a
            // pull it abandons, and so does this.
            let peer = cluster.e(from as usize).node_id();
            cluster
                .e(to as usize)
                .forget_snapshot_pulls_of(peer)
                .map_err(|e| err("forget_snapshot_pulls_of", e))?;
        }
    }
    let joiner = Joiner::join(base, cluster.e(from))?;
    quiet(&mut cluster, &ledger, &joiner, route)
}

/// The schedules of seeds 235, 259 and 953, written out so that a change to the generator does not lose them; the seeds are past
/// the library's own. A longer run found members of each holding different options rows for `orders`: a creation a collection's
/// drop refused was not served onward.
const FOUND: [(u64, &str); 3] = [
    (
        235,
        "purger off; C.create(orders)@110; C.drop(orders)@120; A.dropdb@140; A<-C scoped(orders); A<-B whole; C<-A entries; \
         B.write(items/3)@150; A.drop(orders)@190",
    ),
    (
        259,
        "C.create(orders)@120; C.create(orders)@130; A.create(orders)@110; A.write(orders/2)@120; B<-A whole; \
         C.index(orders,k_1:asc)@170; B.drop(orders)@170; B<-A entries; A.index(orders,j_1:asc)@180; C<-A entries; \
         C.dropindex(orders,k_1)@230",
    ),
    (
        953,
        "B.create(orders)@110; B<-C entries; A.drop(orders)@140; B.drop(orders)@140; C.create(orders)@130; \
         C.create(orders)@140; B.index(orders,k_1:asc)@170; B.create(items)@180; C.drop(orders)@170; B.dropdb@200; \
         B.write(orders/1)@210",
    ),
];

/// Those schedules agree in every column, by entries and by pages, each with the joiner its seed's run had.
#[test]
fn the_schedules_of_seeds_235_259_and_953_agree_in_every_column() {
    let mut failures = Vec::new();
    for (seed, schedule) in FOUND {
        let ops = parse(schedule);
        for route in [Route::Entries, Route::Pages] {
            let run = match plain_run(&ops, route, (seed % 3) as usize) {
                Ok(run) => run,
                Err(e) => {
                    failures.push(format!("seed {seed} {route:?}: {e}"));
                    continue;
                }
            };
            let (_, _, of_orders) = &run.names[0];
            assert!(
                of_orders.iter().any(|v| v.options.is_some()),
                "seed {seed} {route:?}: premise: there is an options row to compare"
            );
            for (name, want, views) in &run.names {
                for d in compare(want, views) {
                    failures.push(format!(
                        "seed {seed} {route:?} {name}: {} {:?}: {}",
                        member_letter(d.member),
                        d.column,
                        d.detail
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{} failure(s):\n{}", failures.len(), failures.join("\n"));
}

/// The schedule of seed 13, whose joiner is B, as the generated run printed it. A database drop buries A's `orders` by an entry of
/// its own, and C's later drop of `orders` is the floor the function names. PIN B holds a name to such a burial only where the
/// burial sorts above that floor: here it sorts below, and every member holds the floor. A pin that took this burial too was caught
/// by the generated run, and by this seed alone of the library's, so the schedule is a row of its own.
const BELOW_THE_FLOOR: &str = "A.create(orders)@110; C.create(orders)@120; B<-C entries; B<-C whole; A<-C scoped(orders); \
                               C.create(items)@160; A.dropdb@170; C<-B entries(cut at 2); C.write(orders/1)@190; \
                               A.drop(orders)@200; C.drop(orders)@210";

#[test]
fn a_database_drops_burial_below_the_floor_is_not_pinned() {
    let ops = parse(BELOW_THE_FLOOR);
    for route in [Route::Entries, Route::Pages] {
        let run = plain_run(&ops, route, B as usize).unwrap_or_else(|e| panic!("{route:?}: {e}"));
        for (name, want, views) in &run.names {
            let differences = compare(want, views);
            assert!(differences.is_empty(), "{route:?} {name}: {differences:?}");
        }
    }
    // the premise, on members of its own: what the ledger holds of `orders`
    let base = base();
    let mut cluster = Cluster::new(base);
    let mut ledger = Ledger::default();
    for (i, op) in ops.iter().enumerate() {
        cluster.step = i;
        ledger.step(&mut cluster, *op).unwrap();
        cluster.finish_purges().unwrap();
    }
    let id = CollectionId::derive(DB, "orders");
    let want = ledger.expected("orders");
    let floor = want.answer.floor.expect("premise: orders has a floor");
    let (entry, of) = &ledger.entries[&floor];
    assert!(
        entry.kind == OpKind::DropCollection
            && entry.collection == id
            && of.is_none()
            && floor.node == cluster.e(C as usize).node_id()
            && floor.hlc.wall_ms == base + 210,
        "premise: the floor the function names is C's drop of orders at 210: {entry:?}"
    );
    assert!(
        ledger.entries.iter().any(|(stamp, (entry, of))| {
            entry.collection == id
                && of.is_some()
                && stamp.hlc.wall_ms == base + 170
                && *stamp < floor
        }),
        "premise: a database drop buried orders at 170, below that floor"
    );
}

/// With the purger off, a member that dropped a collection still holds its rows, with no collection over them. No schedule's
/// quiescence leaves such a row, so the read that reports one is exercised here.
const UNPURGED: &str =
    "purger off; B.create(orders)@110; B.write(orders/1)@120; B.drop(orders)@130";

#[test]
fn a_live_row_with_no_collection_over_it_is_read() {
    let mut cluster = Cluster::new(base());
    let mut ledger = Ledger::default();
    for (i, op) in parse(UNPURGED).iter().enumerate() {
        cluster.step = i;
        ledger.step(&mut cluster, *op).unwrap();
    }
    let want = ledger.expected("orders");
    let before = view(cluster.e(B as usize), "orders", &want).unwrap();
    assert!(!before.identity_stands, "premise: B holds no collection");
    assert!(
        before.documents.get(&1).is_some_and(|(_, body)| body.starts_with("no collection held")),
        "the live row with no collection over it is read: {:?}",
        before.documents
    );
    cluster.finish_purges().unwrap();
    let after = view(cluster.e(B as usize), "orders", &want).unwrap();
    assert!(after.documents.is_empty(), "premise: the purge removes it: {:?}", after.documents);
}

/// A schedule whose last step leaves a pull of ONE collection waiting: B drops a life that holds a row while its purger is
/// behind, then pulls a page of A's newer life of the name, which waits for the purger and which the model never resumes.
/// Quiescence pulls entries, or whole databases by pages, and never one collection's page, so nothing but the run's own
/// forgetting ([`plain_run`]) ends that pull, by either route. The library's seeds do not show that the run must: with the
/// forgetting taken out, their run still passed.
const ABANDONED: &str = "purger off; B.create(orders)@110; B.write(orders/1)@120; B.drop(orders)@130; \
                         A.create(orders)@140; B<-A scoped(orders)";

#[test]
fn a_pull_the_run_abandons_at_a_waiting_page_is_forgotten() {
    let ops = parse(ABANDONED);
    // the premise, on members of its own, before anything forgets: the model leaves the pull registered
    let mut cluster = Cluster::new(base());
    let mut ledger = Ledger::default();
    for (i, op) in ops.iter().enumerate() {
        cluster.step = i;
        ledger.step(&mut cluster, *op).unwrap();
    }
    let waiting = (cluster.e(A as usize).node_id(), Some(CollectionId::derive(DB, "orders")));
    assert_eq!(
        *cluster.e(B as usize).pulls_in_flight.lock(),
        std::collections::HashSet::from([waiting]),
        "premise: the page waits for the purger, and its pull stays registered"
    );
    for route in [Route::Entries, Route::Pages] {
        let run = plain_run(&ops, route, A as usize).unwrap_or_else(|e| panic!("{route:?}: {e}"));
        for (name, want, views) in &run.names {
            let differences = compare(want, views);
            assert!(differences.is_empty(), "{route:?} {name}: {differences:?}");
        }
    }
}

/// The seeds of the library's run. `EQ_SEEDS` and `EQ_SEED_START` draw a longer or another run.
const DEFAULT_SEEDS: u64 = 100;

/// What the library's own seeds drew when the floor was measured: for each set of kinds, the names (a route at a time) that had
/// it, how many of them PIN B held, and the columns that held nothing in any of those names besides [`NOTHING_SEEN`]. A set's
/// agreement in a column says something only where the column held something on at least one side ([`shows`]): one seen in another
/// set does not count for this one. The clocks are scripted, so these repeat. A change to any of them is a change in what the run
/// asserts, and is restated on purpose: a draw that shrank would pass every other assert.
///
/// **These counts are tied to the generator** ([`plain_schedule`]: the convergence model's `generate`, less its garbage
/// collections), **to the seeds `0..DEFAULT_SEEDS` and to the two routes. A deliberate change to any of them measures these
/// again, in the same commit.**
const MEASURED: &[(&[Kind], usize, usize, &[Column])] = &[
    (
        &[],
        136,
        0,
        &[
            Column::Serves,
            Column::Identity,
            Column::Floor,
            Column::Documents,
            Column::LiveRows,
            Column::OwnTombstone,
            Column::DatabaseTombstone,
            Column::EffectiveFloor,
            Column::Options,
        ],
    ),
    (
        &[Kind::Create],
        78,
        0,
        &[
            Column::Floor,
            Column::Documents,
            Column::LiveRows,
            Column::OwnTombstone,
            Column::DatabaseTombstone,
            Column::EffectiveFloor,
        ],
    ),
    (
        &[Kind::Create, Kind::Touch],
        82,
        0,
        &[Column::Floor, Column::OwnTombstone, Column::DatabaseTombstone, Column::EffectiveFloor],
    ),
    (
        &[Kind::Create, Kind::Drop],
        32,
        0,
        &[Column::Documents, Column::LiveRows, Column::DatabaseTombstone],
    ),
    (&[Kind::Create, Kind::Touch, Kind::Drop], 20, 0, &[Column::DatabaseTombstone]),
    (
        &[Kind::DatabaseDrop],
        16,
        0,
        &[
            Column::Serves,
            Column::Identity,
            Column::Documents,
            Column::LiveRows,
            Column::OwnTombstone,
            Column::Options,
        ],
    ),
    (&[Kind::Create, Kind::DatabaseDrop], 14, 8, &[Column::Documents, Column::LiveRows]),
    (&[Kind::Create, Kind::Touch, Kind::DatabaseDrop], 14, 10, &[]),
    (
        &[Kind::Create, Kind::Drop, Kind::DatabaseDrop],
        4,
        4,
        &[Column::Serves, Column::Identity, Column::Documents, Column::LiveRows],
    ),
    (
        &[Kind::Create, Kind::Touch, Kind::Drop, Kind::DatabaseDrop],
        4,
        2,
        &[Column::Documents, Column::LiveRows],
    ),
];

/// The columns that held nothing in any set of kinds of the library's seeds: empty on both sides, so that their agreeing shows
/// nothing anywhere. No step drawn here is of a swap, so the seeds put nothing in the stages held, the register, what is
/// superseded, the invalid facts, the names rows or any column of a physical. The document tombstones are another kind of empty:
/// the seeds draw deletes, and none leaves a tombstone above the floor (longer runs see some; the documents row's delete reads
/// one). A column that holds nothing is still compared, and any difference in it still fails.
const NOTHING_SEEN: &[Column] = &[
    Column::Alive,
    Column::Register,
    Column::Superseded,
    Column::Invalid,
    Column::Tombstones,
    Column::Names,
    Column::PhysicalRows,
    Column::PhysicalTombstones,
    Column::PhysicalOptions,
    Column::PhysicalNames,
];

/// Whether `column` holds something on at least one side for this member of this name: against the function, the member's read or
/// the function's value; against the first member, this member's or the first's; for the tombstones wanted, this member's or what
/// is wanted of it. Where it holds nothing, its agreeing says nothing.
fn shows(column: Column, want: &Expected, v: &View, first: &View) -> bool {
    let a = &want.answer;
    match column {
        Column::Serves => v.serves != Serves::Absent || a.serves != Serves::Absent,
        Column::Identity => v.identity_stands || a.identity_stands,
        Column::Floor => v.effective_floor().is_some() || want.floor_today().is_some(),
        Column::Alive => !v.alive.is_empty() || !a.alive.is_empty(),
        Column::Register => !v.register.is_empty() || !a.register.is_empty(),
        Column::Superseded => {
            v.superseded.values().chain(a.superseded.values()).any(Option::is_some)
        }
        Column::Invalid => !v.invalid_held.is_empty() || !a.invalid.is_empty(),
        Column::Documents => !v.documents.is_empty() || !want.documents.is_empty(),
        Column::LiveRows => !v.documents.is_empty() || !first.documents.is_empty(),
        Column::Tombstones => !v.tombstones.is_empty() || !first.tombstones.is_empty(),
        Column::OwnTombstone => v.own_tombstone.is_some() || first.own_tombstone.is_some(),
        Column::DatabaseTombstone => {
            v.database_tombstone.is_some() || first.database_tombstone.is_some()
        }
        Column::EffectiveFloor => {
            v.effective_floor().is_some() || first.effective_floor().is_some()
        }
        Column::Options => v.options.is_some() || first.options.is_some(),
        Column::Names => v.names.is_some() || first.names.is_some(),
        Column::PhysicalRows => [v, first].into_iter().any(|v| of_any(v, |p| !p.rows.is_empty())),
        Column::PhysicalTombstones => {
            [v, first].into_iter().any(|v| of_any(v, |p| !p.tombstones.is_empty()))
        }
        Column::PhysicalOptions => {
            [v, first].into_iter().any(|v| of_any(v, |p| p.options.is_some()))
        }
        Column::PhysicalNames => [v, first].into_iter().any(|v| of_any(v, |p| p.names.is_some())),
        Column::Presence => {
            of_any(v, |p| p.collection_tombstone.is_some())
                || !tombstones_wanted(want, v.seat).is_empty()
        }
    }
}

/// Whether `holds` is true of what the member holds of some physical.
fn of_any(view: &View, holds: impl Fn(&Physical) -> bool) -> bool {
    view.physicals.values().any(holds)
}

/// Of the columns `seen` to hold something in some name of a set of kinds, those that never did.
fn held_nothing(seen: Option<&BTreeSet<Column>>) -> BTreeSet<Column> {
    columns(false).into_iter().filter(|c| !seen.is_some_and(|seen| seen.contains(c))).collect()
}

/// [`shows`] reads both sides of each comparison. Where the members of a schedule agree, the two sides are equal, so only sides
/// made unequal can tell a reading of one side from a reading of both.
#[test]
fn a_column_holds_something_where_either_side_does() {
    use Column::*;
    let against_the_function =
        [Serves, Identity, Floor, Alive, Register, Superseded, Invalid, Documents, Presence];
    let cluster = Cluster::new(base());
    let author = cluster.e(0);
    let stamp = Stamp::new(Hlc::new(1, 0), author.node_id(), author.epoch());
    let stage = || "orders.__stg.x".to_string();
    let no_function = Ledger::default().expected(NAMES[0]);
    let no_member = View {
        seat: Seat::Member,
        serves: kimmy_core::register_function::Serves::Absent,
        identity_stands: false,
        alive: BTreeMap::new(),
        register: Vec::new(),
        superseded: BTreeMap::new(),
        invalid_held: BTreeSet::new(),
        documents: BTreeMap::new(),
        tombstones: BTreeMap::new(),
        own_tombstone: None,
        database_tombstone: None,
        options: None,
        names: None,
        physicals: BTreeMap::new(),
    };
    let mut wrong = Vec::new();
    let mut check = |held: bool, wanted: bool, what: String| {
        if held != wanted {
            wrong.push(what);
        }
    };
    let every_column: BTreeSet<Column> = columns(false).into_iter().chain(columns(true)).collect();
    for column in every_column {
        let mut member = no_member.clone();
        match column {
            Serves => member.serves = kimmy_core::register_function::Serves::Identity,
            Identity => member.identity_stands = true,
            Floor | EffectiveFloor | OwnTombstone => member.own_tombstone = Some(stamp),
            DatabaseTombstone => member.database_tombstone = Some(stamp),
            Alive => {
                member.alive.insert(stage(), Some(stamp.hlc));
            }
            Register => member.register.push((stamp, RegisterEvent::LogicalDrop)),
            Superseded => {
                member.superseded.insert(stage(), Some(stamp));
            }
            Invalid => {
                member.invalid_held.insert(stamp);
            }
            Documents | LiveRows => {
                member.documents.insert(1, (stamp, String::new()));
            }
            Tombstones => {
                member.tombstones.insert(1, stamp);
            }
            Options => member.options = Some(vec![0]),
            Names => member.names = Some(vec![0]),
            PhysicalRows => {
                member.physicals.entry(stage()).or_default().rows.insert(1, (stamp, String::new()));
            }
            PhysicalTombstones => {
                member.physicals.entry(stage()).or_default().tombstones.insert(1, stamp);
            }
            PhysicalOptions => {
                member.physicals.entry(stage()).or_default().options = Some(vec![0]);
            }
            PhysicalNames => {
                member.physicals.entry(stage()).or_default().names = Some(vec![0]);
            }
            Presence => {
                member.physicals.entry(stage()).or_default().collection_tombstone = Some(stamp);
            }
        }
        let neither = shows(column, &no_function, &no_member, &no_member);
        check(neither, false, format!("{column:?}: neither side holds anything"));
        let alone = shows(column, &no_function, &member, &no_member);
        check(alone, true, format!("{column:?}: the member's side alone"));
        if !against_the_function.contains(&column) {
            let first = shows(column, &no_function, &no_member, &member);
            check(first, true, format!("{column:?}: the first's side alone"));
            continue;
        }
        let first = shows(column, &no_function, &no_member, &member);
        check(first, false, format!("{column:?}: the first member is no side of it"));
        let mut function = no_function.clone();
        match column {
            Serves => function.answer.serves = kimmy_core::register_function::Serves::Identity,
            Identity => function.answer.identity_stands = true,
            Floor => function.answer.floor = Some(stamp),
            Alive => {
                function.answer.alive.insert(stage(), stamp.hlc);
            }
            Register => function.answer.register.push((stamp, FoldEvent::Drop)),
            Superseded => {
                function.answer.superseded.insert(stage(), Some(stamp));
            }
            Invalid => {
                function.answer.invalid.insert(stamp);
            }
            Documents => {
                function.documents.insert(1, stamp);
            }
            Presence => {
                function.answer.dead.insert(stage(), stamp);
            }
            other => panic!("{other:?} is not compared with the function"),
        }
        let alone = shows(column, &function, &no_member, &no_member);
        check(alone, true, format!("{column:?}: the function's side alone"));
    }
    assert!(wrong.is_empty(), "wrongly judged:\n{}", wrong.join("\n"));
}

#[test]
fn generated_plain_schedules_hold_the_measured_floor() {
    let start = env_u64("EQ_SEED_START", 0);
    let count = env_u64("EQ_SEEDS", DEFAULT_SEEDS);
    let started = std::time::Instant::now();
    // for one column of one set of kinds: every name, a route at a time, that differed in it
    let mut cells: BTreeMap<(Kinds, Column), Vec<String>> = BTreeMap::new();
    // for each set of kinds: the names that had it, and how many of them PIN B held
    let mut drawn: BTreeMap<Kinds, (usize, usize)> = BTreeMap::new();
    // for each set of kinds: the columns that held something in at least one of its names
    let mut seen_in: BTreeMap<Kinds, BTreeSet<Column>> = BTreeMap::new();
    let mut errors = Vec::new();
    for seed in start..start + count {
        let ops = plain_schedule(seed);
        for route in [Route::Entries, Route::Pages] {
            let run = match plain_run(&ops, route, (seed % 3) as usize) {
                Ok(run) => run,
                Err(e) => {
                    errors.push(format!("seed {seed} {route:?}: {e}\n    {}", show(&ops)));
                    continue;
                }
            };
            for (name, want, views) in &run.names {
                let of_these_kinds = drawn.entry(want.kinds.clone()).or_default();
                of_these_kinds.0 += 1;
                of_these_kinds.1 += usize::from(want.pinned_at().is_some());
                let differences = compare(want, views);
                for column in columns(!want.answer.register.is_empty()) {
                    if views.iter().any(|v| shows(column, want, v, &views[0])) {
                        seen_in.entry(want.kinds.clone()).or_default().insert(column);
                    }
                    let of_members: Vec<String> = differences
                        .iter()
                        .filter(|d| d.column == column)
                        .map(|d| format!("{}: {}", member_letter(d.member), d.detail))
                        .collect();
                    if of_members.is_empty() {
                        continue;
                    }
                    cells.entry((want.kinds.clone(), column)).or_default().push(format!(
                        "seed {seed} {route:?} {name}, {}\n        {}",
                        of_members.join("; "),
                        show(&ops)
                    ));
                }
            }
        }
    }
    // for each set of kinds, the columns that held nothing in any of its names: their agreeing there shows nothing
    let nothing_in: BTreeMap<Kinds, BTreeSet<Column>> =
        drawn.keys().map(|kinds| (kinds.clone(), held_nothing(seen_in.get(kinds)))).collect();
    let mut report = format!(
        "EQ, {count} seeds from {start}, by entries and by pages, in {:?}:\n",
        started.elapsed()
    );
    for (kinds, (names, pinned)) in &drawn {
        let differ: Vec<(&Column, &Vec<String>)> = cells
            .iter()
            .filter(|((of, _), _)| of == kinds)
            .map(|((_, column), runs)| (column, runs))
            .collect();
        let _ = writeln!(
            report,
            "  {kinds:?}: {names} name runs, PIN B held {pinned}; {}",
            if differ.is_empty() {
                "every column that held something agrees"
            } else {
                "differs in:"
            }
        );
        for (column, runs) in differ {
            let _ = writeln!(report, "    {column:?}: {} of {names}", runs.len());
            for run in runs {
                let _ = writeln!(report, "      {run}");
            }
        }
        let _ = writeln!(report, "    nothing seen in {:?}", nothing_in[kinds]);
    }
    eprintln!("{report}");
    assert!(
        errors.is_empty(),
        "{} run(s) did not reach a quiet state that can be compared:\n{}\n{report}",
        errors.len(),
        errors.join("\n")
    );
    assert!(cells.is_empty(), "members differ outside the floor and its pin:\n{report}");
    // what the library's own seeds draw is part of what is asserted
    if start == 0 && count == DEFAULT_SEEDS {
        let measured: BTreeMap<Kinds, (usize, usize)> = MEASURED
            .iter()
            .map(|(kinds, names, pinned, _)| (kinds.iter().copied().collect(), (*names, *pinned)))
            .collect();
        assert_eq!(drawn, measured, "what the seeds draw:\n{report}");
        let expected: BTreeMap<Kinds, BTreeSet<Column>> = MEASURED
            .iter()
            .map(|(kinds, _, _, also)| {
                let nothing = NOTHING_SEEN.iter().chain(also.iter()).copied().collect();
                (kinds.iter().copied().collect(), nothing)
            })
            .collect();
        assert_eq!(nothing_in, expected, "the columns with nothing in them, set by set:\n{report}");
    }
}
