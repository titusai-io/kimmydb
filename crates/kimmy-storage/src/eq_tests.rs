//! Unit EQ (ADR-272): after a schedule of deliveries, at quiescence, TWO comparisons on real engines, because each misses what
//! the other sees. **Every member against the register function** of all the facts (`kimmy_core::register_function::fold`):
//! members can all agree with one another and not with the function. **Every member against the first**: the function says
//! nothing of document bodies, options rows or names rows.
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
//! **One pinned difference, PIN B.** A database drop's burial of a collection is today an entry with a stamp of its own, above
//! the drop, and a member that buries by it keeps that stamp as the collection's floor. So where such a burial sorts above the
//! floor the function names, every member must hold the BURIAL's stamp: exactly that, so that any other floor fails, the
//! function's included. The pin goes with the change that stamps those burials at the drop. It is the only difference the floor,
//! measured over the plain steps, showed: every other column of every set of kinds agreed.
//!
//! **What this draws today: the plain collection only.** No stage, binding or recreation, no delivery out of causal order, no
//! garbage collection (a tombstone one member has collected and another has not is no difference of state), and no shadow
//! ([`SHADOWS`]). Index definitions are not a compared column; the convergence model compares them.
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
use crate::register::{Carried, stored_row_count_for_tests, stored_row_for_tests};
use crate::{
    Engine, MarkedRange, PeerWindow, ServeVerdict, SnapshotProgress, StorageError, SyncOutcome,
    WalkScope, codec, tables,
};

/// Whether the vector shadows are compared. They come with the commits that build the shadow's rules; no schedule here makes one.
const SHADOWS: bool = false;

// -- Kinds and the gate ----------------------------------------------------------------------

/// A kind of fact of a name. A database drop's own entries of the collections it buries are part of that database drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Create,
    Touch,
    Drop,
    DatabaseDrop,
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
            FoldKind::StageCreate { .. } | FoldKind::Bind { .. } | FoldKind::Recreate { .. } => {
                unreachable!("no schedule here draws a swap step: {fact:?}")
            }
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

/// Every fact's entry, by its stamp; a burial a database drop made carries that drop's `hlc`.
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<Stamp, (OplogEntry, Option<Hlc>)>,
}

/// Whether `entry` is a fact of one of the names or of their database.
fn is_a_fact(entry: &OplogEntry) -> bool {
    let of_a_name = NAMES.iter().any(|name| entry.collection == CollectionId::derive(DB, name));
    (of_a_name
        && (matches!(entry.kind, OpKind::CreateCollection | OpKind::DropCollection)
            || is_a_touch(entry.kind)))
        || (entry.kind == OpKind::DropCollection && entry.collection == database_drop_id(DB))
}

/// The facts in `e`'s oplog.
fn facts_held(e: &Engine) -> Res<Vec<OplogEntry>> {
    let window = e
        .entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background)
        .map_err(|e| err("entries_for_peer", e))?;
    Ok(window.entries.into_iter().filter(is_a_fact).collect())
}

/// The facts `e` itself wrote.
fn own_facts(e: &Engine) -> Res<BTreeMap<Stamp, OplogEntry>> {
    let node = e.node_id();
    Ok(facts_held(e)?
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
        let before = own_facts(cluster.e(author))?;
        cluster.apply(op)?;
        let new: Vec<OplogEntry> = own_facts(cluster.e(author))?
            .into_iter()
            .filter(|(stamp, _)| !before.contains_key(stamp))
            .map(|(_, entry)| entry)
            .collect();
        let database = database_drop_id(DB);
        let head = new.iter().find(|entry| entry.collection == database).map(|e| e.stamp.hlc);
        for entry in new {
            let burial = matches!(op.step, Step::DropDb { .. })
                && entry.kind == OpKind::DropCollection
                && entry.collection != database;
            let of = if burial {
                Some(head.ok_or("a database drop's burial without the drop's own entry")?)
            } else {
                None
            };
            if self.entries.insert(entry.stamp, (entry, of)).is_some() {
                return Err("two facts under one stamp".into());
            }
        }
        Ok(())
    }

    /// The facts of `name`: its own, and every drop of the database.
    fn facts(&self, name: &str) -> Vec<FoldFact> {
        let (id, database) = (CollectionId::derive(DB, name), database_drop_id(DB));
        self.entries
            .values()
            .filter_map(|(entry, of)| {
                let kind = if entry.collection == database {
                    FoldKind::DatabaseDrop
                } else if entry.collection != id {
                    return None;
                } else {
                    match entry.kind {
                        OpKind::CreateCollection => FoldKind::Create,
                        OpKind::DropCollection => FoldKind::Drop { database_drop: *of },
                        _ => FoldKind::Touch,
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
        Expected { answer, kinds, documents, touched, burial_above_the_floor }
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

// -- What a member holds ----------------------------------------------------------------------

/// One member's answer for one name, read from its store.
#[derive(Clone, Debug)]
struct View {
    serves: Serves,
    /// The collection named like the name is held, whether or not the register shows it.
    identity_stands: bool,
    /// The stages and recreations held.
    alive: BTreeSet<String>,
    register: Vec<(Stamp, RegisterEvent)>,
    superseded: BTreeMap<String, Option<Stamp>>,
    /// Of the facts the function says are history by the pure rules, those a register row is held for, in effect or parked.
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
        .map(|c| c.name)
        .collect();
    let mut documents = BTreeMap::new();
    if let Some(meta) = &identity {
        e.for_each_record_after(meta, None, WalkScope::Background, |id, stamp, doc| {
            let DocId::Int64(n) = id else { panic!("an id the model never writes") };
            documents.insert(n, (stamp, doc.to_string()));
            Ok(true)
        })
        .map_err(|e| err("for_each_record_after", e))?;
    }
    let own_tombstone = e.collection_dropped_at(id).map_err(|e| err("collection_dropped_at", e))?;
    let database_tombstone =
        e.database_dropped_at(DB).map_err(|e| err("database_dropped_at", e))?;
    let txn = e.db().begin_read().map_err(|e| err("begin_read", e))?;
    let timeline =
        crate::register::timeline_read(&txn, DB, name).map_err(|e| err("timeline_read", e))?;
    let parked = crate::register::parked_read(&txn, DB, name).map_err(|e| err("parked_read", e))?;
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
        .chain(parked.events())
        .map(|(stamp, _)| *stamp)
        .filter(|stamp| want.answer.invalid.contains(stamp))
        .collect();
    let mut tombstones = BTreeMap::new();
    {
        let records = txn.open_table(tables::DOCS).map_err(|e| err("open_table", e))?;
        for doc in &want.touched {
            let key = crate::docs::doc_key(&DocId::Int64(*doc)).map_err(|e| err("doc_key", e))?;
            let Some(raw) = records.get((id.0, key.as_slice())).map_err(|e| err("get", e))? else {
                continue;
            };
            let record =
                codec::decode_doc_record(raw.value()).map_err(|e| err("decode_doc_record", e))?;
            if record.deleted {
                if want.floor_today().is_none_or(|f| record.stamp.hlc > f.hlc) {
                    tombstones.insert(*doc, record.stamp);
                }
            } else if identity.is_none() {
                // no read of a collection shows this row, and the member holds it all the same
                let body = record.document().map_err(|e| err("document", e))?;
                documents.insert(*doc, (record.stamp, format!("no collection held: {body:?}")));
            }
        }
    }
    Ok(View {
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
    })
}

/// What no member may still hold at quiescence, and what it must not hold at all: a purge hold, a staged carry row, a pull stored
/// or registered, an event parked ahead of its position, a collection of no name, and a row of the options or names table under an
/// id of no name.
fn leftovers(e: &Engine, views: &[View]) -> Res<Vec<String>> {
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
    for (what, table, ours) in [
        ("options", Carried::Options, views.iter().filter(|v| v.options.is_some()).count()),
        ("names", Carried::Names, views.iter().filter(|v| v.names.is_some()).count()),
    ] {
        let rows = register_rows(&txn, table)?;
        if rows != ours as u64 {
            left.push(format!("{rows} {what} rows, {ours} of them under a name's id"));
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
}

/// The columns compared for a name, `swapped` or not (the module's note on floors; the documents of a swapped name have no oracle
/// of their own and are compared between members).
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
    ];
    all.into_iter()
        .filter(|c| !(swapped && matches!(c, Floor | Documents | OwnTombstone | EffectiveFloor)))
        .collect()
}

/// The function's register as storage keeps one: three kinds of event, a database drop being a logical drop. A binding or a
/// recreation that a database drop made a drop at its own stamp needs the facts that make one; no schedule here draws them.
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
/// first.
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
        let alive: BTreeSet<String> = a.alive.keys().cloned().collect();
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
        let joiner = Joiner { engine, _dir: dir };
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
        names.push((name, want, views));
    }
    for (m, e) in members.iter().enumerate() {
        let of_member: Vec<View> = names.iter().map(|(_, _, views)| views[m].clone()).collect();
        let left = leftovers(e, &of_member)?;
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
    type Change<'a> = Box<dyn Fn(&mut View) + 'a>;
    // an array with its type written out, so that each box and each list is coerced where it stands
    let changes: [(&str, Change<'_>, &[Column]); 13] = [
        ("serves", Box::new(|v: &mut View| v.serves = Serves::Absent), &[Column::Serves]),
        ("identity", Box::new(|v: &mut View| v.identity_stands = false), &[Column::Identity]),
        (
            "a held stage",
            Box::new(|v: &mut View| {
                v.alive.insert("orders.__stg.x".into());
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
    // a swapped name's floor: neither the identity's own tombstone nor the effective floor is compared, and the database's is
    let mut swapped = want.clone();
    swapped.answer.register.push((above, FoldEvent::Drop));
    for (what, change, _) in &changes {
        if !matches!(*what, "the own tombstone" | "the database tombstone alone") {
            continue;
        }
        let mut views = views.clone();
        change(&mut views[1]);
        let reported: BTreeSet<Column> = compare(&swapped, &views)
            .iter()
            .map(|d| d.column)
            .filter(|c| *c != Column::Register)
            .collect();
        let expected: BTreeSet<Column> = if *what == "the own tombstone" {
            BTreeSet::new()
        } else {
            BTreeSet::from([Column::DatabaseTombstone])
        };
        assert_eq!(reported, expected, "a swapped name, {what}");
    }
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
/// it, and how many of them PIN B held. The clocks are scripted, so these repeat. A change to either is a change in what the run
/// asserts, and is restated on purpose: a draw that shrank would pass every other assert.
///
/// **These counts are tied to the generator** ([`plain_schedule`]: the convergence model's `generate`, less its garbage
/// collections), **to the seeds `0..DEFAULT_SEEDS` and to the two routes. A deliberate change to any of them measures these
/// again, in the same commit.**
const MEASURED: &[(&[Kind], usize, usize)] = &[
    (&[], 136, 0),
    (&[Kind::Create], 78, 0),
    (&[Kind::Create, Kind::Touch], 82, 0),
    (&[Kind::Create, Kind::Drop], 32, 0),
    (&[Kind::Create, Kind::Touch, Kind::Drop], 20, 0),
    (&[Kind::DatabaseDrop], 16, 0),
    (&[Kind::Create, Kind::DatabaseDrop], 14, 8),
    (&[Kind::Create, Kind::Touch, Kind::DatabaseDrop], 14, 10),
    (&[Kind::Create, Kind::Drop, Kind::DatabaseDrop], 4, 4),
    (&[Kind::Create, Kind::Touch, Kind::Drop, Kind::DatabaseDrop], 4, 2),
];

/// The columns that held nothing in any name of the library's seeds: empty on both sides, so that their agreeing there shows
/// nothing. No step drawn here gives four of them anything. The document tombstones are the fifth kind of empty: the seeds draw
/// deletes, and none leaves a tombstone above the floor (longer runs see some; the documents row's delete reads one).
const NOTHING_SEEN: &[Column] = &[
    Column::Alive,
    Column::Register,
    Column::Superseded,
    Column::Invalid,
    Column::Tombstones,
    Column::Names,
];

/// Whether `column` has something in it for this member of this name: where it has not, its agreeing says nothing.
fn shows(column: Column, want: &Expected, v: &View) -> bool {
    match column {
        Column::Serves => v.serves != Serves::Absent,
        Column::Identity => v.identity_stands,
        Column::Floor | Column::EffectiveFloor => v.effective_floor().is_some(),
        Column::Alive => !v.alive.is_empty(),
        Column::Register => !v.register.is_empty(),
        Column::Superseded => v.superseded.values().any(Option::is_some),
        Column::Invalid => !want.answer.invalid.is_empty(),
        Column::Documents | Column::LiveRows => !v.documents.is_empty(),
        Column::Tombstones => !v.tombstones.is_empty(),
        Column::OwnTombstone => v.own_tombstone.is_some(),
        Column::DatabaseTombstone => v.database_tombstone.is_some(),
        Column::Options => v.options.is_some(),
        Column::Names => v.names.is_some(),
    }
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
    let mut seen = BTreeSet::new();
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
                    if views.iter().any(|v| shows(column, want, v)) {
                        seen.insert(column);
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
    let nothing_seen: BTreeSet<Column> =
        columns(false).into_iter().filter(|column| !seen.contains(column)).collect();
    let mut report = format!(
        "EQ, {count} seeds from {start}, by entries and by pages, in {:?}:\n  nothing seen in {nothing_seen:?}\n",
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
            if differ.is_empty() { "every column agrees" } else { "differs in:" }
        );
        for (column, runs) in differ {
            let _ = writeln!(report, "    {column:?}: {} of {names}", runs.len());
            for run in runs {
                let _ = writeln!(report, "      {run}");
            }
        }
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
            .map(|(kinds, names, pinned)| (kinds.iter().copied().collect(), (*names, *pinned)))
            .collect();
        assert_eq!(drawn, measured, "what the seeds draw:\n{report}");
        let expected: BTreeSet<Column> = NOTHING_SEEN.iter().copied().collect();
        assert_eq!(nothing_seen, expected, "the columns with nothing in them:\n{report}");
    }
}
