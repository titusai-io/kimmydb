//! EQ's swapped names (ADR-272): fixed schedules of a stage, a binding, a drop and a recreation, and a stage's creator that lost
//! what it made, held to the register function and between members, and the collection tombstones of what has ended to the values
//! wanted of each member. And two plain rows the generated run leaves thin.
//!
//! **One entry here is built by hand, the binding** ([`World::bind`]): no product code writes one yet. Everything else is made by
//! a member's own operation.
//!
//! **A member parks an event** when it applies it with no window, which outside tests only its replay of its own lost entries does.
//! Behind an event it parked, such a batch stops: it takes nothing more of that origin, and a pull brings the rest in position.
//! [`World::replay`] COPIES that replay's choice of entries (`replay_apply`, in kimmy-cluster's transport). **The transport is not
//! run here**, nor are the writes a daemon makes as it starts, nor the daemon's restore guard, which a restored member of a
//! cluster with membership passes before it replays anything; and that a push may bring a lost entry first, in a window, where it
//! takes effect and never parks, is a race no test of storage runs.

use kimmy_core::{CollectionBind, Epoch, MergePolicy};

use super::super::Member;
use super::*;

const STAGE: &str = "orders.__stg.a1";
/// The member that makes what is lost, and the member it reads it back from.
const CREATOR: usize = B as usize;
const HOLDER: usize = A as usize;

/// The stamps of the register events of `orders` that `e` holds: those in effect, and those parked.
fn register_of(e: &Engine) -> (Vec<Stamp>, Vec<Stamp>) {
    let txn = e.db().begin_read().unwrap();
    let in_effect = crate::register::timeline_read(&txn, DB, "orders").unwrap();
    let parked = crate::register::parked_read(&txn, DB, "orders").unwrap();
    let stamps = |events: &[(Stamp, RegisterEvent)]| -> Vec<Stamp> {
        events.iter().map(|(stamp, _)| *stamp).collect()
    };
    (stamps(in_effect.events()), stamps(parked.events()))
}

/// The one entry of `appended` of the kind `kind`.
fn the_one(appended: Vec<OplogEntry>, kind: OpKind) -> OplogEntry {
    let mut of_the_kind: Vec<OplogEntry> =
        appended.into_iter().filter(|entry| entry.kind == kind).collect();
    assert_eq!(of_the_kind.len(), 1, "premise: one {kind:?}: {of_the_kind:?}");
    of_the_kind.remove(0)
}

/// Members A, B and C, in which A has made `orders` and `items` and the others hold them, and the ledger of what they do.
struct World {
    cluster: Cluster,
    ledger: Ledger,
}

impl World {
    fn new() -> World {
        let mut world = World { cluster: Cluster::new(base()), ledger: Ledger::default() };
        world.steps("A.create(orders)@110; A.create(items)@111; B<-A entries; C<-A entries");
        world
    }

    fn steps(&mut self, schedule: &str) {
        for step in parse(schedule) {
            self.ledger.step(&mut self.cluster, step).unwrap();
            self.cluster.finish_purges().unwrap();
        }
    }

    /// One step of the model that makes one fact; answers its entry.
    fn made(&mut self, schedule: &str) -> OplogEntry {
        let before: BTreeSet<Stamp> = self.ledger.entries.keys().copied().collect();
        self.steps(schedule);
        let mut new: Vec<OplogEntry> = self
            .ledger
            .entries
            .iter()
            .filter(|(stamp, _)| !before.contains(stamp))
            .map(|(_, (entry, _))| entry.clone())
            .collect();
        assert_eq!(new.len(), 1, "premise: {schedule} makes one fact: {new:?}");
        new.remove(0)
    }

    /// Something member `m` does that is no step of the model; answers what it appended.
    fn does(&mut self, m: usize, act: impl FnOnce(&Engine) -> Res<()>) -> Vec<OplogEntry> {
        self.ledger.authored(&mut self.cluster, m, false, |cluster| act(cluster.e(m))).unwrap()
    }

    /// The other two take what B holds.
    fn spread(&mut self) {
        self.steps("A<-B entries; C<-B entries");
    }

    /// B makes a stage of `orders`.
    fn stage(&mut self, t: u32) -> OplogEntry {
        self.cluster.clock(CREATOR, t);
        let deadline = Hlc::new(self.cluster.base + u64::from(t) + 60_000, 0);
        let appended = self.does(CREATOR, |b| {
            b.create_stage(DB, STAGE, MergePolicy::Document, deadline)
                .map(drop)
                .map_err(|e| err("create_stage", e))
        });
        the_one(appended, OpKind::CreateCollection)
    }

    /// THE ONE HAND-BUILT ENTRY: a binding of the stage by its creator (the node and epoch of the stage's creation), stamped
    /// inside the stage's deadline. It reaches its author in a window that carries it alone.
    fn bind(&mut self, t: u32, stage: &OplogEntry) -> OplogEntry {
        let stamp = Stamp::new(
            Hlc::new(self.cluster.base + u64::from(t), 0),
            stage.stamp.node,
            stage.stamp.epoch,
        );
        assert!(stamp > stage.stamp, "premise: the binding is above the stage");
        let body = CollectionBind { db: DB.into(), name: "orders".into(), physical: STAGE.into() };
        let entry = OplogEntry {
            stamp,
            kind: OpKind::BindCollection,
            collection: CollectionId::derive(DB, "orders"),
            doc_id: None,
            body: Some(bson::serialize_to_vec(&body).unwrap()),
            meta: None,
        };
        let appended = self.does(CREATOR, |b| {
            deliver(b, &entry)?;
            b.advance_clock_past(&entry.stamp);
            Ok(())
        });
        assert_eq!(the_one(appended, OpKind::BindCollection).stamp, stamp, "premise");
        let (in_effect, parked) = register_of(self.cluster.e(CREATOR));
        assert!(
            in_effect.contains(&stamp) && parked.is_empty(),
            "premise: the author's binding is in effect: {in_effect:?}, parked {parked:?}"
        );
        entry
    }

    /// B writes two documents to what it serves and deletes the second, and the others take them: so that a physical holds a live
    /// row and a tombstone to compare. Answers the physical, and the three entries in the order they were made.
    fn write_what_is_served(&mut self, t: u32) -> (String, Vec<OplogEntry>) {
        let served = self.cluster.e(CREATOR).resolve(DB, "orders").unwrap();
        self.cluster.clock(CREATOR, t);
        let appended = self.does(CREATOR, |b| {
            for n in [1_i64, 2] {
                b.replace(&served.meta, &DocId::Int64(n), bson::doc! { "_id": n }, true)
                    .map_err(|e| err("replace", e))?;
            }
            b.delete_if(&served.meta, &DocId::Int64(2), None).map_err(|e| err("delete", e))?;
            Ok(())
        });
        assert_eq!(appended.len(), 3, "premise: two writes and a delete: {appended:?}");
        self.spread();
        (served.physical, appended)
    }

    /// A, B and C hold, of `physical`, the live document and the tombstone [`World::write_what_is_served`] left.
    fn assert_rows_held(&self, physical: &str) {
        let want = self.ledger.expected("orders");
        for m in 0..3 {
            let seen = view(self.cluster.e(m), "orders", &want).unwrap();
            let held = &seen.physicals[physical];
            assert!(
                held.rows.contains_key(&1) && held.tombstones.contains_key(&2),
                "premise: {} holds a live row and a tombstone of {physical}: {held:?}",
                member_letter(m)
            );
        }
    }

    fn backup(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.cluster.e(CREATOR).backup_to(&mut bytes, WalkScope::Background).unwrap();
        bytes
    }

    /// B is replaced by a restore of `backup`, opened as `Cluster::restart` opens a member, and is then a member that lost
    /// `lost`: the same node, a new epoch, and none of those entries.
    fn lose(&mut self, backup: &[u8], lost: &[&OplogEntry]) {
        let (node, epoch) = (self.cluster.e(CREATOR).node_id(), self.cluster.e(CREATOR).epoch());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let mut input = backup;
        crate::backup::restore(&path, &mut input).unwrap();
        let engine = Engine::open(&path).unwrap();
        model_clock(&engine, self.cluster.base);
        engine.reconcile_database_tombstones().unwrap();
        engine.reconcile_late_drops().unwrap();
        engine.finish_purges_now().unwrap();
        let restored = Member { _dir: dir, path, engine: Some(engine) };
        let old = std::mem::replace(&mut self.cluster.members[CREATOR], restored);
        old.engine.expect("the member is open").close().unwrap();
        let restored = self.cluster.e(CREATOR);
        assert_eq!(restored.node_id(), node, "premise: the restore keeps the node");
        assert_ne!(restored.epoch(), epoch, "premise: and opens a new epoch");
        for entry in lost {
            assert_eq!(entry.stamp.node, node, "premise: what is lost is B's own");
            assert!(
                !restored.has_oplog_entry(&entry.stamp).unwrap(),
                "premise: the restored member lacks the {:?}",
                entry.kind
            );
        }
    }

    /// What the transport's replay of a member's own origin applies (`replay_apply`, kimmy-cluster's transport.rs), COPIED: of
    /// the holder's entries, those of this member's node after the position its store had at open; its clock is moved past each;
    /// those its oplog does not hold are applied with no window. Requires that these are exactly `lost`; and that the batch stops
    /// at `stops_at`, behind an event it parked, or, with none, does not stop.
    fn replay(&self, lost: &[&OplogEntry], stops_at: Option<&OplogEntry>) {
        let member = self.cluster.e(CREATOR);
        let me = member.node_id();
        let from = Stamp::new(member.own_position_at_open(), me, Epoch::MAX);
        let own: Vec<OplogEntry> = entries_held(self.cluster.e(HOLDER))
            .unwrap()
            .into_iter()
            .filter(|entry| entry.stamp.node == me && entry.stamp > from)
            .collect();
        for entry in &own {
            member.advance_clock_past(&entry.stamp);
        }
        let taken: Vec<OplogEntry> = own
            .into_iter()
            .filter(|entry| !member.has_oplog_entry(&entry.stamp).unwrap())
            .collect();
        assert_eq!(
            taken.iter().map(|entry| entry.stamp).collect::<Vec<_>>(),
            lost.iter().map(|entry| entry.stamp).collect::<Vec<_>>(),
            "premise: the replay applies what was lost"
        );
        let outcome = member.apply_batch(&taken).unwrap();
        let stop = stops_at.map(|entry| entry.stamp);
        assert_eq!(
            (outcome.stopped(), outcome.stopped_at(), outcome.parked_stop),
            (stop.is_some(), stop, usize::from(stop.is_some())),
            "where the replay stops, and that a parked event is why: {outcome:?}"
        );
        member.finish_purges_now().unwrap();
    }

    /// The replay asked again while it is stalled, as the transport asks. The parked event's entry is held, so what is lost is
    /// then only `left`: it stops at the first of it, and B is as it was.
    fn replay_while_stalled(&self, left: &[&OplogEntry]) {
        let before = self.state_of_b();
        self.replay(left, Some(left[0]));
        assert_eq!(self.state_of_b(), before, "the stalled replay changes nothing on B");
    }

    /// B's vectors, register, oplog and every name's view, as text.
    fn state_of_b(&self) -> String {
        let b = self.cluster.e(CREATOR);
        let oplog: Vec<Stamp> = entries_held(b).unwrap().iter().map(|entry| entry.stamp).collect();
        let views: Vec<View> = NAMES
            .into_iter()
            .map(|name| view(b, name, &self.ledger.expected(name)).unwrap())
            .collect();
        format!(
            "servable {:?}\nwitnessed {:?}\nregister {:?}\noplog {oplog:?}\n{views:?}",
            b.version_vector().unwrap(),
            b.witnessed_vector().unwrap(),
            register_of(b)
        )
    }

    /// B's oplog holds none of `entries`.
    fn assert_left(&self, entries: &[&OplogEntry]) {
        for entry in entries {
            assert!(
                !self.cluster.e(CREATOR).has_oplog_entry(&entry.stamp).unwrap(),
                "the {:?} is left: {entry:?}",
                entry.kind
            );
        }
    }

    /// Those of `docs` under which B stores any record, live or a tombstone, in the collection named `collection`: read from the
    /// table itself, under no floor, and whether or not a collection of that name is held.
    fn records_under(&self, collection: &str, docs: &[i64]) -> Vec<i64> {
        let id = CollectionId::derive(DB, collection);
        let txn = self.cluster.e(CREATOR).db().begin_read().unwrap();
        let records = txn.open_table(crate::tables::DOCS).unwrap();
        docs.iter()
            .copied()
            .filter(|doc| {
                let key = crate::docs::doc_key(&DocId::Int64(*doc)).unwrap();
                records.get((id.0, key.as_slice())).unwrap().is_some()
            })
            .collect()
    }

    /// A member that joins from B now, while B holds an event parked.
    fn join_while_parked(&self) -> Joiner {
        let mut joiner = Joiner::join(self.cluster.base, self.cluster.e(CREATOR)).unwrap();
        joiner.seat = Seat::JoinerWhileParked;
        joiner
    }

    /// `events` are what B has parked, in stamp order: held as information, none in effect, and counted as left over.
    fn assert_parked(&self, events: &[&OplogEntry]) {
        let (in_effect, parked) = register_of(self.cluster.e(CREATOR));
        let stamps: Vec<Stamp> = events.iter().map(|event| event.stamp).collect();
        assert_eq!(parked, stamps, "parked: {events:?}");
        assert!(stamps.iter().all(|s| !in_effect.contains(s)), "none in effect: {in_effect:?}");
        let expected: Vec<(&str, Expected)> =
            NAMES.into_iter().map(|name| (name, self.ledger.expected(name))).collect();
        let wants: Vec<(&str, &Expected)> =
            expected.iter().map(|(name, want)| (*name, want)).collect();
        let left = leftovers(self.cluster.e(CREATOR), &wants).unwrap();
        let counted = format!("{} parked events", events.len());
        assert!(left.contains(&counted), "left over: {left:?}");
    }

    fn assert_released(&self, events: &[&OplogEntry]) {
        let (in_effect, parked) = register_of(self.cluster.e(CREATOR));
        assert!(
            events.iter().all(|event| in_effect.contains(&event.stamp)) && parked.is_empty(),
            "in effect: {in_effect:?}, parked {parked:?}"
        );
    }

    /// While B serves what a parked event supersedes it refuses a write to the name, and says the event is one it knows ahead of
    /// its position: the first such, `by`.
    fn assert_writes_refused(&self, by: &OplogEntry) {
        let answer = self.cluster.e(CREATOR).resolve_for_write(DB, "orders");
        assert!(
            matches!(
                &answer,
                Err(StorageError::NameSuperseded { event, parked: true, .. }) if *event == by.stamp
            ),
            "refused for the parked {:?}: {answer:?}",
            by.kind
        );
    }

    /// The physical B takes a write of `orders` to, or `None` where the name reads absent.
    fn written_to(&self) -> Option<String> {
        let answer = self.cluster.e(CREATOR).resolve_for_write(DB, "orders");
        found(answer, "resolve_for_write").unwrap().map(|resolved| resolved.physical)
    }

    /// The physical B serves for `orders`, or `None` where it serves nothing.
    fn served(&self) -> Option<String> {
        found(self.cluster.e(CREATOR).resolve(DB, "orders"), "resolve")
            .unwrap()
            .map(|resolved| resolved.physical)
    }

    /// Whether B holds the collection named `physical`.
    fn holds(&self, physical: &str) -> bool {
        found(self.cluster.e(CREATOR).get_collection(DB, physical), "get_collection")
            .unwrap()
            .is_some()
    }

    /// How far B has witnessed the origin `entry` was written under.
    fn witnessed_of(&self, entry: &OplogEntry) -> Hlc {
        self.cluster.e(CREATOR).witnessed_vector().unwrap().get(entry.stamp.origin())
    }

    /// One window from the holder to B as a pull that asks no marked range takes it: how many entries it carried, or `None`
    /// where B is behind on nothing and such a pull asks for nothing.
    fn unmarked_window(&self) -> Option<usize> {
        let (to, from) = (self.cluster.e(CREATOR), self.cluster.e(HOLDER));
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let start = mine.behind(&theirs)?;
        let window = from
            .entries_for_peer_marked(start, 1024, Some(&mine), &[], WalkScope::Background)
            .unwrap();
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
        .unwrap();
        to.finish_purges_now().unwrap();
        assert!(outcome.unknown.is_none(), "an entry of an unknown collection: {outcome:?}");
        Some(window.entries.len())
    }

    /// The end of a row: quiescence by entries, then every name in every column, against the function and between members, twice:
    /// with a member that joins from A by snapshot, and with one that joins from B, the creator. Each physical the facts name has
    /// its options row and its names row on every member. Answers the run with the joiner from A.
    fn end(&mut self) -> Quiet {
        self.end_with(None)
    }

    /// [`World::end`], and where `kept` is a member that joined earlier, the same a third time with it.
    fn end_with(&mut self, kept: Option<Joiner>) -> Quiet {
        assert!(self.cluster.quiesce(Route::Entries).unwrap(), "premise: the members quiesce");
        let mut runs = Vec::new();
        for source in [HOLDER, CREATOR] {
            let joiner = Joiner::join(self.cluster.base, self.cluster.e(source)).unwrap();
            let how = format!("joined from {}", member_letter(source));
            runs.push(self.agrees(&joiner, &how));
        }
        if let Some(joiner) = kept {
            self.agrees(&joiner, "with the member that joined from B while it was parked");
        }
        runs.swap_remove(0)
    }

    /// Bring `joiner` level and require every column of every name, and both rows of every physical, on all four. `how` says
    /// which joiner a failure was with.
    fn agrees(&mut self, joiner: &Joiner, how: &str) -> Quiet {
        let run = quiet(&mut self.cluster, &self.ledger, joiner, Route::Entries)
            .unwrap_or_else(|e| panic!("at the end, {how}: {e}"));
        let mut failures = Vec::new();
        for (name, want, views) in &run.names {
            for d in compare(want, views) {
                failures.push(format!(
                    "{name}: {} {:?}: {}",
                    member_letter(d.member),
                    d.column,
                    d.detail
                ));
            }
            for physical in want.physicals.keys() {
                for (m, v) in views.iter().enumerate() {
                    let held = v.physicals.get(physical);
                    if !held.is_some_and(|p| p.options.is_some() && p.names.is_some()) {
                        failures.push(format!(
                            "{name}: {} has not both rows of {physical}: {held:?}",
                            member_letter(m)
                        ));
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{how}, {} failure(s):\n{}",
            failures.len(),
            failures.join("\n")
        );
        run
    }
}

/// Every member holds, of `physical`, the live document and the tombstone [`World::write_what_is_served`] left.
fn assert_rows_to_compare(run: &Quiet, physical: &str) {
    let (_, _, views) = &run.names[0];
    for (m, v) in views.iter().enumerate() {
        let held = &v.physicals[physical];
        assert!(
            held.rows.contains_key(&1) && held.tombstones.contains_key(&2),
            "premise: {} holds a live row and a tombstone of {physical}: {held:?}",
            member_letter(m)
        );
    }
}

/// The stamp pin applies to the stage in `run`: a member is wanted at the drop's stamp, and one that joins after the recreation
/// at the recreation's. Where it did not apply, the stage would be wanted held at any stamp and the row would still pass.
fn assert_stamp_pin_applies(run: &Quiet, dropped: &OplogEntry, recreated: &OplogEntry) {
    let (_, want, _) = &run.names[0];
    let pinned = [Seat::Member, Seat::JoinerAfterRelease].map(|seat| stamp_pin(want, STAGE, seat));
    assert_eq!(pinned, [Some(dropped.stamp), Some(recreated.stamp)], "premise: the pin applies");
}

// -- A creator that lost what it made ----------------------------------------------------------

/// The binding alone is lost. The replay parks it; the next window brings it into effect. And at the end, each read that held
/// nothing for a plain name is shown to find something in a store.
#[test]
fn a_lost_binding_is_parked_and_the_next_window_releases_it() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.spread();
    let backup = world.backup();
    let bind = world.bind(130, &stage);
    world.spread();
    world.lose(&backup, &[&bind]);
    world.replay(&[&bind], None);
    world.assert_parked(&[&bind]);
    assert_eq!(world.served(), Some("orders".into()), "as restored, the identity is served");
    world.assert_writes_refused(&bind);
    assert_eq!(world.witnessed_of(&bind), stage.stamp.hlc, "the vector is below the binding");
    assert_eq!(world.unmarked_window(), Some(1));
    world.assert_released(&[&bind]);
    assert_eq!(world.served(), Some(STAGE.into()));
    assert_eq!(world.written_to(), Some(STAGE.into()));
    let served = world.write_what_is_served(200).0;
    let run = world.end();
    assert_rows_to_compare(&run, &served);

    let (_, want, views) = &run.names[0];
    assert!(pin_p(want, Seat::JoinerAfterRelease), "premise: PIN P applies to the bound-out name");
    for (m, v) in views.iter().enumerate() {
        let m = member_letter(m);
        let stages = BTreeMap::from([(STAGE.to_string(), Some(stage.stamp.hlc))]);
        assert_eq!(v.alive, stages, "{m}: premise: the stage held, with its creation's stamp");
        assert!(!v.register.is_empty(), "{m}: premise: a register to compare");
        assert_eq!(
            v.superseded.get("orders"),
            Some(&Some(bind.stamp)),
            "{m}: premise: the identity is superseded at the binding"
        );
        assert!(v.names.is_some(), "{m}: premise: the name's names row");
    }
    // the read of what is held for an invalid fact: asked of a want that calls the binding invalid, it finds the binding
    let mut called_invalid = want.clone();
    called_invalid.answer.invalid.insert(bind.stamp);
    let read = view(world.cluster.e(HOLDER), "orders", &called_invalid).unwrap();
    assert_eq!(read.invalid_held, BTreeSet::from([bind.stamp]), "premise: the read finds a row");
    // `leftovers` told of no physical of `orders` reports the stage's two rows, each by its id
    let mut told_nothing = want.clone();
    told_nothing.physicals.clear();
    let (_, of_items, _) = &run.names[1];
    let wants = [("orders", &told_nothing), ("items", of_items)];
    let left = leftovers(world.cluster.e(HOLDER), &wants).unwrap();
    let id = CollectionId::derive(DB, STAGE).0.to_be_bytes();
    let strays = ["options", "names"].map(|table| {
        format!("the {table} table: a row under the id of no name and no physical: {id:02x?}")
    });
    assert_eq!(left, strays);
}

/// The backup is older than the stage. The replay makes the stage, which is held, and parks the binding.
#[test]
fn a_lost_stage_and_binding_leave_the_stage_held_and_the_binding_parked() {
    let mut world = World::new();
    let backup = world.backup();
    let stage = world.stage(120);
    let bind = world.bind(130, &stage);
    world.spread();
    // another member's write after the backup, which B has never held: the replay is of B's own entries and must leave it
    let others = world.made("A.write(items/1)@135");
    world.lose(&backup, &[&stage, &bind]);
    assert!(!world.holds(STAGE), "premise: the restored member holds no stage");
    assert!(
        !world.cluster.e(CREATOR).has_oplog_entry(&others.stamp).unwrap(),
        "premise: B lacks A's write"
    );
    world.replay(&[&stage, &bind], None);
    world.assert_parked(&[&bind]);
    assert!(world.holds(STAGE), "the stage's creation is applied");
    assert_eq!(world.served(), Some("orders".into()));
    assert_eq!(world.witnessed_of(&bind), stage.stamp.hlc, "the vector is below the binding");
    assert_eq!(world.unmarked_window(), Some(2), "the binding and A's write");
    world.assert_released(&[&bind]);
    assert_eq!(world.served(), Some(STAGE.into()));
    let served = world.write_what_is_served(200).0;
    let run = world.end();
    assert_rows_to_compare(&run, &served);
}

/// The lost entry is the drop of a swapped name. While it is parked the member still serves the stage.
#[test]
fn a_lost_drop_of_a_swapped_name_is_parked_and_the_stage_still_served() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.bind(130, &stage);
    world.spread();
    assert_eq!(world.write_what_is_served(135).0, STAGE);
    world.assert_rows_held(STAGE);
    let backup = world.backup();
    let dropped = world.made("B.drop(orders)@140");
    let (in_effect, _) = register_of(world.cluster.e(CREATOR));
    assert!(in_effect.contains(&dropped.stamp), "premise: the drop is a register event");
    world.spread();
    world.lose(&backup, &[&dropped]);
    world.replay(&[&dropped], None);
    world.assert_parked(&[&dropped]);
    assert_eq!(world.served(), Some(STAGE.into()), "the stage is still served");
    assert!(world.holds(STAGE));
    world.assert_writes_refused(&dropped);
    assert!(world.witnessed_of(&dropped) < dropped.stamp.hlc, "the vector is below the drop");
    assert_eq!(world.unmarked_window(), Some(1));
    world.assert_released(&[&dropped]);
    assert_eq!(world.served(), None);
    assert_eq!(world.written_to(), None);
    assert!(!world.holds(STAGE), "and the stage is buried");
    let run = world.end();
    let (_, want, views) = &run.names[0];
    // no recreation follows the drop, so a member that joins afterwards is wanted at the drop's stamp, as the members are
    assert_eq!(
        stamp_pin(want, STAGE, Seat::JoinerAfterRelease),
        Some(dropped.stamp),
        "premise: the stamp pin's control"
    );
    for (m, v) in views.iter().enumerate() {
        let held = &v.physicals[STAGE];
        assert!(held.rows.is_empty(), "{}: no row of the buried stage: {held:?}", member_letter(m));
    }
}

/// The lost entry is a recreation. While it is parked the name serves nothing, and the recreation's physical is held.
#[test]
fn a_lost_recreation_is_parked_and_the_name_serves_nothing() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.bind(130, &stage);
    world.spread();
    let dropped = world.made("B.drop(orders)@140");
    world.spread();
    let backup = world.backup();
    let recreated = world.made("B.create(orders)@150");
    let Some((_, FoldKind::Recreate { physical })) = name_fact(&recreated) else {
        panic!("premise: B's creation is a recreation: {recreated:?}");
    };
    world.spread();
    world.lose(&backup, &[&recreated]);
    assert!(!world.holds(&physical), "premise: the restored member holds no recreation");
    world.replay(&[&recreated], None);
    world.assert_parked(&[&recreated]);
    assert_eq!(world.served(), None);
    assert!(world.holds(&physical), "the physical is made, and hidden");
    assert_eq!(world.witnessed_of(&recreated), dropped.stamp.hlc, "below the recreation");
    assert_eq!(world.unmarked_window(), Some(1));
    world.assert_released(&[&recreated]);
    assert_eq!(world.served(), Some(physical.clone()));
    assert_eq!(world.write_what_is_served(200).0, physical);
    let run = world.end();
    assert_stamp_pin_applies(&run, &dropped, &recreated);
    assert_rows_to_compare(&run, &physical);
}

/// The binding, the drop of the name it swapped and the recreation are all lost. The replay parks the binding and stops at the
/// drop, so the recreation's physical is not made. One window brings all three, each in its place.
#[test]
fn a_replay_of_three_lost_events_stops_at_the_drop_and_one_window_releases_all() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.spread();
    let backup = world.backup();
    let bind = world.bind(130, &stage);
    let dropped = world.made("B.drop(orders)@140");
    let recreated = world.made("B.create(orders)@150");
    let Some((_, FoldKind::Recreate { physical })) = name_fact(&recreated) else {
        panic!("premise: B's creation is a recreation: {recreated:?}");
    };
    world.spread();
    let lost = [&bind, &dropped, &recreated];
    world.lose(&backup, &lost);
    world.replay(&lost, Some(&dropped));
    world.assert_parked(&[&bind]);
    world.assert_left(&[&dropped, &recreated]);
    assert!(!world.holds(&physical), "the recreation is left, so its physical is not made");
    assert_eq!(world.served(), Some("orders".into()), "as restored, the identity is served");
    world.assert_writes_refused(&bind);
    assert_eq!(world.witnessed_of(&bind), stage.stamp.hlc, "the vector is below all three");
    world.replay_while_stalled(&[&dropped, &recreated]);
    assert_eq!(world.unmarked_window(), Some(3));
    world.assert_released(&lost);
    assert_eq!(world.served(), Some(physical.clone()));
    assert_eq!(world.written_to(), Some(physical.clone()));
    assert_eq!(world.write_what_is_served(200).0, physical);
    let run = world.end();
    assert_stamp_pin_applies(&run, &dropped, &recreated);
    assert_rows_to_compare(&run, &physical);
}

/// The binding and a later write of B's own, to another collection, are lost. The replay parks the binding and stops at the
/// write: B stores nothing of it, and its vector stays below the binding. A member that joins from B then is held, with the
/// others, to every column at the end.
#[test]
fn a_later_write_of_the_members_own_is_left_behind_the_parked_binding() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.spread();
    let backup = world.backup();
    let bind = world.bind(130, &stage);
    let write = world.made("B.write(items/1)@135");
    assert!(
        write.stamp > bind.stamp && write.stamp.epoch == bind.stamp.epoch,
        "premise: B's own write, stamped above the binding"
    );
    world.spread();
    world.lose(&backup, &[&bind, &write]);
    world.replay(&[&bind, &write], Some(&write));
    world.assert_parked(&[&bind]);
    world.assert_left(&[&write]);
    assert_eq!(world.records_under("items", &[1]), [0_i64; 0], "no document, and no tombstone");
    assert_eq!(world.served(), Some("orders".into()), "as restored, the identity is served");
    world.assert_writes_refused(&bind);
    assert_eq!(world.witnessed_of(&bind), stage.stamp.hlc, "the vector is below the binding");
    world.replay_while_stalled(&[&write]);
    let parked = world.join_while_parked();
    let (in_effect, _) = register_of(&parked.engine);
    assert!(!in_effect.contains(&bind.stamp), "premise: it joined without the binding in effect");
    let identity = CollectionId::derive(DB, "orders");
    assert!(
        found(parked.engine.get_collection(DB, "orders"), "get_collection").unwrap().is_some()
            && parked.engine.collection_dropped_at(identity).unwrap().is_none(),
        "premise: at the join it holds the identity collection, and no tombstone of it"
    );
    assert_eq!(world.unmarked_window(), Some(2), "the binding and the write");
    world.assert_released(&[&bind]);
    assert_eq!(world.served(), Some(STAGE.into()));
    assert_eq!(world.records_under("items", &[1]), [1], "and B stores the document");
    let served = world.write_what_is_served(200).0;
    let run = world.end_with(Some(parked));
    assert_rows_to_compare(&run, &served);
}

/// The binding and what was then written into the stage it bound, two writes and a delete, are lost. The replay parks the
/// binding and stops at the first write: B stores nothing of them under the stage. One window releases the binding and brings
/// the three into it.
#[test]
fn writes_into_a_newly_bound_stage_are_left_behind_the_parked_binding() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.spread();
    let backup = world.backup();
    let bind = world.bind(130, &stage);
    let (served, written) = world.write_what_is_served(135);
    assert_eq!(served, STAGE, "premise: the writes are into the stage");
    let left: Vec<&OplogEntry> = written.iter().collect();
    let lost: Vec<&OplogEntry> = std::iter::once(&bind).chain(&written).collect();
    world.lose(&backup, &lost);
    world.replay(&lost, Some(left[0]));
    world.assert_parked(&[&bind]);
    world.assert_left(&left);
    assert_eq!(world.records_under(STAGE, &[1, 2]), [0_i64; 0], "no row, and no tombstone");
    assert_eq!(world.served(), Some("orders".into()), "as restored, the identity is served");
    world.assert_writes_refused(&bind);
    assert_eq!(world.witnessed_of(&bind), stage.stamp.hlc, "the vector is below the binding");
    assert_eq!(world.unmarked_window(), Some(4));
    world.assert_released(&[&bind]);
    assert_eq!(world.served(), Some(STAGE.into()));
    assert_eq!(world.records_under(STAGE, &[1, 2]), [1, 2], "and B stores both records");
    world.assert_rows_held(STAGE);
    let run = world.end();
    assert_rows_to_compare(&run, STAGE);
}

/// A recreation and what was then written into it are lost. The replay parks the recreation, whose physical it makes and hides,
/// and stops at the first write: B stores nothing of them under it. One window releases it and brings the three into it.
#[test]
fn writes_into_a_newly_served_recreation_are_left_behind_the_parked_recreation() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.bind(130, &stage);
    world.spread();
    let dropped = world.made("B.drop(orders)@140");
    world.spread();
    let backup = world.backup();
    let recreated = world.made("B.create(orders)@150");
    let Some((_, FoldKind::Recreate { physical })) = name_fact(&recreated) else {
        panic!("premise: B's creation is a recreation: {recreated:?}");
    };
    world.spread();
    let (served, written) = world.write_what_is_served(155);
    assert_eq!(served, physical, "premise: the writes are into the recreation");
    let left: Vec<&OplogEntry> = written.iter().collect();
    let lost: Vec<&OplogEntry> = std::iter::once(&recreated).chain(&written).collect();
    world.lose(&backup, &lost);
    assert!(!world.holds(&physical), "premise: the restored member holds no recreation");
    world.replay(&lost, Some(left[0]));
    world.assert_parked(&[&recreated]);
    world.assert_left(&left);
    assert!(world.holds(&physical), "the physical is made, and hidden");
    assert_eq!(world.records_under(&physical, &[1, 2]), [0_i64; 0], "no row, and no tombstone");
    assert_eq!(world.served(), None);
    assert_eq!(world.witnessed_of(&recreated), dropped.stamp.hlc, "below the recreation");
    assert_eq!(world.unmarked_window(), Some(4));
    world.assert_released(&[&recreated]);
    assert_eq!(world.served(), Some(physical.clone()));
    assert_eq!(world.records_under(&physical, &[1, 2]), [1, 2], "and B stores both records");
    world.assert_rows_held(&physical);
    let run = world.end();
    assert_stamp_pin_applies(&run, &dropped, &recreated);
    assert_rows_to_compare(&run, &physical);
}

// -- The stamp of a dropped stage's tombstone -------------------------------------------------

/// No member loses anything: a stage, its binding, a drop of the name and a recreation, taken by the members as they are made,
/// then the two joins. The stage's tombstone is wanted at the drop's stamp on A, B and C, and at the recreation's on a member
/// that joins afterwards.
#[test]
fn a_dropped_stage_under_a_recreation_is_held_to_the_stamp_pin() {
    let mut world = World::new();
    let stage = world.stage(120);
    world.bind(130, &stage);
    world.spread();
    let dropped = world.made("B.drop(orders)@140");
    world.spread();
    let recreated = world.made("B.create(orders)@150");
    world.spread();
    let run = world.end();
    assert_stamp_pin_applies(&run, &dropped, &recreated);
}

// -- What the plain generated run leaves thin -------------------------------------------------

/// Each of `schedules` by both routes, with a joiner from B: its set of kinds is `kinds`, `premise` holds of `orders`, and every
/// column of every name agrees.
fn both_drops(schedules: [&str; 2], kinds: &[Kind], premise: fn(&Expected, &[View]) -> bool) {
    let kinds: Kinds = kinds.iter().copied().collect();
    let mut failures = Vec::new();
    for schedule in schedules {
        let ops = parse(schedule);
        for route in [Route::Entries, Route::Pages] {
            let run = match plain_run(&ops, route, B as usize) {
                Ok(run) => run,
                Err(e) => {
                    failures.push(format!("{route:?} {schedule}: {e}"));
                    continue;
                }
            };
            let (_, want, views) = &run.names[0];
            if want.kinds != kinds {
                failures.push(format!("{route:?} {schedule}: premise: kinds {:?}", want.kinds));
            }
            if !premise(want, views) {
                failures.push(format!("{route:?} {schedule}: the premise does not hold"));
            }
            for (name, want, views) in &run.names {
                for d in compare(want, views) {
                    failures.push(format!(
                        "{route:?} {schedule}: {name} {} {:?}: {}",
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

/// A write above both a collection's drop and a database's, by a member that had heard of neither. In the generated run no name
/// with these four kinds had a live document.
#[test]
fn a_write_above_a_collection_drop_and_a_database_drop_survives() {
    both_drops(
        [
            "A.create(orders)@110; A.write(orders/1)@120; B<-A entries; C<-A entries; A.drop(orders)@130; B.dropdb@140; \
             C.write(orders/2)@200",
            "A.create(orders)@110; A.write(orders/1)@120; B<-A entries; C<-A entries; B.dropdb@130; A.drop(orders)@140; \
             C.write(orders/2)@200",
        ],
        &[Kind::Create, Kind::Touch, Kind::Drop, Kind::DatabaseDrop],
        |want, views| !want.documents.is_empty() && views.iter().all(|v| !v.documents.is_empty()),
    );
}

/// A creation above both. In the generated run no name with these three kinds had its collection standing.
#[test]
fn a_creation_above_a_collection_drop_and_a_database_drop_stands() {
    both_drops(
        [
            "A.create(orders)@110; B<-A entries; A.drop(orders)@130; B.dropdb@140; A<-B entries; A.create(orders)@200",
            "A.create(orders)@110; B<-A entries; B.dropdb@130; A.drop(orders)@140; A<-B entries; A.create(orders)@200",
        ],
        &[Kind::Create, Kind::Drop, Kind::DatabaseDrop],
        |want, views| want.answer.identity_stands && views.iter().all(|v| v.identity_stands),
    );
}
