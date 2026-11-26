//! The plain collection's life, over every order of delivery (ADR-272 convergence design, unit P; the plain finding's cases a, b, b2, the empty b2, and the concurrent creation).
//!
//! `FINDING-plain-empty-life-divergence-for-chris.md` and `u14-plain-rows.py` (the model rows: the fold converges in every order, the build does not). No stage anywhere. A member decides what an
//! arriving creation or drop means from what it holds when it arrives, and members that take the same entries in different orders end in different lives, or none. After unit P every member
//! ends holding the same life, named by the drop it follows, whatever the order.
//!
//! Each case is built by real members with real local operations (their stamps rise with the wall clock, a few ms apart; the premise is asserted), and then ONE member, rebuilt for each order, takes
//! the facts it has not applied yet in every order that keeps each author's order and each fact's causal past (the model's `orders`). What a member holds at the end is compared with what the fold
//! gives, written below per case: the answer, the drop its life follows (the identity's tombstone), the creation of the life it holds, and its documents. The tests collect every mismatch and fail
//! with the list, so a run says how many orders are red and which.
//!
//! RED TODAY (the build judges on arrival; confirmed by u14-p1, Nov 26): a, b, b2, b2e (the orders in which the later creation is taken BEFORE the middle drop: the life is lost), and
//! `the_outcome_does_not_depend_on_when_the_purger_runs` (a, b, b2, b2e AND cc, in the orders where a creation meets data not yet purged and nothing offers it again; CORRECTED Nov 26 at hardproblems' P review: the test now offers again WHAT WAS NOT TAKEN, as the transport does, so those waits are green today and its red set is read from the first run: see the test). `cc` is a GUARD today: green
//! with the purger running at once, red only under the late purger (the model said red; the real build is not). Green after unit P. Not committed: they go in with unit P's first commit,
//! "the tests, failing first".

use std::collections::BTreeMap;

use bson::doc;
use kimmy_core::{CollectionId, Hlc, OpKind, OplogEntry, Stamp, VersionVector};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

/// When the purger runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Purge {
    /// after every entry, and after the members' own drops in the script
    Eager,
    /// only after every entry of the order has been offered; then ONLY WHAT THE MEMBER DID NOT TAKE is offered again, as the transport does (a window that stopped at an entry because a purge is pending, or because the collection is unknown,
    /// leaves it untaken and the next round offers it again), until nothing is left untaken (hardproblems' P review, adopted 12:25 PM, A5). An entry the member TOOK (applied, or judged history) is not offered again.
    AtEnd,
    /// as `AtEnd`, and then the same entries are offered again in the same order (as anti-entropy offers what a member has not applied), the purger running after each
    AtEndThenOffered,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    /// a: nothing is written to the life that follows the middle drop
    A,
    /// b: one insert, stamped after drop 3
    B,
    /// b2: b, and a fourth member's drop stamped between drop 3 and creation 4
    B2,
    /// the empty b2
    B2Empty,
    /// two creations above one drop, concurrent
    Cc,
    /// cc, and a drop stamped strictly BETWEEN the two creations (c2 < d3 < c4) by a third member that has taken c2 but not c4: a drop of c2's life, concurrent with c4. NO write anywhere: the empty life
    CcDropEmpty,
    /// the same, with a write i2 into c2's life (below the drop: dead) and a write W = i5 above c4 (the life's only document)
    CcDropW,
    /// the SECOND LEG of `CcDropEmpty`: the drop between c2 and c4 is a DATABASE drop (the database sweep's own split, `created <= dropped`), not a collection drop
    CcDbDropEmpty,
    /// the second leg of `CcDropW`
    CcDbDropW,
}

impl Shape {
    /// The drop-between shapes: (with writes, the drop is a database drop).
    fn drop_between(self) -> Option<(bool, bool)> {
        match self {
            Shape::CcDropEmpty => Some((false, false)),
            Shape::CcDropW => Some((true, false)),
            Shape::CcDbDropEmpty => Some((false, true)),
            Shape::CcDbDropW => Some((true, true)),
            _ => None,
        }
    }
}

/// One built world: the members, and the facts by name.
struct World {
    members: Vec<Engine>,
    _dirs: Vec<tempfile::TempDir>,
    facts: BTreeMap<&'static str, OplogEntry>,
    /// the author of each fact (a member index)
    author: BTreeMap<&'static str, usize>,
    /// the facts a fact's author had applied before writing it
    deps: BTreeMap<&'static str, Vec<&'static str>>,
    /// per member: the facts it applied while the world was built (its own operations, and what it took before them)
    prefix: Vec<Vec<&'static str>>,
}

fn own(m: &Engine, kind: OpKind) -> OplogEntry {
    let identity = CollectionId::derive(DB, "orders");
    everything(m)
        .into_iter()
        .filter(|e| e.kind == kind && e.collection == identity && e.stamp.node == m.node_id())
        .last()
        .expect("the member logged it")
}

fn own_write(m: &Engine, above: Hlc) -> OplogEntry {
    let identity = CollectionId::derive(DB, "orders");
    everything(m)
        .into_iter()
        .filter(|e| {
            e.doc_id.is_some()
                && e.collection == identity
                && e.stamp.node == m.node_id()
                && e.stamp.hlc > above
        })
        .last()
        .expect("the member logged the write")
}

/// `lazy`: the members that drop in the script do not run the purger afterwards (the purger of a real member runs in the background, some time after the drop).
fn build(shape: Shape, lazy: bool) -> World {
    if let Some((writes, db_drop)) = shape.drop_between() {
        return build_cc_drop(lazy, writes, db_drop);
    }
    let (s, _ds) = with_orders();
    let four = matches!(shape, Shape::B2 | Shape::B2Empty);
    let n = if four { 4 } else { 3 };
    let mut members = Vec::new();
    let mut dirs = Vec::new();
    for _ in 0..n {
        let (m, d) = engine();
        window(&m, &everything(&s));
        members.push(m);
        dirs.push(d);
    }
    let identity = CollectionId::derive(DB, "orders");
    let mut facts = BTreeMap::new();
    let mut author = BTreeMap::new();
    let mut deps: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    let mut prefix: Vec<Vec<&'static str>> = vec![Vec::new(); n];
    // m1 drops orders (1) and creates it again (2)
    assert!(members[1].drop_collection(DB, "orders").unwrap());
    let d1 = own(&members[1], OpKind::DropCollection);
    members[1].purge_dropped_collection(identity).unwrap();
    let life2 = members[1].create_collection(DB, "orders").unwrap();
    let c2 = own(&members[1], OpKind::CreateCollection);
    prefix[1] = vec!["d1", "c2"];
    // cc: one insert into each life, so that the two lives differ by what they hold
    let i2 = if shape == Shape::Cc {
        pause();
        members[1].insert(&life2, doc! { "_id": 8, "v": "life 2" }).unwrap();
        prefix[1].push("i2");
        Some(own_write(&members[1], c2.stamp.hlc))
    } else {
        None
    };
    // m0, which has heard nothing, drops its orders (3)
    pause();
    let d3 = if shape == Shape::Cc {
        None
    } else {
        assert!(members[0].drop_collection(DB, "orders").unwrap());
        if !lazy {
            members[0].purge_dropped_collection(identity).unwrap();
        }
        prefix[0] = vec!["d3"];
        Some(own(&members[0], OpKind::DropCollection))
    };
    // m3, which still holds the original life, drops orders after drop 3 and before creation 4 (b2)
    let d3b = if four {
        pause();
        assert!(members[3].drop_collection(DB, "orders").unwrap());
        if !lazy {
            members[3].purge_dropped_collection(identity).unwrap();
        }
        prefix[3] = vec!["d3b"];
        Some(own(&members[3], OpKind::DropCollection))
    } else {
        None
    };
    // m2 has taken m1's drop only, and creates orders (4)
    window(&members[2], std::slice::from_ref(&d1));
    members[2].purge_dropped_collection(identity).unwrap();
    pause();
    let life4 = members[2].create_collection(DB, "orders").unwrap();
    let c4 = own(&members[2], OpKind::CreateCollection);
    prefix[2] = vec!["d1", "c4"];
    facts.insert("d1", d1.clone());
    facts.insert("c2", c2.clone());
    facts.insert("c4", c4.clone());
    author.insert("d1", 1);
    author.insert("c2", 1);
    author.insert("c4", 2);
    deps.insert("d1", vec![]);
    deps.insert("c2", vec!["d1"]);
    deps.insert("c4", vec!["d1"]);
    assert!(d1.stamp.hlc < c2.stamp.hlc, "premise: 1 < 2");
    if let Some(d3) = &d3 {
        assert!(c2.stamp.hlc < d3.stamp.hlc && d3.stamp.hlc < c4.stamp.hlc, "premise: 2 < 3 < 4");
        facts.insert("d3", d3.clone());
        author.insert("d3", 0);
        deps.insert("d3", vec![]);
    } else {
        assert!(c2.stamp.hlc < c4.stamp.hlc, "premise: 2 < 4");
    }
    if let (Some(d3), Some(d3b)) = (&d3, &d3b) {
        assert!(
            d3.stamp.hlc < d3b.stamp.hlc && d3b.stamp.hlc < c4.stamp.hlc,
            "premise: 3 < 3b < 4"
        );
        facts.insert("d3b", d3b.clone());
        author.insert("d3b", 3);
        deps.insert("d3b", vec![]);
    }
    if let Some(i2) = i2 {
        assert!(i2.stamp.hlc < c4.stamp.hlc, "premise: the insert of life 2 is below creation 4");
        facts.insert("i2", i2);
        author.insert("i2", 1);
        deps.insert("i2", vec!["d1", "c2"]);
        pause();
        members[2].insert(&life4, doc! { "_id": 9, "v": "life 4" }).unwrap();
        let i4 = own_write(&members[2], c4.stamp.hlc);
        facts.insert("i4", i4);
        author.insert("i4", 2);
        deps.insert("i4", vec!["d1", "c4"]);
        prefix[2].push("i4");
    }
    if matches!(shape, Shape::B | Shape::B2) {
        pause();
        members[2].insert(&life4, doc! { "_id": 7, "v": "after the drop" }).unwrap();
        let i5 = own_write(&members[2], c4.stamp.hlc);
        assert!(i5.stamp.hlc > c4.stamp.hlc);
        facts.insert("i5", i5);
        author.insert("i5", 2);
        deps.insert("i5", vec!["d1", "c4"]);
        prefix[2].push("i5");
    }
    World { members, _dirs: dirs, facts, author, deps, prefix }
}

/// `Shape::CcDropEmpty` / `Shape::CcDropW` (`writes`): m1 drops the original (d1), creates orders (c2) and, with `writes`, inserts into life 2 (i2); m0 takes d1, c2 (and i2) and then DROPS life 2 (d3:
/// stamped after c2, before c4; its author has not taken c4); m2 took d1 only, creates orders (c4) after d3 and, with `writes`, inserts into life 4 (i5 = W, above c4). The three authors are different
/// members. The fold: the newest drop is d3 (the floor); the life is the first creation above it, c4; its documents are the inserts above c4: W alone (i2 is below the drop and dead), none when empty.
/// `db_drop`: d3 is a DATABASE drop (`drop_database`, its entry addressed to `database_drop_id`) instead of a collection drop: the second leg.
fn build_cc_drop(lazy: bool, writes: bool, db_drop: bool) -> World {
    let (s, _ds) = with_orders();
    let mut members = Vec::new();
    let mut dirs = Vec::new();
    for _ in 0..3 {
        let (m, d) = engine();
        window(&m, &everything(&s));
        members.push(m);
        dirs.push(d);
    }
    let identity = CollectionId::derive(DB, "orders");
    assert!(members[1].drop_collection(DB, "orders").unwrap());
    let d1 = own(&members[1], OpKind::DropCollection);
    members[1].purge_dropped_collection(identity).unwrap();
    let life2 = members[1].create_collection(DB, "orders").unwrap();
    let c2 = own(&members[1], OpKind::CreateCollection);
    let i2 = if writes {
        pause();
        members[1].insert(&life2, doc! { "_id": 8, "v": "life 2" }).unwrap();
        Some(own_write(&members[1], c2.stamp.hlc))
    } else {
        None
    };
    // m0 takes 1, 2 (and the insert), then drops life 2
    pause();
    window(&members[0], std::slice::from_ref(&d1));
    members[0].purge_dropped_collection(identity).unwrap();
    window(&members[0], std::slice::from_ref(&c2));
    if let Some(i2) = &i2 {
        window(&members[0], std::slice::from_ref(i2));
    }
    pause();
    let d3 = if db_drop {
        assert!(members[0].drop_database(DB).unwrap());
        if !lazy {
            let _ = members[0].purge_dropped_collection(identity);
        }
        let database = kimmy_core::database_drop_id(DB);
        everything(&members[0])
            .into_iter()
            .rfind(|e| {
                e.kind == OpKind::DropCollection
                    && e.collection == database
                    && e.stamp.node == members[0].node_id()
            })
            .expect("the member logged the database drop")
    } else {
        assert!(members[0].drop_collection(DB, "orders").unwrap());
        if !lazy {
            members[0].purge_dropped_collection(identity).unwrap();
        }
        own(&members[0], OpKind::DropCollection)
    };
    // m2 took drop 1 only, creates orders after drop 3 and writes into it
    pause();
    window(&members[2], std::slice::from_ref(&d1));
    members[2].purge_dropped_collection(identity).unwrap();
    pause();
    let life4 = members[2].create_collection(DB, "orders").unwrap();
    let c4 = own(&members[2], OpKind::CreateCollection);
    let i5 = if writes {
        pause();
        members[2].insert(&life4, doc! { "_id": 9, "v": "life 4" }).unwrap();
        Some(own_write(&members[2], c4.stamp.hlc))
    } else {
        None
    };
    assert!(d1.stamp.hlc < c2.stamp.hlc, "premise: 1 < 2");
    assert!(
        c2.stamp.hlc < d3.stamp.hlc && d3.stamp.hlc < c4.stamp.hlc,
        "premise: 2 < 3 < 4 (the drop is strictly between the two creations)"
    );
    if let Some(i2) = &i2 {
        assert!(i2.stamp.hlc < d3.stamp.hlc, "premise: the insert of life 2 is below the drop");
    }
    if let Some(i5) = &i5 {
        assert!(c4.stamp.hlc < i5.stamp.hlc, "premise: the insert of life 4 is above creation 4");
    }
    let mut facts = BTreeMap::new();
    let mut author = BTreeMap::new();
    let mut deps: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    let mut prefix = vec![vec!["d1", "c2"], vec!["d1", "c2"], vec!["d1", "c4"]];
    let mut put = |name: &'static str, entry: OplogEntry, who: usize, dep: Vec<&'static str>| {
        facts.insert(name, entry);
        author.insert(name, who);
        deps.insert(name, dep);
    };
    put("d1", d1, 1, vec![]);
    put("c2", c2, 1, vec!["d1"]);
    put("c4", c4, 2, vec!["d1"]);
    if let Some(i2) = i2 {
        put("i2", i2, 1, vec!["d1", "c2"]);
        prefix[0].push("i2");
        prefix[1].push("i2");
    }
    put("d3", d3, 0, if writes { vec!["d1", "c2", "i2"] } else { vec!["d1", "c2"] });
    prefix[0].push("d3");
    if let Some(i5) = i5 {
        put("i5", i5, 2, vec!["d1", "c4"]);
        prefix[2].push("i5");
    }
    World { members, _dirs: dirs, facts, author, deps, prefix }
}

/// Every order in which member `who` can take the facts it has not applied: each author's facts in stamp order, each fact after the facts its author had applied.
fn orders(w: &World, who: usize) -> Vec<Vec<&'static str>> {
    let free: Vec<&'static str> =
        w.facts.keys().copied().filter(|f| !w.prefix[who].contains(f)).collect();
    let mut out = Vec::new();
    fn go(
        w: &World,
        applied: &mut Vec<&'static str>,
        left: &mut Vec<&'static str>,
        acc: &mut Vec<&'static str>,
        out: &mut Vec<Vec<&'static str>>,
    ) {
        if left.is_empty() {
            out.push(acc.clone());
            return;
        }
        for i in 0..left.len() {
            let f = left[i];
            // the author's earlier facts first
            if left
                .iter()
                .any(|g| w.author[g] == w.author[f] && w.facts[g].stamp.hlc < w.facts[f].stamp.hlc)
            {
                continue;
            }
            if !w.deps[f].iter().all(|d| applied.contains(d)) {
                continue;
            }
            left.remove(i);
            applied.push(f);
            acc.push(f);
            go(w, applied, left, acc, out);
            acc.pop();
            applied.pop();
            left.insert(i, f);
        }
    }
    let mut applied = w.prefix[who].clone();
    go(w, &mut applied, &mut free.clone(), &mut Vec::new(), &mut out);
    out
}

/// What a member holds: whether the name answers, the drop its life follows, the creation of its life, its documents. The life is compared by the drop it follows (`follows`), not by `created`:
/// unit P keeps the re-dating record, and with it a `created` that the re-dating invents (case b), so equal `created` is not what P promises. `created` is reported, not compared.
#[derive(Debug)]
struct Held {
    found: bool,
    follows: Option<Stamp>,
    created: Option<Hlc>,
    documents: u64,
}

/// `db_leg`: the floor is the DATABASE's tombstone ALONE (`database_dropped_at`; ruling, Nov 26 3:08 AM: the P legs pin the sweep's split and nothing else; the collection's own tombstone after a database drop
/// is F2's row, `f2_the_database_drops_own_entries_act_as_of_d...`, and the max of the two made the author's burial stamps show here); otherwise the collection's own tombstone.
fn held(m: &Engine, db_leg: bool) -> Held {
    let identity = CollectionId::derive(DB, "orders");
    let follows = if db_leg {
        m.database_dropped_at(DB).unwrap()
    } else {
        m.collection_dropped_at(identity).unwrap()
    };
    match m.get_collection(DB, "orders") {
        Ok(c) => {
            let documents = m.count(&c, WalkScope::Background).unwrap();
            Held { found: true, follows, created: Some(c.created), documents }
        }
        Err(_) => Held { found: false, follows, created: None, documents: 0 },
    }
}

/// The fold's answer for a shape, from the facts of a world: the newest drop is the floor; the life is the first creation above it; its documents are the inserts above that creation.
fn same(got: &Held, want: &Held) -> bool {
    // `created` is NOT compared, for any shape (ruling, Nov 26, 2:59 AM: P removes the JUDGING by `created`, not the difference; after P members may still keep different `created` for concurrent creations)
    got.found == want.found && got.follows == want.follows && got.documents == want.documents
}

fn expected(shape: Shape, w: &World) -> Held {
    let stamp = |f: &str| w.facts[f].stamp;
    let (floor, life) = match shape {
        Shape::A | Shape::B => ("d3", "c4"),
        Shape::B2 | Shape::B2Empty => ("d3b", "c4"),
        Shape::Cc => ("d1", "c2"),
        Shape::CcDropEmpty | Shape::CcDropW | Shape::CcDbDropEmpty | Shape::CcDbDropW => {
            ("d3", "c4")
        }
    };
    // CcDrop*: the insert of life 2 (i2) is below the drop and dead; only the insert above c4 (i5 = W) stands, and none when the life is empty
    let documents: &[&str] = match shape {
        Shape::CcDropEmpty | Shape::CcDbDropEmpty => &[],
        Shape::CcDropW | Shape::CcDbDropW => &["i5"],
        _ => &["i2", "i4", "i5"],
    };
    Held {
        found: true,
        follows: Some(stamp(floor)),
        created: Some(w.facts[life].stamp.hlc),
        documents: documents.iter().filter(|f| w.facts.contains_key(*f)).count() as u64,
    }
}

fn run(shape: Shape, purge: Purge) -> (usize, Vec<String>) {
    let lazy = purge != Purge::Eager;
    let probe = build(shape, lazy);
    let mut total = 0;
    let mut red = Vec::new();
    for who in 0..probe.members.len() {
        for order in orders(&probe, who) {
            // a fresh world for each order, so that the member under test starts where the script left it
            let w = build(shape, lazy);
            let identity = CollectionId::derive(DB, "orders");
            // what the member did not TAKE (the window stopped at it: a purge is pending, or the collection is unknown): the transport offers it again, and only it
            let mut untaken: Vec<&'static str> = Vec::new();
            for f in &order {
                let outcome = window(&w.members[who], std::slice::from_ref(&w.facts[f]));
                if purge == Purge::AtEnd
                    && (outcome.purge_pending > 0 || outcome.unknown_collection > 0)
                {
                    untaken.push(*f);
                }
                // the daemon's purger, run at once: a creation that meets a collection whose data is still waiting to be purged waits for it (collection_purging), and these tests do not start the daemon
                if !lazy {
                    let _ = w.members[who].purge_dropped_collection(identity);
                }
            }
            if lazy {
                let _ = w.members[who].purge_dropped_collection(identity);
            }
            if purge == Purge::AtEnd {
                // each round offers what is still untaken, in the order it was left; a round that takes nothing new ends the offering (at most 5 rounds)
                for _round in 0..5 {
                    if untaken.is_empty() {
                        break;
                    }
                    let mut still = Vec::new();
                    for f in std::mem::take(&mut untaken) {
                        let outcome = window(&w.members[who], std::slice::from_ref(&w.facts[f]));
                        let _ = w.members[who].purge_dropped_collection(identity);
                        if outcome.purge_pending > 0 || outcome.unknown_collection > 0 {
                            still.push(f);
                        }
                    }
                    untaken = still;
                }
            }
            if purge == Purge::AtEndThenOffered {
                for f in &order {
                    window(&w.members[who], std::slice::from_ref(&w.facts[f]));
                    let _ = w.members[who].purge_dropped_collection(identity);
                }
            }
            total += 1;
            let want = expected(shape, &w);
            let got = held(&w.members[who], shape.drop_between().is_some_and(|(_, db)| db));
            if !same(&got, &want) {
                // for the drop-between rows, name the three stamps: c2 < d < c4
                let which = match shape.drop_between() {
                    Some((_, db)) => format!(
                        " [leg={},c2={:?},d={:?},c4={:?}]",
                        if db { "database-drop" } else { "collection-drop" },
                        w.facts["c2"].stamp.hlc,
                        w.facts["d3"].stamp.hlc,
                        w.facts["c4"].stamp.hlc
                    ),
                    None => String::new(),
                };
                red.push(format!(
                    "m{who} takes {order:?}: holds {got:?}, the fold gives {want:?}{which}"
                ));
            }
        }
    }
    (total, red)
}

fn check(shape: Shape) {
    let (total, red) = run(shape, Purge::Eager);
    assert!(total > 0, "no order was tried");
    assert!(
        red.is_empty(),
        "{shape:?}: {} of {total} orders end in a state other than the fold's:\n{}",
        red.len(),
        red.join("\n")
    );
}

#[test]
fn a_later_empty_life_is_held_by_every_member_in_every_order() {
    check(Shape::A);
}

#[test]
fn one_insert_after_the_middle_drop_leaves_every_member_in_the_same_life_in_every_order() {
    check(Shape::B);
}

#[test]
fn a_drop_between_the_two_creations_leaves_every_member_in_the_same_life_in_every_order() {
    check(Shape::B2);
}

#[test]
fn a_drop_between_the_two_creations_of_an_empty_life_leaves_every_member_in_the_same_life_in_every_order()
 {
    check(Shape::B2Empty);
}

/// A GUARD today (u14-p1, Nov 26) on found, the floor and documents: green with the purger running at once; red only under the late purger (see `the_outcome_does_not_depend_on_when_the_purger_runs`).
/// `created` is NOT compared (ruling, Nov 26 2:59 AM). OBSERVED today (u14-p2): a member keeps the `created` of the concurrent creation that reached it FIRST: in the orders where c4 comes before c2 (the
/// three m0 orders, and m2 [c2, i2], whose own creation is c4) the held `created` is c4's (about 25 ms after c2's), not c2's; found, floor and documents are right. After P members may still differ
/// in `created`: P removes the judging by `created`, not the difference.
#[test]
fn two_concurrent_creations_above_one_drop_are_one_life_the_first_in_every_order() {
    check(Shape::Cc);
}

/// RED P ROWS (ruling, Nov 26 2:59 AM; expected RED today, not yet seen in a run): the drop d3 is stamped strictly BETWEEN the two creations (c2 < d3 < c4), all three by DIFFERENT members; d3's author
/// took c2 but not c4, so it is a drop of c2's life, concurrent with c4. Every member, in every order, is compared with the fold on found, the floor (d3) and documents (NOT `created`). Red lines end
/// [leg=collection-drop,c2=..,d=..,c4=..] (the database leg is the two tests further down).
/// EMPTY life (no write anywhere): the fold has the collection standing, empty (c4's life above the floor d3); in some orders the collection is buried instead (the arriving drop is judged below the life
/// it meets, or kills the life it holds, and nothing remakes it).
#[test]
fn two_creations_with_a_drop_between_them_leave_every_member_with_the_same_empty_life_in_every_order()
 {
    check(Shape::CcDropEmpty);
}

/// The variant with a write W above c4 (and a write i2 into c2's life, below the drop): the fold has c4's life with W alone (one document; i2 is dead). The member that buried the life holds no W, the
/// others do; a member that holds c4's life and keeps the drop as history would keep i2 too (two documents).
#[test]
fn two_creations_with_a_drop_between_them_leave_every_member_with_the_same_documents_in_every_order()
 {
    check(Shape::CcDropW);
}

/// SECOND LEG (hardproblems' request, Nov 26 2:58 AM; expected RED today, not yet seen in a run): the same as the two rows above with a DATABASE drop between c2 and c4 instead of the collection drop. It
/// goes through the database sweep's own split (database_tombstone.rs `sweep_collection`: `created <= dropped`), where the collection drop goes through the incarnation test (sync.rs:1872-1876): P cannot
/// delete one reader and leave the other. The floor compared here is the database's tombstone ALONE (Nov 26 3:08 AM). Expected red only on m1's orders. Red lines say `leg=database-drop`.
#[test]
fn two_creations_with_a_database_drop_between_them_leave_every_member_with_the_same_empty_life_in_every_order()
 {
    check(Shape::CcDbDropEmpty);
}

/// The second leg of the write variant: W above c4 (and i2 below the drop), a database drop between the creations.
#[test]
fn two_creations_with_a_database_drop_between_them_leave_every_member_with_the_same_documents_in_every_order()
 {
    check(Shape::CcDbDropW);
}

/// F2's row, red first (ruling, Nov 26 3:08 AM; expected RED today on the author, not yet seen in a run): "the database drop's own entries act as of D". After a database drop EVERY member that holds the
/// collection has its own collection tombstone AT D, the drop's stamp, the author included. Today the author's is at d+2 or d+3 ms, the stamp of its own burial entries (u14-p3). One collection, one
/// database drop, three members that all hold the collection: the author (m0), and two that take the author's drop entries, in three deliveries (the database drop alone; all of the author's drop entries in
/// log order; all of them reversed). A member that never held the collection is NOT asserted (it has no own tombstone: gap 2's residue).
#[test]
#[ignore = "F2: a database drop's burials are stamped at D, not minted"]
fn f2_the_database_drops_own_entries_act_as_of_d_every_members_own_collection_tombstone_is_at_d() {
    let (s, _ds) = with_orders();
    let identity = CollectionId::derive(DB, "orders");
    let holder = || {
        let (m, d) = engine();
        window(&m, &everything(&s));
        assert!(m.get_collection(DB, "orders").is_ok(), "premise: the member holds the collection");
        (m, d)
    };
    let (author, _da) = holder();
    pause();
    assert!(author.drop_database(DB).unwrap());
    let drops: Vec<OplogEntry> = everything(&author)
        .into_iter()
        .filter(|e| e.kind == OpKind::DropCollection && e.stamp.node == author.node_id())
        .collect();
    let database = kimmy_core::database_drop_id(DB);
    let head = drops
        .iter()
        .find(|e| e.collection == database)
        .expect("the database drop's own entry")
        .clone();
    let d = head.stamp;
    let mut report = Vec::new();
    let own = author.collection_dropped_at(identity).unwrap();
    if own != Some(d) {
        report.push(format!("m0 (the author): own collection tombstone {own:?}, D = {d:?}"));
    }
    let deliveries: Vec<(&str, Vec<OplogEntry>)> = vec![
        ("the database drop alone", vec![head.clone()]),
        ("all the author's drop entries, in log order", drops.clone()),
        ("all the author's drop entries, reversed", drops.iter().rev().cloned().collect()),
    ];
    for (what, entries) in deliveries {
        let (m, _d) = holder();
        for e in &entries {
            window(&m, std::slice::from_ref(e));
        }
        let own = m.collection_dropped_at(identity).unwrap();
        if own != Some(d) {
            report.push(format!(
                "a member taking {what}: own collection tombstone {own:?}, D = {d:?}"
            ));
        }
    }
    assert!(
        report.is_empty(),
        "F2, the database drop's own entries act as of D ({} drop entries by the author):\n{}",
        drops.len(),
        report.join("\n")
    );
}

fn check_all(purge: Purge, what: &str) {
    let mut report = Vec::new();
    let mut total = 0;
    for shape in [Shape::A, Shape::B, Shape::B2, Shape::B2Empty, Shape::Cc] {
        let (n, red) = run(shape, purge);
        total += n;
        if !red.is_empty() {
            report.push(format!("{shape:?}: {} of {n} orders:\n{}", red.len(), red.join("\n")));
        }
    }
    assert!(total > 0, "no order was tried");
    assert!(
        report.is_empty(),
        "{what}: orders end in a state other than the fold's:\n{}",
        report.join("\n")
    );
}

/// CORRECTED Nov 26 (hardproblems' P review, A5 and B1, adopted 12:25 PM): the test OFFERS AGAIN WHAT THE MEMBER DID NOT TAKE, as the transport does. While a purge is pending a creation (and, after P7, any fact above the floor) is left UNTAKEN
/// (`PurgePending`), never judged history, and the next round offers it again; the outcome must not depend on when the purger runs. Before the correction nothing was offered again and the test was red in every order where a creation met data
/// not yet purged (u14-p1, Nov 26: A 3/6, B 4/8, B2 34/48, B2Empty 18/28, Cc 6/8 orders; every red was found=false). WITH the re-offer those creation waits are GREEN TODAY: today's build already leaves a creation that meets unpurged data untaken. What can
/// still be red is a fact that is CONSUMED while a purge is pending (judged history, or taken by a remake that never completes): that is P7 and P10's to prevent, and the test is the guard that a remake meeting `CollectionPurging` leaves the entry untaken. WHICH ORDERS ARE
/// RED TODAY is therefore read from the first run, not claimed: the plan's "red, P10" for this row is withdrawn until that run. The blanket re-offer (every entry again) is `a_late_purge_and_the_same_entries_offered_again_end_in_the_folds_state`, below.
#[test]
fn the_outcome_does_not_depend_on_when_the_purger_runs() {
    check_all(Purge::AtEnd, "with the purger running only after the last entry");
}

/// What today's build relies on: a creation that met data not yet purged and waited is applied when it is offered again. With the purger late and every entry offered again in the same order, every
/// member of every case ends in the fold's state. GREEN today (it is why the divergence of case (a) is transient); it must stay green. Without the offering it is the test above.
#[test]
fn a_late_purge_and_the_same_entries_offered_again_end_in_the_folds_state() {
    check_all(Purge::AtEndThenOffered, "with a late purger and the entries offered again");
}

// ---------------------------------------------------------------------------------------------------------------------
// The base options of a remade life (unit N's design rulings, ruling (b): "keyed by id, newest wins", Q6-build-order.md, addendum "unit N's design rulings"). Appended Thu Nov 26 2026 by the tester.
//
// A@1 and B@1.5: creations of one collection on two members under DIFFERENT merge policies (A field, B document; B is later in `policy_key` order, the creation's Hlc and then the policy's text). Then a drop @2 and a
// touch @3 (an insert: a write above the floor makes a plain collection stand, ruling of Nov 25). The drop's author has taken A only, the touch's author has taken A only: so B is, by stamp, below the drop (history)
// and, by arrival, unknown to them, and it can arrive before or after the drop and the touch. Every member, in every order that keeps each author's order and each fact's causal past, must end standing, in the life the
// drop @2 names, holding the touch, and under B's options: policy document, no R, no K. The policy is compared through the public reads (`get_collection`), not through the oplog.
// Order X (A, drop, touch, B) and order Y (A, B, drop, touch) are named below; the third test takes every order.
//
// RED TODAY, expected (the manager's reason, not yet seen in a run): a history creation returns before `adopt_creation_policy`, so in X a standing life keeps A's policy; and where the collection is made by the touch
// alone (the pending write: a member that has no collection and takes the touch first) today's build has no options row to give it, so the order may be red for that reason too. Which orders are red, and why, is
// read from the run. If today's product cannot express the remake at all before P, this block goes under `cfg(any())` like the tie row below, with these expectations.
// Green after unit P: "a standing life adopts the row whenever the row rises, from a history creation too". NOT COMPILED, NEVER RUN: the calls are those of the tests above (Engine::create_collection_with_policy,
// drop_collection, insert, get_collection, collection_dropped_at, count); the premise (A@1 < B@1.5 < drop@2 < touch@3) is asserted. Not committed: it goes into unit P's first commit with the rest of this file.
// ---------------------------------------------------------------------------------------------------------------------

use kimmy_core::MergePolicy;

/// Members: 0 = A's author (field), 1 = B's author (document), 2 = the drop's author (has taken A only), 3 = the touch's author (has taken A only), 4 = a member that has taken nothing.
fn build_options() -> World {
    let mut members = Vec::new();
    let mut dirs = Vec::new();
    for _ in 0..5 {
        let (m, d) = engine();
        members.push(m);
        dirs.push(d);
    }
    let identity = CollectionId::derive(DB, "orders");
    members[0].create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    let a = own(&members[0], OpKind::CreateCollection);
    pause();
    members[1].create_collection_with_policy(DB, "orders", MergePolicy::Document, None).unwrap();
    let b = own(&members[1], OpKind::CreateCollection);
    // the drop's author and the touch's author each hold A's life and nothing of B's
    window(&members[2], std::slice::from_ref(&a));
    window(&members[3], std::slice::from_ref(&a));
    pause();
    assert!(members[2].drop_collection(DB, "orders").unwrap());
    members[2].purge_dropped_collection(identity).unwrap();
    let d = own(&members[2], OpKind::DropCollection);
    pause();
    let held_by_touch_author = members[3].get_collection(DB, "orders").unwrap();
    members[3].insert(&held_by_touch_author, doc! { "_id": 1, "v": "after the drop" }).unwrap();
    let t = own_write(&members[3], a.stamp.hlc);
    assert!(
        a.stamp.hlc < b.stamp.hlc && b.stamp.hlc < d.stamp.hlc && d.stamp.hlc < t.stamp.hlc,
        "premise: A@1 < B@1.5 < drop@2 < touch@3"
    );
    assert_eq!(members[0].get_collection(DB, "orders").unwrap().merge_policy, MergePolicy::Field);
    assert_eq!(
        members[1].get_collection(DB, "orders").unwrap().merge_policy,
        MergePolicy::Document
    );
    let mut facts = BTreeMap::new();
    let mut author = BTreeMap::new();
    let mut deps: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for (name, entry, who, needs) in
        [("a", a, 0, vec![]), ("b", b, 1, vec![]), ("d", d, 2, vec!["a"]), ("t", t, 3, vec!["a"])]
    {
        facts.insert(name, entry);
        author.insert(name, who);
        deps.insert(name, needs);
    }
    let prefix = vec![vec!["a"], vec!["b"], vec!["a", "d"], vec!["a", "t"], vec![]];
    World { members, _dirs: dirs, facts, author, deps, prefix }
}

/// What the test compares: whether the name answers, the drop its life follows, its documents, and the options, read through `get_collection`.
#[derive(Debug, PartialEq)]
struct HeldOptions {
    found: bool,
    follows: Option<Stamp>,
    documents: u64,
    policy: Option<MergePolicy>,
    join_retention_ms: Option<u64>,
    has_loss_limits: bool,
}

fn held_options(m: &Engine) -> HeldOptions {
    let identity = CollectionId::derive(DB, "orders");
    let follows = m.collection_dropped_at(identity).unwrap();
    match m.get_collection(DB, "orders") {
        Ok(c) => HeldOptions {
            found: true,
            follows,
            documents: m.count(&c, WalkScope::Background).unwrap(),
            policy: Some(c.merge_policy),
            join_retention_ms: c.join_retention_ms,
            has_loss_limits: c.loss_limits.is_some(),
        },
        Err(_) => HeldOptions {
            found: false,
            follows,
            documents: 0,
            policy: None,
            join_retention_ms: None,
            has_loss_limits: false,
        },
    }
}

/// The fold's answer: standing, in the life the drop @2 names, holding the touch, under B's options (document: no R, no K).
fn options_expected(w: &World) -> HeldOptions {
    HeldOptions {
        found: true,
        follows: Some(w.facts["d"].stamp),
        documents: 1,
        policy: Some(MergePolicy::Document),
        join_retention_ms: None,
        has_loss_limits: false,
    }
}

/// Member `who` takes `order`, the purger running at once after each fact (as the other tests here): what it holds, and what the fold gives.
fn options_take(who: usize, order: &[&'static str]) -> (HeldOptions, HeldOptions) {
    let w = build_options();
    let identity = CollectionId::derive(DB, "orders");
    for f in order {
        window(&w.members[who], std::slice::from_ref(&w.facts[f]));
        let _ = w.members[who].purge_dropped_collection(identity);
    }
    (held_options(&w.members[who]), options_expected(&w))
}

/// The two halves a red here can be waiting on (ruling, Nov 26). REMAKE: the collection does not answer at all (the touch above the drop did not remake it: unit P's remake). OPTIONS: it stands, but under
/// the wrong options (the adoption from a history creation: the options row). A red says which half it waits on; the options half is only reached once the remake is built.
const REMAKE_PREMISE: &str =
    "premise: the touch above the drop remade the collection (unit P's remake); not built yet";

fn options_half(
    who: usize,
    order: &[&'static str],
    got: &HeldOptions,
    want: &HeldOptions,
) -> Option<(bool, String)> {
    if !got.found && want.found {
        Some((
            true,
            format!(
                "[REMAKE] {REMAKE_PREMISE}: m{who} takes {order:?}: the name does not answer ({got:?})"
            ),
        ))
    } else if got != want {
        Some((
            false,
            format!("[OPTIONS] m{who} takes {order:?}: holds {got:?}, the fold gives {want:?}"),
        ))
    } else {
        None
    }
}

/// Order X: A, the drop, the touch, then B (B arrives below a tombstone: history).
#[test]
fn x_a_history_creation_that_arrives_after_the_drop_and_the_touch_gives_the_standing_life_its_options()
 {
    let order = ["a", "d", "t", "b"];
    let (got, want) = options_take(4, &order);
    assert!(
        got.found || !want.found,
        "{REMAKE_PREMISE}: m4 takes {order:?}: the name does not answer ({got:?})"
    );
    assert_eq!(got, want, "options: m4 takes {order:?}");
}

/// Order Y: A, B, the drop, the touch (B arrives while the collection is live, and wins on policy_key before the drop).
#[test]
fn y_the_later_creation_taken_before_the_drop_gives_the_remade_life_its_options() {
    let order = ["a", "b", "d", "t"];
    let (got, want) = options_take(4, &order);
    assert!(
        got.found || !want.found,
        "{REMAKE_PREMISE}: m4 takes {order:?}: the name does not answer ({got:?})"
    );
    assert_eq!(got, want, "options: m4 takes {order:?}");
}

/// Every member (the four authors, each taking the facts it lacks, and the member that has nothing), every order that keeps each author's order and each fact's causal past.
#[test]
fn every_member_in_every_order_ends_standing_with_the_newest_creations_options() {
    let probe = build_options();
    let mut total = 0;
    let mut red = Vec::new();
    for who in 0..probe.members.len() {
        for order in orders(&probe, who) {
            total += 1;
            // a fresh world for each order: an author starts from what it applied while the world was built (its prefix)
            let w = build_options();
            let identity = CollectionId::derive(DB, "orders");
            for f in &order {
                window(&w.members[who], std::slice::from_ref(&w.facts[f]));
                let _ = w.members[who].purge_dropped_collection(identity);
            }
            let (got, want) = (held_options(&w.members[who]), options_expected(&w));
            if let Some(half) = options_half(who, &order, &got, &want) {
                red.push(half);
            }
        }
    }
    assert!(total > 0, "no order was tried");
    let waiting_on_the_remake = red.iter().filter(|(remake, _)| *remake).count();
    assert!(
        red.is_empty(),
        "{} of {total} orders end in options other than B's: {waiting_on_the_remake} are waiting on the REMAKE (premise), {} on the OPTIONS adoption:\n{}",
        red.len(),
        red.len() - waiting_on_the_remake,
        red.iter().map(|(_, m)| m.as_str()).collect::<Vec<_>>().join("\n")
    );
}

// =====================================================================================================================================================================================
// THE RULED P ROWS OF THU NOV 26 2026 (build-order addenda from 12:08 AM CST on). Appended by the tester at the manager's order, 12:59 AM CST. Each row names its source addendum and its expected red reason. NOT COMPILED, NEVER RUN.
// The rows that need N's options row or P's API are `#[cfg(any())]` with their expectations and ASSUMED names; the others are compile-targeted and use only calls the fixtures of this crate's tests already make (ASSUMPTIONS marked).
// =====================================================================================================================================================================================
mod ruled_rows_of_nov_26 {
    use std::time::Duration;

    use super::*;
    use kimmy_core::{CollectionBind, DocId, MergePolicy};

    use crate::{SnapshotProgress, physical_now_ms};

    const FIRST: &str = "orders.__stg.s2";

    fn try_window(b: &Engine, entries: &[OplogEntry]) -> crate::Result<SyncOutcome> {
        let mut theirs = VersionVector::default();
        for entry in entries {
            theirs.observe(entry.stamp);
        }
        let mine = b.witnessed_vector()?;
        let mut outcome = SyncOutcome::default();
        b.apply_peer_window_into(
            &theirs,
            Some(&mine),
            entries,
            PeerWindow {
                scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
                exhausted: true,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )?;
        Ok(outcome)
    }

    fn orders_id() -> CollectionId {
        CollectionId::derive(DB, "orders")
    }

    fn creation_of(a: &Engine, name: &str) -> OplogEntry {
        everything(a)
            .into_iter()
            .rev()
            .find(|e| {
                e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, name)
            })
            .expect("the creation is in the author's oplog")
    }

    /// An entry of `a`'s oplog, re-stamped by hand to `wall_ms` as `by` would have minted it (the fixtures' own device: `join_tests::doc_entry`, `shadow_burial_tests::drop_of`).
    fn stamped(mut entry: OplogEntry, wall_ms: u64, by: &Engine) -> OplogEntry {
        entry.stamp = Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch());
        entry
    }

    fn drop_of(a: &Engine, wall_ms: u64) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
            kind: OpKind::DropCollection,
            collection: orders_id(),
            doc_id: None,
            body: Some(
                bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                    .expect("the drop encodes"),
            ),
            meta: None,
        }
    }

    /// The tombstone of `ids` set by hand far below any sender a test can build (`join_tests` :2456-2464).
    fn lower_the_tombstones(m: &Engine, by: &Engine, ids: &[CollectionId]) -> Stamp {
        let low = Stamp::new(Hlc::new(1, 0), by.node_id(), by.epoch());
        for id in ids {
            let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
            txn.open_table(crate::tables::COLLECTIONS_DROPPED)
                .expect("the table opens")
                .insert(id.0, crate::codec::oplog_key(&low).as_slice())
                .expect("the tombstone is lowered");
            txn.commit().expect("the commit lands");
        }
        low
    }

    fn apply_the_pages(
        m: &Engine,
        sender: &Engine,
    ) -> (SnapshotProgress, Vec<crate::SnapshotApplied>) {
        let mut progress = SnapshotProgress::whole_database();
        let mut applied = Vec::new();
        for _ in 0..50 {
            if progress.is_complete() {
                break;
            }
            let page = sender
                .snapshot_page(progress.after().cloned(), progress.scope())
                .expect("the sender's page");
            let done = m
                .apply_snapshot_page(sender.node_id(), &mut progress, &page)
                .expect("the page applies");
            let waits = done.purge_pending.is_some();
            applied.push(done);
            if waits {
                break;
            }
        }
        (progress, applied)
    }

    // ---- ROW 1: no remake for a name with SWAP HISTORY -------------------------------------------------------------------------------------

    /// The swap of `shadow_burial_tests`: one stage, its binding stamped above it.
    struct Swap {
        first: OplogEntry,
        bind: OplogEntry,
        author: Engine,
        _dir: tempfile::TempDir,
    }

    fn swap() -> Swap {
        let (a, dir) = engine();
        let now = physical_now_ms();
        a.create_stage(DB, FIRST, MergePolicy::Document, Hlc::new(now + 120_000, 0))
            .expect("the stage is created");
        let first = creation_of(&a, FIRST);
        let bind = OplogEntry {
            stamp: Stamp::new(Hlc::new(now + 20_000, 0), a.node_id(), a.epoch()),
            kind: OpKind::BindCollection,
            collection: orders_id(),
            doc_id: None,
            body: Some(
                bson::serialize_to_vec(&CollectionBind {
                    db: DB.into(),
                    name: "orders".into(),
                    physical: FIRST.into(),
                })
                .expect("the binding encodes"),
            ),
            meta: None,
        };
        Swap { first, bind, author: a, _dir: dir }
    }

    /// A member that held `orders` with a document and the stage, then took the binding: the identity is buried at the binding's stamp and its rows are held for the purger.
    fn a_member_that_flipped(sw: &Swap) -> (Engine, tempfile::TempDir) {
        let (m, d) = engine();
        let orders = m.create_collection(DB, "orders").expect("the identity");
        m.insert(&orders, doc! { "_id": 1, "v": "old" }).expect("a document");
        window(&m, std::slice::from_ref(&sw.first));
        m.set_purge_deferral(Duration::from_secs(60));
        window(&m, std::slice::from_ref(&sw.bind));
        assert_eq!(
            m.resolve(DB, "orders").expect("the name resolves").physical,
            FIRST,
            "premise: the binding is visible"
        );
        assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
        assert_eq!(
            m.collection_dropped_at(orders_id()).unwrap(),
            Some(sw.bind.stamp),
            "premise: the burial mark is the binding's stamp"
        );
        assert!(m.purge_pending(orders_id()).unwrap(), "premise: the identity's rows are held");
        (m, d)
    }

    /// Source: build-order addendum 12:22 AM, "RULED, a condition on P: the remake does not fire for a name with swap history. For such a name the tombstone is the register's burial mark, and a write above it is a swap's late write. Add one gate in P and
    /// one P row, by entry and by page." BY ENTRY: a document of the identity, stamped ABOVE the burial mark, from a member that has not heard of the swap, reaches a member that flipped.
    /// EXPECTED TODAY: GREEN, a GUARD (today "gone, and a tombstone says so" is history, sync.rs:2234-2238; the remake does not exist). It goes red when unit P's remake fires for it: the identity is made again beside the stage, or the entry stops at
    /// `CollectionPurging` under the hold. WANTED after P (the same): the entry is taken and counted superseded, nothing is made, nothing under the held id changes, the burial mark stands, and the member is not stopped.
    /// Mutant: P's remake without the swap-history gate.
    #[test]
    fn row1a_a_late_write_above_a_swaps_burial_mark_does_not_remake_the_identity_by_entry() {
        let sw = swap();
        let (m, _d) = a_member_that_flipped(&sw);
        let rows = m.rows_under(orders_id()).unwrap();
        // the late write: made by a member that never heard of the swap, stamped above the binding
        let (x, _dx) = engine();
        let xo = x.create_collection(DB, "orders").expect("x's plain life");
        x.insert(&xo, doc! { "_id": 7, "v": "late" }).expect("x writes");
        let late = stamped(
            everything(&x)
                .into_iter()
                .rev()
                .find(|e| e.kind == OpKind::Insert && e.doc_id == Some(DocId::Int64(7)))
                .expect("x's insert"),
            sw.bind.stamp.hlc.wall_ms + 5_000,
            &x,
        );
        assert!(late.stamp.hlc > sw.bind.stamp.hlc, "premise: the write is above the burial mark");

        let outcome = try_window(&m, std::slice::from_ref(&late))
            .expect("the entry does not stop the member (no CollectionPurging, no error)");

        assert_eq!(
            outcome.unknown_collection, 0,
            "the member is not stopped by the late write ({outcome:?})"
        );
        assert!(m.get_collection(DB, "orders").is_err(), "no identity is made beside the stage");
        assert_eq!(
            m.resolve(DB, "orders").expect("the name resolves").physical,
            FIRST,
            "the name still answers from the stage"
        );
        assert_eq!(m.rows_under(orders_id()).unwrap(), rows, "nothing under the held id changed");
        assert_eq!(
            m.collection_dropped_at(orders_id()).unwrap(),
            Some(sw.bind.stamp),
            "the burial mark stands"
        );
    }

    /// The same BY PAGE: a sender whose plain life of `orders` holds documents ABOVE the burial mark (the mark is lowered by hand so that a sender built now is above it, as `join_tests` does). Same source and same expectation as 1a: today GREEN (a GUARD),
    /// and the same as the swapped identity's document half in unit S's S3: the page completes, waits on no purge, makes no identity, counts the documents superseded, and leaves the held rows alone.
    #[test]
    fn row1b_a_page_of_documents_above_a_swaps_burial_mark_does_not_remake_the_identity() {
        let sw = swap();
        let (m, _d) = a_member_that_flipped(&sw);
        let rows = m.rows_under(orders_id()).unwrap();
        lower_the_tombstones(&m, &sw.author, &[orders_id()]);
        let (a, _da) = engine();
        let ao = a.create_collection(DB, "orders").expect("the sender's life");
        for id in [1, 2, 3] {
            a.insert(&ao, doc! { "_id": id, "v": "new" }).expect("the sender's document");
        }

        let (progress, applied) = apply_the_pages(&m, &a);

        assert!(progress.is_complete(), "the page completes");
        assert!(applied.iter().all(|done| done.purge_pending.is_none()), "and waits on no purge");
        assert!(m.get_collection(DB, "orders").is_err(), "no identity is made beside the stage");
        assert!(
            applied.iter().map(|done| done.superseded).sum::<usize>() >= 3,
            "the documents are counted superseded"
        );
        assert_eq!(m.rows_under(orders_id()).unwrap(), rows, "the held rows are untouched");
    }

    // ---- ROW 2: reader 1 -------------------------------------------------------------------------------------------------------------------

    /// The five facts of the coder's case (`FINDING-redating-sweep-trace.md`, Addendum, item 2), hand-stamped: create @10 (A), drop @20 (A), create again @25 (A), drop @30 (A), insert W @35 (B, which had not heard of the drops).
    struct Redating {
        c1: OplogEntry,
        d1: OplogEntry,
        c2: OplogEntry,
        d3: OplogEntry,
        w: OplogEntry,
        base: u64,
        _keep: Vec<(Engine, tempfile::TempDir)>,
    }

    fn redating() -> Redating {
        let base = physical_now_ms() + 10_000;
        let (a, da) = engine();
        a.create_collection(DB, "orders").expect("A creates");
        let c1 = stamped(creation_of(&a, "orders"), base + 10, &a);
        assert!(a.drop_collection(DB, "orders").unwrap());
        a.create_collection(DB, "orders").expect("A creates again");
        let c2 = stamped(creation_of(&a, "orders"), base + 25, &a);
        let d1 = drop_of(&a, base + 20);
        let d3 = drop_of(&a, base + 30);
        let (b, db) = engine();
        let bo = b.create_collection(DB, "orders").expect("B's life");
        b.insert(&bo, doc! { "_id": 5, "v": "W" }).expect("B writes W");
        let w = stamped(
            everything(&b)
                .into_iter()
                .rev()
                .find(|e| e.kind == OpKind::Insert && e.doc_id == Some(DocId::Int64(5)))
                .expect("B's insert"),
            base + 35,
            &b,
        );
        Redating { c1, d1, c2, d3, w, base, _keep: vec![(a, da), (b, db)] }
    }

    fn takes(m: &Engine, facts: &[&OplogEntry]) {
        for fact in facts {
            window(m, std::slice::from_ref(*fact));
        }
    }

    /// Source: build-order addendum 12:26 AM, "Reader 1, RULED: no fix before P; one test in P. The burial goes through `drop_keeping_later_writes`, which keeps everything above the floor. The two verdicts differ only in WHEN the drop applies. The P test: a
    /// receiver with a re-dated life takes the same facts' page from senders with `from` 10 and with `from` 25. It ends with the same documents and floor from both, holding a write above the floor that neither sender's coverage names."
    /// The senders: M1 takes [c1, w, d1, c2, d3] (marker `from` = 10), M2 takes [c1, d1, c2, w, d3] (`from` = 25); both hold W, created 31, floor 30 (the coder's case; the premises are asserted). The receiver X took [c1, w, d1] (a re-dated life, `from` 10)
    /// and a write W2 @40 of a third author that neither sender has heard of. X is built twice, takes M1's pages once and M2's once.
    /// EXPECTED TODAY: UNCONFIRMED, probably GREEN. The coder showed that `arriving_life_ends_the_standing_one` (sync.rs:1896-1913) answers differently for the two `from`s, and did NOT construct a case where X loses a write ("I only note that the input differs"). If the
    /// burial of X's standing life by the second verdict loses W2, this is red today and the failure names it. It is the test that pins the ruling "no fix before P", and it must stay green through P and F2.
    /// ASSUMPTION: `CollectionMeta::redated` is `Option<Redated { from, .. }>` (meta.rs:120-146, read).
    #[test]
    fn row2_a_receiver_with_a_redated_life_ends_the_same_from_two_senders_whose_markers_differ_and_keeps_its_own_later_write()
     {
        let f = redating();
        let from_of = |m: &Engine| {
            m.get_collection(DB, "orders").ok().and_then(|c| c.redated).map(|r| r.from)
        };
        // the two senders
        let (m1, _d1) = engine();
        takes(&m1, &[&f.c1, &f.w, &f.d1, &f.c2, &f.d3]);
        let (m2, _d2) = engine();
        takes(&m2, &[&f.c1, &f.d1, &f.c2, &f.w, &f.d3]);
        assert_eq!(
            from_of(&m1),
            Some(Hlc::new(f.base + 10, 0)),
            "premise: M1's marker `from` is 10"
        );
        assert_eq!(
            from_of(&m2),
            Some(Hlc::new(f.base + 25, 0)),
            "premise: M2's marker `from` is 25"
        );
        for m in [&m1, &m2] {
            assert_eq!(
                m.count(&m.get_collection(DB, "orders").unwrap(), WalkScope::Background).unwrap(),
                1,
                "premise: each sender holds W and nothing else"
            );
        }
        // a write neither sender has heard of, from a third author
        let (z, _dz) = engine();
        let zo = z.create_collection(DB, "orders").expect("Z's life");
        z.insert(&zo, doc! { "_id": 9, "v": "W2" }).expect("Z writes W2");
        let w2 = stamped(
            everything(&z)
                .into_iter()
                .rev()
                .find(|e| e.kind == OpKind::Insert && e.doc_id == Some(DocId::Int64(9)))
                .expect("Z's insert"),
            f.base + 40,
            &z,
        );
        let receiver = || {
            let (x, d) = engine();
            takes(&x, &[&f.c1, &f.w, &f.d1]);
            window(&x, std::slice::from_ref(&w2));
            assert_eq!(
                from_of(&x),
                Some(Hlc::new(f.base + 10, 0)),
                "premise: X's life is re-dated, `from` 10"
            );
            (x, d)
        };
        let end = |x: &Engine| {
            let c = x.get_collection(DB, "orders").ok();
            (
                c.as_ref().map(|c| x.count(c, WalkScope::Background).unwrap()),
                c.as_ref().and_then(|c| x.get(c, &DocId::Int64(9)).unwrap()).is_some(),
                c.as_ref().and_then(|c| x.get(c, &DocId::Int64(5)).unwrap()).is_some(),
                x.collection_dropped_at(orders_id()).unwrap().map(|s| s.hlc),
            )
        };

        let (x1, _dx1) = receiver();
        apply_the_pages(&x1, &m1);
        let (x2, _dx2) = receiver();
        apply_the_pages(&x2, &m2);

        let (e1, e2) = (end(&x1), end(&x2));
        assert_eq!(
            e1, e2,
            "the same documents and floor from both senders: (count, holds W2, holds W, floor)"
        );
        assert_eq!(
            e1,
            (Some(2), true, true, Some(Hlc::new(f.base + 30, 0))),
            "X holds W and its own write W2, above floor 30"
        );
    }

    // ---- ROWS 3-5: the options row (N) and P's remake -----------------------------------------------------------------------------------------

    // ---- ROW 6: the tie where a standing life refuses a second creation today -----------------------------------------------------------------

    /// Source: build-order addendum 12:22 AM, "The tie rule for (b) stands, with hardproblems' condition: a standing life follows the same order where it now refuses a second creation (the 'two Rs' refusals in sync.rs and at snapshot.rs:3567-3584). That change is
    /// P's, with the adoption change", and the 12:46 AM comment owed in piece 1 (the tie encoding is JSON; deliberate). The tie: two creations of one `field` collection at the SAME Hlc (the same policy text) by two members, with a DIFFERENT R: today a member
    /// that holds one refuses the other (`held.merge_policy_at == Some(entry.stamp.hlc)` and R differs: sync.rs, the "carries another join retention ... a creation cannot have two Rs" arm: `DdlOutcome::Refused`), so the order decides. The second creation is the first's
    /// entry re-stamped by hand to the same Hlc and the other node (two real members cannot stamp one Hlc). WANTED: every member ends with the SAME R in either order, and no refusal is counted; which R is the greater canonical encoding of the whole options row, which is
    /// N's row: here only that the two orders agree and that the R is one of the two (the encoding itself is asserted in `needs_the_names_table_tie`).
    /// EXPECTED RED TODAY: the member that took A first holds A's R and refuses B (ddl_refused 1); the member that took B first holds B's R: the two orders end with different R, and a refusal is counted.
    /// Mutant: arrival order wins (the first held); the smaller encoding wins; only the page route changed (the entries order stays red).
    /// ASSUMPTION: a hand-re-stamped creation entry is applied as a creation of its stamp (the entry carries its options in `meta`/`body`, not its stamp: as `doc_entry`'s restamping assumes for a document).
    #[test]
    fn row6_two_creations_at_one_hlc_with_different_joins_end_with_the_same_one_in_either_order() {
        const HOUR_MS: u64 = 3_600_000;
        let (a, _da) = engine();
        a.set_default_join_retention(Duration::from_millis(HOUR_MS));
        a.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).expect("A creates");
        let ea = creation_of(&a, "orders");
        let (b, _db) = engine();
        b.set_default_join_retention(Duration::from_millis(48 * HOUR_MS));
        b.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).expect("B creates");
        let eb = OplogEntry {
            stamp: Stamp::new(ea.stamp.hlc, b.node_id(), b.epoch()),
            ..creation_of(&b, "orders")
        };
        assert_eq!(
            ea.stamp.hlc, eb.stamp.hlc,
            "premise: equal policy_key (one Hlc, one policy text)"
        );
        assert_ne!(ea.stamp, eb.stamp, "premise: two creations, by two members");

        let mut ends = Vec::new();
        let mut refused = 0;
        for order in [[&ea, &eb], [&eb, &ea]] {
            let (z, _dz) = engine();
            for entry in order {
                refused += window(&z, std::slice::from_ref(entry)).ddl_refused;
            }
            ends.push(
                z.get_collection(DB, "orders").expect("the collection stands").join_retention_ms,
            );
        }

        assert_eq!(
            refused, 0,
            "a tie is settled, not refused: no member counts the 'two Rs' refusal"
        );
        assert_eq!(ends[0], ends[1], "both orders end with the same join retention");
        assert!(
            ends[0] == Some(HOUR_MS) || ends[0] == Some(48 * HOUR_MS),
            "and it is one of the two creations' ({:?})",
            ends[0]
        );
    }
}
