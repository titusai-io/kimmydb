//! Tests of the incarnation floor as an operation on a state ([`MergeState::drop_at_or_below`]).
//!
//! The operation is not in the reference model, so its bound is here: it is idempotent, it leaves
//! the version vector alone, what it leaves is a state `decode` accepts, and filtering the inputs of
//! a join first changes nothing of what the floor leaves of the result (where no delete at or below
//! the floor has a say, which is the one case where it is meant to).

use std::collections::BTreeSet;

use super::prop_tests::{nested_ord_pools, nested_pools, opts, ord_pools, pools, run_counters};
use super::tests::{fl, i, p, wid};
use super::*;
use crate::hlc::Hlc;

fn writer(c: char, o: &JoinOptions) -> Writer {
    Writer::new(wid(c, 0), o.clone())
}

/// Every stamp a state names by a cell, a life or a delete: the floors worth trying on it.
fn stamps(st: &MergeState) -> BTreeSet<Hlc> {
    st.write_stamps().map(|s| s.hlc).collect()
}

/// Whether, in what two states hold together, a life at or below the floor that holds no cell above it could
/// win the concurrent-first-insert rule (the newest whole document wins) over a life that holds one. The join
/// decides that rule with the history life present; a member that filtered first never has it compete, so the
/// two orders can differ by the write after the floor the loser carried, which the join keeps only as a loss.
fn a_history_life_could_beat_a_live_one(a: &MergeState, b: &MergeState, floor: Hlc) -> bool {
    let lives: BTreeSet<MStamp> = a.lives.keys().chain(b.lives.keys()).copied().collect();
    let above: BTreeSet<MStamp> = [a, b]
        .into_iter()
        .flat_map(|s| {
            s.regs
                .iter()
                .filter(|(_, r)| r.stamp.hlc > floor)
                .map(|(k, _)| k.0)
                .chain(s.slots.iter().filter(|(_, v)| v.stamp.hlc > floor).map(|(k, _)| k.0))
        })
        .collect();
    lives.iter().any(|x| {
        x.hlc <= floor
            && !above.contains(x)
            && above.iter().any(|y| lives.contains(y) && y != x && x > y)
    })
}

fn all_pools(o: &JoinOptions) -> Vec<Vec<MergeState>> {
    let mut sets = pools(12, 24, o);
    sets.extend(nested_pools(12, 26, o));
    sets.extend(ord_pools(12, 24, o));
    sets.extend(nested_ord_pools(12, 26, o));
    sets.extend((1..=12).map(|s| run_counters(s, 30, o)));
    sets
}

#[test]
fn a_register_at_or_below_the_floor_goes_and_one_above_it_stays_in_its_life() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    a.write_set(&p("y"), i(2), 0).unwrap();
    let mut st = a.state.clone();
    assert!(st.drop_at_or_below(first.hlc), "the insert is at the floor: it goes");
    assert_eq!(st.regs.len(), 1, "only the register of the write above the floor is left");
    assert!(st.regs.keys().all(|(_, path)| *path == p("y")));
    assert_eq!(st.lives.len(), 1, "the life the write after the floor reached stays");
    assert_eq!(st.visible(&o), Some([(p("y"), i(2))].into_iter().collect()));
    assert_eq!(st.vv, a.state.vv, "the version vector is not touched");
}

#[test]
fn a_write_at_the_floor_is_the_drops_and_one_a_tick_above_is_kept() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let at = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    let mut tie = a.state.clone();
    assert!(tie.drop_at_or_below(at.hlc));
    assert!(tie.is_bare(), "a tie goes to the drop (ADR-236): nothing is left");
    let mut below = a.state.clone();
    assert!(!below.drop_at_or_below(Hlc::new(at.hlc.wall_ms.saturating_sub(1), 0)));
    assert_eq!(below, a.state, "a floor below every stamp changes nothing");
}

#[test]
fn a_counter_slot_stamped_above_the_floor_is_kept_whole() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    a.write_inc(&p("x"), 2, 0).unwrap();
    let slots = a.state.slots.clone();
    assert!(!slots.is_empty(), "premise: the increment made a slot");
    let mut st = a.state.clone();
    st.drop_at_or_below(first.hlc);
    assert_eq!(st.slots, slots, "a slot is one cell, judged by its last increment, and kept whole");
}

#[test]
fn a_field_a_write_after_the_floor_added_keeps_its_place_in_a_life_made_before_it() {
    // z is written before y, so the document reads z, y; by path alone it would read y, z. The two
    // writes are after the floor, in a life made at it, and the order state they made is theirs.
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    a.write_set(&p("z"), i(2), 0).unwrap();
    a.write_set(&p("y"), i(3), 0).unwrap();
    let names = |st: &MergeState| -> Vec<String> {
        st.ordered_document(&o).expect("a life is alive").into_iter().map(|(k, _)| k).collect()
    };
    assert_eq!(names(&a.state), ["x", "z", "y"], "premise: the order is the order of the writes");
    let mut st = a.state.clone();
    st.drop_at_or_below(first.hlc);
    assert_eq!(names(&st), ["z", "y"], "the fields after the floor are there, in their place");
    assert!(st.check_decoded().is_ok());
}

#[test]
fn a_life_held_dead_by_a_delete_before_the_floor_and_the_same_life_alive_meet_alive() {
    // A made X and deleted it (D). B saw only the insert and wrote y a long way after D without seeing
    // it. The floor is D's stamp. The early learner joined D with B's write before it knew the floor,
    // so it holds the life dead; the late learner only ever saw the thinned states, so it holds it alive.
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    a.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let insert_only = a.state.clone();
    let deleted = a.write_delete(0).unwrap().stamp;
    let floor = deleted.hlc;
    let mut b = writer('B', &o);
    b.receive(&insert_only);
    b.write_set(&p("y"), i(2), 1_000).unwrap();
    assert!(
        b.state.regs.values().any(|r| r.stamp.hlc > floor),
        "premise: B's write is stamped after the floor"
    );
    let thin = |s: &MergeState| {
        let mut t = s.clone();
        t.drop_at_or_below(floor);
        t
    };
    let mut early = join(&a.state, &b.state, &o).0;
    assert_eq!(
        early.visible(&o),
        None,
        "premise: joined before the floor, the delete kills the life"
    );
    early.drop_at_or_below(floor);
    let late = join(&thin(&a.state), &thin(&b.state), &o).0;
    assert_eq!(late.visible(&o), Some([(p("y"), i(2))].into_iter().collect()), "the life stands");
    let met = thin(&join(&early, &late, &o).0);
    assert_eq!(
        met.visible(&o),
        late.visible(&o),
        "they meet on the life alive, with the write after"
    );
    assert_eq!(thin(&join(&met, &early, &o).0), met, "and the met state absorbs the early one");
    assert_eq!(thin(&join(&met, &late, &o).0), met, "and the late one");
}

#[test]
fn a_counter_slot_at_the_floor_goes() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    a.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let inc = a.write_inc(&p("x"), 2, 0).unwrap().stamp;
    let mut st = a.state.clone();
    st.drop_at_or_below(inc.hlc);
    assert!(st.slots.is_empty(), "a slot at the floor is the drop's, judged by its last increment");
}

#[test]
fn a_delete_at_or_below_the_floor_does_not_survive_it() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    a.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let deleted = a.write_delete(0).unwrap().stamp;
    assert_eq!(a.state.deletes.len(), 1, "premise: the delete is in the state");
    let mut st = a.state.clone();
    assert!(st.drop_at_or_below(deleted.hlc));
    assert!(st.deletes.is_empty(), "the delete of the life before the drop is history");
    assert!(st.is_bare(), "and the dead life it ended goes with it");
    assert_eq!(st.vv, a.state.vv);
}

#[test]
fn a_life_after_the_floor_that_names_a_delete_before_it_is_a_state_decode_accepts() {
    // B created its life after it had seen A's delete, so its life lists the delete in `seen`. The
    // floor then drops the delete and keeps the life: the list names a stamp no delete is held for.
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    a.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let deleted = a.write_delete(0).unwrap().stamp;
    let mut b = writer('B', &o);
    b.receive(&a.state);
    b.insert(&fl(&[("z", i(9))]), 0).unwrap();
    assert!(
        b.state.lives.values().any(|life| life.seen.contains(&deleted)),
        "premise: a life of B's names A's delete"
    );
    let mut st = b.state.clone();
    assert!(st.drop_at_or_below(deleted.hlc));
    assert!(st.deletes.is_empty(), "premise: the delete is gone");
    assert!(
        st.lives.values().any(|life| life.seen.contains(&deleted)),
        "and B's life still names it"
    );
    assert!(st.check_decoded().is_ok(), "which decode accepts");
    assert_eq!(MergeState::decode_compact(&st.encode_compact()).as_ref(), Ok(&st));
    assert_eq!(st.visible(&o), Some([(p("z"), i(9))].into_iter().collect()));
}

#[test]
fn a_delete_before_the_floor_does_not_kill_a_life_made_after_it_that_never_saw_it() {
    // The reason the delete goes with the cells: a peer's old state arrives with the delete of the
    // life before the drop, and the member's life made after the drop never saw it.
    let o = opts(1_000_000_000);
    let mut old = writer('A', &o);
    old.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let deleted = old.write_delete(0).unwrap().stamp;
    let mut fresh = writer('B', &o);
    // B's clock is above the delete: it is a life of the new incarnation.
    fresh.state.vv.insert(deleted.writer, deleted.hlc);
    fresh.insert(&fl(&[("z", i(9))]), 0).unwrap();
    let after_own_write = fresh.state.clone();
    let mut arriving = old.state.clone();
    arriving.drop_at_or_below(deleted.hlc);
    fresh.receive(&arriving);
    assert_eq!(
        fresh.visible(),
        Some([(p("z"), i(9))].into_iter().collect()),
        "the life after the drop is alive"
    );
    // Without the filter the same arrival kills it: the premise this test stands on.
    let mut unfiltered = writer('B', &o);
    unfiltered.state = after_own_write;
    unfiltered.receive(&old.state);
    assert_eq!(unfiltered.visible(), None, "premise: the unfiltered delete kills the new life");
}

#[test]
fn the_floor_is_idempotent_leaves_the_version_vector_and_a_state_decode_accepts() {
    for ret in [1_000_000_000, 6] {
        let o = opts(ret);
        for pool in all_pools(&o) {
            for st in &pool {
                for floor in stamps(st) {
                    let mut once = st.clone();
                    once.drop_at_or_below(floor);
                    assert_eq!(once.vv, st.vv, "the version vector is not touched");
                    assert!(once.check_decoded().is_ok(), "decode accepts what the floor leaves");
                    assert_eq!(
                        MergeState::decode_compact(&once.encode_compact()).as_ref(),
                        Ok(&once)
                    );
                    let mut twice = once.clone();
                    assert!(!twice.drop_at_or_below(floor), "a second pass drops nothing");
                    assert_eq!(twice, once, "idempotent");
                    assert!(
                        once.regs
                            .iter()
                            .all(|((_, path), r)| r.stamp.hlc > floor || *path == p("_id")),
                        "nothing at or below the floor is left but the identity of a life that stays"
                    );
                    assert!(
                        st.regs.keys().all(|k| {
                            k.1 != p("_id")
                                || !once.lives.contains_key(&k.0)
                                || once.regs.contains_key(k)
                        }),
                        "every life that stays keeps its `_id`"
                    );
                    assert!(once.slots.values().all(|s| s.stamp.hlc > floor));
                    assert!(once.deletes.keys().all(|d| d.hlc > floor));
                    assert!(
                        once.ords.iter().filter(|((_, at), _)| *at != OrdAt::Field(p("_id"))).all(
                            |(_, x)| {
                                x.firsts.values().all(|c| c.0.hlc > floor)
                                    && x.last.is_none_or(|c| c.0.hlc > floor)
                                    && x.floor.is_none_or(|f| f.stamp().hlc > floor)
                                    && x.lvl.is_none_or(|l| l.hlc > floor)
                            }
                        ),
                        "the order state at or below the floor goes"
                    );
                    assert!(
                        once.losses.keys().all(|k| k.stamp.hlc > floor),
                        "a loss record of a write at or below the floor goes"
                    );
                    assert!(once.dead.iter().all(|l| once.lives.contains_key(l)));
                }
            }
        }
    }
}

#[test]
fn filtering_the_inputs_of_a_join_first_changes_nothing_the_floor_leaves_of_the_result() {
    // Where no delete at or below the floor is in either input (with one, the filtered inputs do not
    // kill a life that the unfiltered join does, which is what the floor is for), and no life at or below
    // the floor could win the first-insert rule over a life with a write after it (the next test).
    let mut compared = 0usize;
    for ret in [1_000_000_000, 6] {
        let o = opts(ret);
        for pool in all_pools(&o) {
            for (k, a) in pool.iter().enumerate() {
                let b = &pool[(k * 7 + 3) % pool.len()];
                for floor in stamps(a).into_iter().chain(stamps(b)).step_by(3) {
                    if a.deletes.keys().chain(b.deletes.keys()).any(|d| d.hlc <= floor)
                        || a_history_life_could_beat_a_live_one(a, b, floor)
                    {
                        continue;
                    }
                    let (mut joined, _) = join(a, b, &o);
                    joined.drop_at_or_below(floor);
                    let (mut fa, mut fb) = (a.clone(), b.clone());
                    fa.drop_at_or_below(floor);
                    fb.drop_at_or_below(floor);
                    let (mut first, _) = join(&fa, &fb, &o);
                    first.drop_at_or_below(floor);
                    assert_eq!(first, joined, "floor {floor:?}");
                    compared += 1;
                }
            }
        }
    }
    assert!(compared > 200, "the law was checked on {compared} cases, too few to mean anything");
}

#[test]
fn members_that_met_the_floor_at_different_times_converge_on_exchange() {
    // Unrestricted: pre-floor deletes allowed. X is what a member holds that joined first and thinned
    // after, Y what one holds that only ever saw thinned states. The state they meet on absorbs both,
    // so a digest repair converges every member to it. (The met state need not equal X: a life the
    // delete killed in X stands in Y, and the met state is the one with the write after the floor.)
    let mut compared = 0usize;
    for ret in [1_000_000_000, 6] {
        let o = opts(ret);
        for pool in all_pools(&o) {
            for (k, a) in pool.iter().enumerate() {
                let b = &pool[(k * 5 + 1) % pool.len()];
                for floor in stamps(a).into_iter().chain(stamps(b)).step_by(3) {
                    let thin = |s: &MergeState| {
                        let mut t = s.clone();
                        t.drop_at_or_below(floor);
                        t
                    };
                    let x = thin(&join(a, b, &o).0);
                    let y = join(&thin(a), &thin(b), &o).0;
                    let met = thin(&join(&x, &y, &o).0);
                    assert_eq!(
                        thin(&join(&met, &x, &o).0),
                        met,
                        "absorbs the early learner, floor {floor:?}"
                    );
                    assert_eq!(
                        thin(&join(&met, &y, &o).0),
                        met,
                        "absorbs the late learner, floor {floor:?}"
                    );
                    assert_eq!(met, thin(&join(&y, &x, &o).0), "the exchange is symmetric");
                    if a_history_life_could_beat_a_live_one(a, b, floor)
                        && y.regs.values().any(|r| r.stamp.hlc > floor)
                    {
                        assert!(
                            met.regs.values().any(|r| r.stamp.hlc > floor),
                            "the write after the floor is alive in the met state, floor {floor:?}"
                        );
                    }
                    compared += 1;
                }
            }
        }
    }
    assert!(compared > 200, "checked on {compared} cases, too few to mean anything");
}

#[test]
fn a_write_after_the_floor_into_a_life_that_lost_a_concurrent_first_insert_meets_alive() {
    // A and C insert the same document concurrently, each in a life of its own; D saw only the one that
    // would lose, and writes m after the floor, which is the stamp of both inserts. A member that joined
    // everything before it knew the floor lost D's write to the newer life, which the join reports as a
    // loss; one that only saw states thinned at the floor never had the older life compete.
    let o = opts(1_000_000_000);
    let (mut wa, mut wc) = (writer('A', &o), writer('C', &o));
    let sa = wa.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    let sc = wc.insert(&fl(&[("x", i(2))]), 0).unwrap().stamp;
    let (hi, lo) = if sc > sa { (wc, wa) } else { (wa, wc) };
    let floor = sa.hlc.max(sc.hlc);
    let mut d = writer('D', &o);
    d.receive(&lo.state);
    d.write_set(&p("m"), i(7), 1_000).unwrap();
    assert!(
        d.state.regs.values().any(|r| r.stamp.hlc > floor),
        "premise: D's write is after the floor"
    );
    let thin = |s: &MergeState| {
        let mut t = s.clone();
        t.drop_at_or_below(floor);
        t
    };
    let early = thin(&join(&hi.state, &d.state, &o).0);
    assert_eq!(
        early.visible(&o),
        None,
        "premise: joined with the older life present, the write is lost"
    );
    assert!(!early.losses.is_empty(), "premise: and reported as a loss");
    let late = join(&thin(&hi.state), &thin(&d.state), &o).0;
    let want = Some([(p("m"), i(7))].into_iter().collect());
    assert_eq!(late.visible(&o), want, "filtered first, the write after the floor is alive");
    let met = thin(&join(&early, &late, &o).0);
    assert_eq!(met.visible(&o), want, "the members meet on the write alive");
    assert_eq!(thin(&join(&met, &early, &o).0), met, "absorbing the early learner");
    assert_eq!(thin(&join(&met, &late, &o).0), met, "and the late one");
}

#[test]
fn the_identity_of_a_life_that_stays_is_kept_though_it_is_at_the_floor() {
    // `_id` is written by the insert, at the floor; y is written after it. The life stays, and the document it
    // renders has its `_id` (the tie goes to the drop for every other cell, so x is gone).
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("_id", i(1)), ("x", i(1))]), 0).unwrap().stamp;
    a.write_set(&p("y"), i(2), 0).unwrap();
    let mut st = a.state.clone();
    assert!(st.drop_at_or_below(first.hlc));
    assert!(st.regs.keys().any(|k| k.1 == p("_id")), "the `_id` register stays");
    assert!(st.regs.keys().all(|k| k.1 != p("x")), "x, at the floor, goes");
    assert_eq!(
        st.visible(&o),
        Some([(p("_id"), i(1)), (p("y"), i(2))].into_iter().collect()),
        "the identity of the life that stays is rendered"
    );
    assert!(st.check_decoded().is_ok());
    let mut again = st.clone();
    assert!(!again.drop_at_or_below(first.hlc), "idempotent with the identity kept");
}

#[test]
fn the_identity_alone_is_no_reason_to_keep_a_life_at_or_below_the_floor() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("_id", i(1)), ("x", i(1))]), 0).unwrap().stamp;
    let mut st = a.state.clone();
    assert!(st.drop_at_or_below(first.hlc));
    assert!(st.is_bare(), "a life with nothing after the floor but its `_id` goes");
    assert!(st.regs.is_empty(), "and takes its `_id` with it");
}

// ---- the absorbed pair and the floor (q7-absorbed, F1 and F4) ---------------------------------------------

fn read(st: &MergeState, o: &JoinOptions, path: &str) -> Option<Value> {
    st.visible(o).and_then(|m| m.get(&p(path)).cloned())
}

/// Every pair of every slot, as `(slot life, writing life, pair)`.
fn pairs_of(st: &MergeState) -> Vec<(MStamp, MStamp, Pair)> {
    st.slots
        .iter()
        .flat_map(|(k, s)| s.absorbed.iter().map(move |pr| (k.0, pr.life, *pr)))
        .collect()
}

/// F1: a pair goes with its slot. L is inserted; A adds 5 (a slot of 5) and sets `c` to 0 (the base absorbs the slot); the floor is the
/// set; B writes `x` above it; A applies the floor and adds 1. The slot and its pair are gone: `c` reads 1, never the -4 a pair that
/// outlived its slot would subtract (`1 - 5`).
#[test]
fn a_pair_goes_with_its_slot_at_the_floor_and_a_new_increment_reads_whole() {
    let o = opts(1_000_000_000);
    let (mut a, mut b) = (writer('A', &o), writer('B', &o));
    b.insert(&fl(&[("k", i(1))]), 0).unwrap();
    let ins = b.state.clone();
    a.receive(&ins);
    a.write_inc(&p("c"), 5, 0).unwrap();
    let floor = a.write_set(&p("c"), i(0), 0).unwrap().stamp.hlc;
    assert!(!pairs_of(&a.state).is_empty(), "premise: the base absorbed the slot");
    b.write_set(&p("x"), i(1), 1_000).unwrap();
    let w = b.state.clone();
    a.receive(&w);
    assert!(a.state.drop_at_or_below(floor));
    assert!(pairs_of(&a.state).is_empty(), "the slot went with its stamp, and its pair with it");
    assert!(a.state.check_decoded().is_ok());
    a.write_inc(&p("c"), 1, 0).unwrap();
    assert_eq!(read(&a.state, &o, "c"), Some(i(1)));
    assert_eq!(read(&a.state, &o, "x"), Some(i(1)));
}

/// The history of F4 and F5 (`q7-floor-probes.py`, `q7-floor-commute.py`): A upserts `+1` (life U); D adds 2 into U; B inserts P (newer than
/// U); A receives both and sets `c` to 0 INTO P (a base made in P that absorbs both U slots: the pairs are P's). Returns A, C and the floor, with
/// `second_base` adding a base made in U itself before P exists (a pair of the slot's own life), and `c_supersedes_p` a member C whose newer insert
/// supersedes P.
struct F4 {
    a: Writer,
    c: Writer,
    floor: Hlc,
}

fn f4_history(o: &JoinOptions, second_base: bool, c_supersedes_p: bool) -> F4 {
    let (mut a, mut b, mut c, mut d) =
        (writer('A', o), writer('B', o), writer('C', o), writer('D', o));
    a.upsert_inc(&p("c"), 1, 0).unwrap();
    let u = a.state.clone();
    d.receive(&u);
    d.write_inc(&p("c"), 2, 0).unwrap();
    let inc = d.state.clone();
    a.receive(&inc);
    if second_base {
        a.write_set(&p("c"), i(0), 0).unwrap();
        a.write_inc(&p("c"), 1, 0).unwrap();
    }
    b.insert(&fl(&[("k", i(1))]), 1).unwrap();
    let pins = b.state.clone();
    a.receive(&pins);
    a.write_set(&p("c"), i(0), 0).unwrap();
    if c_supersedes_p {
        // C inserts Q, newer than P, and receives A's whole state: P is superseded at C, and its cells and pairs are dropped
        c.insert(&fl(&[("k", i(2))]), 5).unwrap();
        let sa = a.state.clone();
        c.receive(&sa);
    }
    let floor = a
        .state
        .vv
        .values()
        .chain(c.state.vv.values())
        .chain(d.state.vv.values())
        .copied()
        .max()
        .expect("a version vector");
    // D, holding only U, adds 1 above the floor
    d.write_inc(&p("c"), 1, 1_000).unwrap();
    let after = d.state.clone();
    a.receive(&after);
    if c_supersedes_p {
        c.receive(&after);
    }
    F4 { a, c, floor }
}

/// F4(b): the floor removes the writing life P and keeps the slot of U that D's write after the floor moved, so the pair is RE-HOMED
/// onto U: the pre-floor `+2` stays absorbed and `c` reads 1 (`+1`, `+2`, set 0, floor, `+1`). Without it the `+2` resurfaces: 3.
#[test]
fn the_floor_rehomes_a_pair_whose_writing_life_it_removes_onto_the_slots_own_life() {
    let o = opts(1_000_000_000);
    for second_base in [false, true] {
        let mut f = f4_history(&o, second_base, false);
        assert!(
            pairs_of(&f.a.state).iter().any(|(slot, writing, _)| slot != writing),
            "premise: a pair of a write made in another life than its slot's"
        );
        assert_eq!(read(&f.a.state, &o, "c"), Some(i(1)), "before the floor");
        f.a.state.drop_at_or_below(f.floor);
        assert!(f.a.state.check_decoded().is_ok());
        let pairs = pairs_of(&f.a.state);
        assert_eq!(pairs.len(), 1, "second_base={second_base}: the slot D moved keeps one pair");
        let (slot_life, writing, pr) = pairs[0];
        assert_eq!(slot_life, writing, "the pair sits on the slot's own life now");
        assert_eq!(pr.life, slot_life);
        assert_eq!(pr.total, Num::from(2), "what D's +2 made of the slot");
        assert_eq!(read(&f.a.state, &o, "c"), Some(i(1)), "second_base={second_base}");
        // the rule it replaces (retain by writing life): no pair left, the +2 resurfaces
        let mut bare = f.a.state.clone();
        for s in bare.slots.values_mut() {
            s.absorbed.clear();
        }
        assert_eq!(
            read(&bare, &o, "c"),
            Some(i(3)),
            "without the re-home the pre-floor +2 resurfaces"
        );
    }
}

/// The re-home merges with the pair the slot already has on its own life by the higher stamp, whichever way round they come.
#[test]
fn a_rehomed_pair_merges_with_the_slots_own_by_the_higher_stamp() {
    let pair = |life: u64, stamp: u64, total: i128| Pair {
        life: MStamp { hlc: Hlc::new(life, 0), writer: wid('A', 0) },
        stamp: MStamp { hlc: Hlc::new(stamp, 0), writer: wid('A', 0) },
        total: total.into(),
    };
    let u = pair(1, 2, 7).life;
    for (own, moved, want) in
        [(pair(1, 3, 4), pair(2, 5, 9), 9_i128), (pair(1, 6, 4), pair(2, 5, 9), 4)]
    {
        let mut slot = Slot::new(pair(9, 9, 0).stamp, 0.into(), Some(own));
        slot.rehome(u, moved);
        assert_eq!(slot.absorbed.len(), 1, "one pair per life");
        assert_eq!((slot.absorbed[0].life, slot.absorbed[0].total), (u, Num::from(want)));
    }
    let mut slot = Slot::new(pair(9, 9, 0).stamp, 0.into(), None);
    slot.rehome(u, pair(2, 5, 9));
    assert_eq!(
        slot.absorbed.iter().map(|x| x.life).collect::<Vec<_>>(),
        [u],
        "a pair of another life is re-homed, the life is the slot's"
    );
}

/// F5 (`q7-floor-commute.py`): the route case. P is also superseded, at one member, and both lives are at or below the floor. A holds P
/// alive with the pairs; C holds P superseded and the pairs dropped. Floor each state and then join (every thinning route does): the
/// re-home happens, reads 1. Join first and floor when the floor is learned: the pair went with P, reads 3. The documented transient. A
/// repair join of the two results, either way round and each floored again, is ONE canonical state and reads 1: the union keeps the
/// pair of an alive writing life, and more absorption never raises the count.
#[test]
fn floor_then_join_and_join_then_floor_differ_for_a_superseded_writing_life_and_a_repair_settles_them()
 {
    let o = opts(1_000_000_000);
    let f = f4_history(&o, false, true);
    let (a, b) = (f.a.state.clone(), f.c.state.clone());
    assert!(!pairs_of(&a).is_empty(), "premise: A holds the pairs of P");
    assert!(
        pairs_of(&b).iter().all(|(slot, writing, _)| slot == writing),
        "premise: C holds none of P (superseded): {:?}",
        pairs_of(&b)
    );
    let floored = |s: &MergeState| {
        let mut t = s.clone();
        t.drop_at_or_below(f.floor);
        t
    };
    let x = join(&floored(&a), &floored(&b), &o).0;
    let y = floored(&join(&a, &b, &o).0);
    assert_eq!(read(&x, &o, "c"), Some(i(1)), "floor first: the pair is re-homed");
    assert_eq!(
        read(&y, &o, "c"),
        Some(i(3)),
        "join first: the pair went with P, the pre-floor +2 resurfaces (the documented transient)"
    );
    // the repair: each takes the other's state (a snapshot), and both floor again
    let (xy, yx) = (join(&x, &y, &o).0, join(&y, &x, &o).0);
    let all = [xy.clone(), yx.clone(), floored(&xy), floored(&yx)];
    for s in &all {
        assert_eq!(read(s, &o, "c"), Some(i(1)), "after the repair, everybody reads 1");
        assert_eq!(s.compact_bytes(), all[0].compact_bytes(), "one canonical state");
    }
}
