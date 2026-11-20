//! The ORDER differential: random scripts of local writes and receives, run on the Python model
//! (`tests/data/gen_ord_differential.py`, the vendored model6.py, ABSORBED on, implicit markers on,
//! retention 10^9), replayed here through `Writer`s. After EVERY step the acting member's ordered document
//! and order state must be the model's, and the step must give the stamp the model gave, or none: a
//! refusal or a no-op is part of what is compared.
//!
//! The model's state is not in the repository; the fixture `tests/data/ord_diff_scripts.json` records, for
//! each step, a digest (FNV-1a 64) of the canonical text of the member's ordered document and of its order
//! state (after a join with the empty state, as the Rust writer's state is normalised). The text is
//! written by the functions below in the words the generator uses. A disagreement is a finding: the
//! generator prints the model's text for every step of one script (`gen_ord_differential.py <dir> <seed>`),
//! and this test prints the Rust text of the first step that differs.
//!
//! The stage-A differentials (`merge_state_diff*.rs`) compare bytes and hashes with the Ord state cleared
//! (the vendored stage-A model has none); this one is where the Ord state meets a model that has it. The
//! restriction is lifted at the D8 re-vendor. No field in the scripts is named "" (the model keys the root of
//! a replace by "" and cannot tell it from a top-level field of that name; the Rust keys them apart and has
//! its own test).

#[path = "common/ord_replay.rs"]
mod ord_replay;

use kimmy_core::merge_state::JoinOptions;

use ord_replay::replay;

#[test]
fn the_writers_order_state_and_ordered_document_equal_the_models_after_every_step() {
    let f = replay(&JoinOptions::default());
    for b in f.bad.iter().take(3) {
        eprintln!("{b}");
    }
    assert!(
        f.bad.is_empty(),
        "{} scripts disagree: {:?}",
        f.bad.len(),
        f.bad.iter().map(|b| b.lines().next().unwrap()).collect::<Vec<_>>()
    );
    println!(
        "ord differential: {} scripts, {} steps ({} writes, {} of them refused or no-ops, {} receives), {} documents and {} order entries compared",
        f.scripts, f.steps, f.writes, f.refused, f.receives, f.ordered_docs, f.ord_keys
    );
    // non-vacuity, pinned (the scripts are seeded)
    assert_eq!(
        (f.scripts, f.steps, f.writes, f.refused, f.receives),
        (300, 9000, 6155, 1451, 2845)
    );
    // the upserts of nested paths (A2.3 and eb40bbcf name their ancestors): drawn, and compared like every other step
    assert_eq!(ord_replay::nested_upserts(), 94);
    // THE FLIP'S EVIDENCE. The scripts are the model's with ABSORBED on, which the writers equal. Run with ABSORBED off, the
    // model makes another script of these seeds: in 4 of the 300 the digest of an operation (or its result) differs at the
    // step given, in 5 more only the digest after the full sync differs (step 30). The seeds are pinned.
    // (Re-vendored for the absorbed pair keyed by its writing life: a base write made into a life that the newest insert then
    // supersedes no longer absorbs the counters it saw, so the model's ABSORBED-on scripts of flat 138, nested 1083 and nested 1141
    // are the ABSORBED-off ones, and 7 of the 300 became 4. The 5 at the full sync are the same seeds as before.)
    let d = ord_replay::off_differences();
    assert!(
        d.iter().all(|x| x.3),
        "every recorded script differs from the ABSORBED-off model: {d:?}"
    );
    let (in_step, at_sync): (Vec<_>, Vec<_>) = d.iter().partition(|x| x.2 < 30);
    let key = |v: &[&(String, u64, usize, bool)]| {
        v.iter().map(|x| (x.0.clone(), x.1, x.2)).collect::<Vec<_>>()
    };
    let want = |v: &[(&str, u64, usize)]| {
        v.iter().map(|(k, s, i)| (k.to_string(), *s, *i)).collect::<Vec<_>>()
    };
    assert_eq!(
        key(&in_step),
        want(&[("flat", 40, 20), ("flat", 80, 22), ("flat", 132, 16), ("flat", 143, 28),])
    );
    assert_eq!(
        key(&at_sync),
        want(&[
            ("flat", 48, 30),
            ("flat", 70, 30),
            ("flat", 78, 30),
            ("nested", 1022, 30),
            ("nested", 1126, 30),
        ])
    );
    assert!(f.ordered_docs > 7000 && f.ord_keys > 30_000, "{} {}", f.ordered_docs, f.ord_keys);
}

/// Each switch of the order state, set wrong, makes the differential disagree: the random scripts kill
/// all sixteen on their own (the named scenarios of `ord_tests` are the sharper killers).
#[test]
fn every_switch_of_the_order_state_is_caught_by_the_random_scripts() {
    type Set = fn(&mut JoinOptions);
    let switches: [(&str, Set); 16] = [
        ("ord_keep_all", |o| o.set_ord_keep_all(true)),
        ("ord_latest", |o| o.set_ord_latest(true)),
        ("ord_no_last", |o| o.set_ord_no_last(true)),
        ("ord_by_name", |o| o.set_ord_by_name(true)),
        ("ord_no_survivor_clamp", |o| o.set_ord_no_survivor_clamp(true)),
        ("ord_no_level_floor", |o| o.set_ord_no_level_floor(true)),
        ("ord_ignores_bit", |o| o.set_ord_ignores_bit(true)),
        ("ord_no_reset", |o| o.set_ord_no_reset(true)),
        ("ord_no_pos", |o| o.set_ord_no_pos(true)),
        ("ord_no_ancestor", |o| o.set_ord_no_ancestor(true)),
        ("ord_no_hybrid", |o| o.set_ord_no_hybrid(true)),
        ("ord_noop_unset_stands", |o| o.set_ord_noop_unset_stands(true)),
        ("ord_bit_unpacked", |o| o.set_ord_bit_unpacked(true)),
        ("ord_no_first", |o| o.set_ord_no_first(true)),
        ("ord_replace_without_reset", |o| o.set_ord_replace_without_reset(true)),
        ("ord_reorder_is_noop", |o| o.set_ord_reorder_is_noop(true)),
    ];
    let mut report = Vec::new();
    for (name, set) in switches {
        let mut o = JoinOptions::default();
        set(&mut o);
        let f = replay(&o);
        report.push(format!("{name}: {} disagreements", f.bad.len()));
        assert!(!f.bad.is_empty(), "{name} is not caught by the random scripts");
    }
    println!("{}", report.join("\n"));
}

/// The DELIVERY scripts: 200 scripts (100 flat, 100 nested, 36 steps) in which, besides the writes, a member takes the peer's current state
/// (a snapshot, no oplog entry), pulls a window of the peer's oplog entries (each skipped when the receiver's vector covers the entry's
/// state and joined otherwise, the window cut short when more entries follow), backs itself up, and goes back to the backup under the next
/// epoch (a new writer identity, the clock kept). After every step the acting member's ordered document and order state must be the model's,
/// the skip and the append of every entry must be the model's, and the stamps the member gives after a restore carry its new epoch.
///
/// The counts are what the replay really did (not what the generator drew), and they are pinned: a delivery script that never restored, or
/// whose windows were never cut, would pass without testing what it claims.
#[test]
fn the_delivery_scripts_give_the_models_orders_after_a_snapshot_a_covered_skip_a_cut_window_and_a_restore()
 {
    use ord_replay::{Delivery, replay_delivery};
    let f = replay_delivery(&JoinOptions::default());
    for b in f.bad.iter().take(3) {
        eprintln!("{b}");
    }
    assert!(
        f.bad.is_empty(),
        "{} scripts disagree: {:?}",
        f.bad.len(),
        f.bad.iter().map(|b| b.lines().next().unwrap()).collect::<Vec<_>>()
    );
    println!("delivery differential: {} scripts, {} steps, {:?}", f.scripts, f.steps, f.delivery);
    assert_eq!((f.scripts, f.steps), (200, 7200));
    assert_eq!(
        f.delivery,
        Delivery {
            snaps: 566,
            backups: 270,
            restores: 52,
            scripts_with_a_restore: 41,
            pulls: 1921,
            truncated: 839,
            skipped: 1573,
            applied: 1592,
            writes_after_a_restore: 88,
            restore_then_a_pull_that_joined: 17,
            restore_then_a_snapshot: 13,
        }
    );
}

/// The two mutants of the absorbed pair (the old keying by the slot's life; one pair per slot) and the fold mutant that folds only the write's
/// own life, run over both groups of scripts. The random sim does not draw the shapes of the first two, the differential does: the old keying
/// disagrees in 3 of the 300 plain scripts (flat 138, nested 1083 and 1141, the same three that the ABSORBED-off model no longer differs on)
/// and in 1 of the 200 delivery scripts; one pair per slot in 1 of the plain scripts. The numbers are pinned so that a change of a draw, of the
/// model or of the replay that makes a group blind to them is noticed.
#[test]
fn the_absorbed_pair_mutants_are_caught_by_the_ord_differential() {
    use ord_replay::{replay, replay_delivery};
    type Set = fn(&mut JoinOptions);
    let mutants: [(&str, Set, usize, usize); 3] = [
        ("absorb_by_slot_life", |o| o.set_absorb_by_slot_life(true), 3, 1),
        ("single_pair", |o| o.set_single_pair(true), 1, 0),
        ("fold_life_only", |o| o.set_fold_life_only(true), 31, 25),
    ];
    for (name, set, plain, delivery) in mutants {
        let mut o = JoinOptions::default();
        set(&mut o);
        assert_eq!(
            (replay(&o).bad.len(), replay_delivery(&o).bad.len()),
            (plain, delivery),
            "{name}: scripts that disagree (plain, delivery)"
        );
    }
}
