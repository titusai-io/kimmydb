//! The ORDER differential: random scripts of local writes and receives, run on the A2.1 Python model
//! (`tests/data/gen_ord_differential.py`, model6.py 865808753a5f9a4b, ABSORBED off, no implicit markers,
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
        (300, 9000, 6154, 1452, 2846)
    );
    // the upserts of nested paths (A2.3 and eb40bbcf name their ancestors): drawn, and compared like every other step
    assert_eq!(ord_replay::nested_upserts(), 94);
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
