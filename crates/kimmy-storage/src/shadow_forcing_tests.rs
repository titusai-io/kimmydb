//! A vector shadow's merge policy is FORCED to `document` on every route (unit P's rows; the tester's, written Thu Nov 26 2026 against 859cfa17).
//!
//! WHY THESE ROWS EXIST. `MergePolicy::fixed_for(name)` (ddl.rs:99-105) makes every shadow name `document` whatever a creation asks, and it is
//! applied at SEVEN sites: engine.rs:5908 (`create_collection_in_txn`, the one every route's creation goes through), engine.rs:5614
//! (`adopt_creation_policy`), register.rs:1385 (`BaseOptions::of_creation`, the options row), sync.rs:2705 and :2726 (the entries route's
//! held branch) and snapshot.rs:4147 (the page route's held guard; its adoption goes through `adopt_creation_policy`). Nothing pinned any of them. An
//! honest member never sends a shadow creation that says `field` or carries `R`/`K` (the entry is built from the FORCED policy and the
//! stored `R`/`K`, engine.rs ~6105), so these are the defence against a member whose own check was bypassed, and the convergence argument:
//! a shadow held `document` on one member and `field` on another is a "members differ" failure that nothing else would see.
//!
//! FOUND WHILE WRITING THEM (read from 859cfa17, not run):
//! 1. The held branch compares the entry's RAW `R`/`K` with the held ones (sync.rs:2705, :2726, snapshot.rs:4147), while the held ones were stored through the forcing
//!    (`R`/`K` kept only for `field`). A shadow-named creation that carries `R` therefore ALWAYS "differs" from what is held. On the entries route that is a
//!    refusal on the SECOND delivery of the same entry (sync.rs:2705: the held `merge_policy_at` equals the entry's stamp only on a member that made the shadow BY this
//!    entry, so a member whose shadow another creation made does not refuse it), counted in `ddl_refused`: a verdict that depends on which creation made the
//!    held shadow, not on the entry. On the page route the same comparison is a HARD ERROR that stops the page (snapshot.rs:4147-4157, `Unsupported`),
//!    where an honest page never trips it. State still converges either way; the refusal is noise on one member and an
//!    error on a pull. THE CONVERGENT ANSWER, one line in each site and the rule `of_creation` already follows: compare the entry's options THROUGH the forcing
//!    (`R`/`K` read as `None` when `fixed_for(name)` is `document`), so a creation of a shadow name is never refused or an error for what the forcing discards.
//!    ADOPTED (hardproblems' ruling, adopted by the manager Thu Nov 26 2026). The rule is stated on the EFFECTIVE policy: `R` and `K` are what `of_creation` keeps,
//!    `None` unless the forced policy is `field`. In P the two "two Rs" refusals (sync.rs:2705 and the guard at snapshot.rs:4144-4161) go away, so PD1, PD2 and PF3 turn
//!    green BY CONSTRUCTION. The three DESIGNED-RED rows below pin it.
//! 2. `merge_policy_at` of a held shadow depends on arrival order (a later creation with the same forced policy and `R` moves nothing when it arrives
//!    second and is the base when it arrives first). Benign for a shadow, whose policy text is constant, and it is the design for every `document`
//!    collection; it matters only through the guard of 1. NOT pinned here: pinning it would cement an order dependence. The consequence is that the
//!    mutant removing `fixed_for` at sync.rs:2726 is observable only through `merge_policy_at` (adopt re-forces at 5614), so it is an EQUIVALENT mutant on state;
//!    do not write a row for it unless P changes `merge_policy_at` for shadows.
//! 3. A carried options row is judged by its KEY alone: `judge_carried_row` (register.rs:1471-1478) and the restore check (register.rs:2347-2362) ask only that the names the row
//!    holds derive the key. A row that says `field` with an `R` for a shadow, or says `at` above `Hlc::ZERO`, is held and RISES over the honest one (the rule orders
//!    options by `(at, policy text)`), and is served on to every joiner forever. No creation route writes such a row (`of_creation` makes the one normal form), so a row that is not
//!    `of_creation` of its own fields is a member whose check was bypassed. The rule: refuse it, as the other shape checks do.
//!
//! ROWS AND THE MUTANTS THEY KILL (every mutant removes one `fixed_for`):
//! - PF1 (a held shadow, a later creation carrying `field`, `R`, `K`): engine.rs:5614. Also reads the options row (register.rs:1385).
//! - PF2 (two creations, both orders, fresh members): engine.rs:5908 (the member that meets the forged one FIRST makes the shadow from it) and
//!   register.rs:1385 (the options row ties at `Hlc::ZERO` for a shadow and breaks the tie by policy text, so an unforced `field` row WINS in both orders).
//! - PF3 (the page route, a fresh member, a forged page): DESIGNED RED at 859cfa17 (the manager's correction): the forged page makes the shadow, and the guard at
//!   snapshot.rs:4144-4161 in the SAME `restore_collection` call returns `Unsupported` against the collection it just made (its `R` is `None`, the page's is not), so the
//!   pull fails. Green by construction in P (the guard goes away); THEN it kills engine.rs:5908 on the page's creation and register.rs:1385 through snapshot.rs:3978.
//! - PD1, PD2 (ADOPTED, DESIGNED RED today, see 1): the redelivery refusal (sync.rs:2705) and the page error (snapshot.rs:4144-4161). Both go green with the rule in 1.
//! - PF4 (a NEW P item, a convergence fix; see 3): a carried options row must equal `of_creation` of its OWN fields. Page rows (scoped page) and, in a `cfg(any())` block
//!   to install in backup_register_tests.rs, the backup rows. DESIGNED RED today. Mutants: the normal-form check removed at register.rs `judge_carried_row` (the page rows)
//!   and at the restore check (the backup rows), `PF4-M1` and `PF4-M2`.
//!
//! Written against 859cfa17 (unit S's tip, on 7b6d2ba4 with N). NOT compiled, NOT run, NOT queued. First-compile checks: that `CollectionState`'s
//! `merge_policy`, `join_retention_ms` and `loss_limits` are assignable from this crate; that a scoped page of a shadow by its id applies as any collection's does;
//! `BaseOptions`' fields and `carried_rows` as `names_and_options_tests.rs` uses them. Install as crates/kimmy-storage/src/shadow_forcing_tests.rs with
//! `#[cfg(test)] mod shadow_forcing_tests;`.

use kimmy_core::{
    CollectionCreate, CollectionId, Epoch, Hlc, LossLimits, MergePolicy, NodeId, OpKind,
    OplogEntry, Stamp,
};
use redb::ReadableDatabase;

use crate::register::{BaseOptions, Carried};
use crate::{Engine, SnapshotProgress, physical_now_ms};

const DB: &str = "app";
const SHADOW: &str = "orders.__vectors";
/// A join retention no honest shadow carries.
const R: u64 = 86_400_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn st(wall_ms: u64, origin: u8) -> Stamp {
    Stamp::new(Hlc::new(wall_ms, 0), NodeId::from_bytes([origin; 16]), Epoch::ZERO)
}

/// A creation of the shadow's name as a member whose own check was bypassed would send it: the policy and the options as ASKED, never forced.
fn creation_of_the_shadow(at: Stamp, field: bool) -> OplogEntry {
    OplogEntry {
        stamp: at,
        kind: OpKind::CreateCollection,
        collection: CollectionId::derive(DB, SHADOW),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionCreate {
                db: DB.into(),
                name: SHADOW.into(),
                merge_policy: if field { MergePolicy::Field } else { MergePolicy::Document },
                validator: None,
                join_retention_ms: field.then_some(R),
                loss_limits: field.then_some(LossLimits::DEFAULT),
                stage: None,
            })
            .expect("the creation encodes"),
        ),
        meta: None,
    }
}

/// What the body of `entry` says, decoded here: the premise that the entry asks for what the test says it asks for.
fn asked(entry: &OplogEntry) -> CollectionCreate {
    bson::deserialize_from_slice(entry.body.as_ref().expect("a body")).expect("the body decodes")
}

fn id_key(name: &str) -> Vec<u8> {
    CollectionId::derive(DB, name).0.to_be_bytes().to_vec()
}

/// The options row of `name`, if the table holds one, read as stored (as `names_and_options_tests.rs` does).
fn options_of(e: &Engine, name: &str) -> Option<BaseOptions> {
    let txn = e.db().begin_read().unwrap();
    let key = id_key(name);
    let (_, value) = crate::register::carried_rows(&txn, Carried::Options)
        .unwrap()
        .into_iter()
        .find(|(k, _)| *k == key)?;
    Some(serde_json::from_slice(&value).unwrap())
}

/// The row every member holds for the shadow by any route: `document`, no `R`, no `K`, and the constant `Hlc::ZERO` (register.rs:1353-1359).
fn the_shadows_row() -> BaseOptions {
    BaseOptions {
        db: DB.to_string(),
        name: SHADOW.to_string(),
        at: Hlc::ZERO,
        merge_policy: MergePolicy::Document,
        join_retention_ms: None,
        loss_limits: None,
        stage: None,
    }
}

/// What the member holds for the shadow: the policy, `R` and `K` of the collection, and the options row.
fn held(e: &Engine) -> (MergePolicy, Option<u64>, Option<LossLimits>, Option<BaseOptions>) {
    let meta = e.get_collection(DB, SHADOW).expect("the shadow is held");
    (meta.merge_policy, meta.join_retention_ms, meta.loss_limits, options_of(e, SHADOW))
}

fn is_the_forced_shadow(e: &Engine, why: &str) {
    assert_eq!(
        held(e),
        (MergePolicy::Document, None, None, Some(the_shadows_row())),
        "the shadow is `document` with no R and no K, and its row is the constant one: {why}"
    );
}

/// A scoped pull of the shadow from `from` into `into`, the pages optionally FORGED to say `field` with an `R` and a `K`, as a sender whose own
/// check was bypassed would serve them. The same loop `merge_policy_tests.rs` pulls with.
fn pull_the_shadow(from: &Engine, into: &Engine, forged: bool) -> crate::Result<()> {
    let mut progress = SnapshotProgress::of_collection(CollectionId::derive(DB, SHADOW));
    while !progress.is_complete() {
        let mut page = from.snapshot_page(progress.after().cloned(), progress.scope())?;
        if forged {
            for state in &mut page.collections {
                state.merge_policy = MergePolicy::Field;
                state.join_retention_ms = Some(R);
                state.loss_limits = Some(LossLimits::DEFAULT);
            }
        }
        into.apply_snapshot_page(from.node_id(), &mut progress, &page)?;
    }
    Ok(())
}

// ---- PF1: the held shadow ---------------------------------------------------------------------------------------------------------------------

/// A member holds the shadow (made by the system path: `document`). A LATER creation of the name arrives, asking `field` with an `R` and a `K`:
/// the entries route's held branch sees the `R` differ and calls `adopt_creation_policy`, which must read the policy through the forcing.
/// Kills: engine.rs:5614 (without it the later creation wins and the shadow becomes `field`, with its `R` and `K`); the options row (register.rs:1385).
#[test]
fn pf1_a_held_shadow_takes_no_field_policy_from_a_later_creation_that_asks_for_one() {
    let (m, _d) = engine();
    m.create_system_collection(DB, SHADOW).expect("the shadow is made by the system path");
    is_the_forced_shadow(&m, "premise: before the creation arrives");
    let later = creation_of_the_shadow(st(physical_now_ms() + 2_000, 7), true);
    let body = asked(&later);
    assert_eq!(
        (body.merge_policy, body.join_retention_ms, body.loss_limits),
        (MergePolicy::Field, Some(R), Some(LossLimits::DEFAULT)),
        "premise: the creation asks for `field`, with an R and a K"
    );
    m.apply_batch(&[later]).expect("the creation applies");
    is_the_forced_shadow(&m, "after a later creation that asked for `field`");
}

// ---- PF2: two creations of the shadow, in both orders ----------------------------------------------------------------------------------------

/// Two creations of the shadow reach two fresh members in opposite orders: an honest one (`document`) and one that asks for `field` with an `R` and a
/// `K`. Every member ends with the same shadow: `document`, no `R`, no `K`, the same constant row. (`merge_policy_at` may differ and is not asked.)
/// Kills: engine.rs:5908 (the member that meets the `field` one FIRST makes the shadow from it) and register.rs:1385 (the row ties at ZERO and the
/// policy text breaks the tie, so an unforced `field` row wins in both orders).
#[test]
fn pf2_two_creations_of_a_shadow_leave_every_member_with_the_forced_shadow_in_both_orders() {
    let base = physical_now_ms();
    let honest = creation_of_the_shadow(st(base - 3_000, 1), false);
    let forged = creation_of_the_shadow(st(base - 2_000, 2), true);
    assert_eq!(asked(&honest).merge_policy, MergePolicy::Document, "premise: the honest one");
    assert_eq!(asked(&forged).merge_policy, MergePolicy::Field, "premise: the forged one");
    for (first, second, order) in
        [(&honest, &forged, "honest, then forged"), (&forged, &honest, "forged, then honest")]
    {
        let (m, _d) = engine();
        m.apply_batch(std::slice::from_ref(first)).expect("the first applies");
        assert!(
            m.get_collection(DB, SHADOW).is_ok(),
            "premise: the first made the shadow ({order})"
        );
        m.apply_batch(std::slice::from_ref(second)).expect("the second applies");
        is_the_forced_shadow(&m, order);
    }
}

// ---- PF3: the page route ------------------------------------------------------------------------------------------------------------------------

/// A fresh member pulls the shadow from a sender whose pages say `field` with an `R` and a `K` (forged): the page's creation is made through the forcing.
/// DESIGNED RED at 859cfa17 (the manager's correction, hardproblems' read): the page makes the shadow, and the guard at snapshot.rs:4144-4161 in the same `restore_collection` call
/// then returns `Unsupported` against the collection it just made (that collection's `R` is `None`, the page's is not), so `.expect("the pull applies")` fails. Green by construction
/// in P (the rule on the effective policy removes the guard); then it kills engine.rs:5908 on the page's creation, and register.rs:1385 through the row the page route writes (snapshot.rs:3978).
#[test]
fn pf3_designed_red_a_page_that_says_field_makes_the_forced_shadow_on_a_fresh_member() {
    let (from, _df) = engine();
    from.create_system_collection(DB, SHADOW).expect("the sender's shadow");
    let (m, _dm) = engine();
    assert!(m.get_collection(DB, SHADOW).is_err(), "premise: the receiver holds nothing");
    pull_the_shadow(&from, &m, true).expect("the pull applies");
    is_the_forced_shadow(&m, "made from a page that said `field`");
}

// ---- PD1, PD2: ADOPTED (hardproblems' ruling, adopted by the manager Thu Nov 26 2026), DESIGNED RED today (see FOUND 1): a shadow-named creation is judged on its EFFECTIVE policy -----------

/// The same forged creation delivered TWICE to a fresh member, and once to a member whose shadow another creation made: none of the three is refused
/// (`ddl_refused` 0), and the state is the forced shadow. RED at 859cfa17: the second delivery to the first member is refused (sync.rs:2705: the held
/// `merge_policy_at` is the entry's stamp, the policy equals the forced one, and the entry's `R` is not the held `None`), the other member does not refuse it:
/// one entry, two verdicts. GREEN with the entry's `R` and `K` read through the forcing at 2705, 2726 (and 4147 for the page).
#[test]
fn pd1_designed_red_a_shadow_named_creation_is_never_refused_for_what_the_forcing_discards() {
    let forged = creation_of_the_shadow(st(physical_now_ms() - 1_000, 2), true);
    let (m, _d) = engine();
    let first = m.apply_batch(std::slice::from_ref(&forged)).expect("the first delivery applies");
    assert_eq!(first.ddl_refused, 0, "the first delivery is not refused");
    let second = m.apply_batch(std::slice::from_ref(&forged)).expect("the second delivery applies");
    assert_eq!(second.ddl_refused, 0, "a redelivery of the same creation is not refused either");
    is_the_forced_shadow(&m, "after the same creation twice");

    let (n, _dn) = engine();
    n.create_system_collection(DB, SHADOW).expect("a shadow another creation made");
    let other = n.apply_batch(std::slice::from_ref(&forged)).expect("the creation applies");
    assert_eq!(other.ddl_refused, 0, "and a member whose shadow another creation made agrees");
    is_the_forced_shadow(&n, "on a member whose shadow another creation made");
}

/// The page route's twin of PD1: the same shadow pulled twice, the second time from a sender whose pages say `field` with an `R` and a `K`, for the SAME
/// creation (the same stamps). The pull applies and the shadow is unchanged. RED at 859cfa17: snapshot.rs:4147-4157 stops the page with `Unsupported`
/// ("a creation cannot have two join retentions"), for an `R` the forcing discards; an honest page never trips it. GREEN with the page's `R` and `K` read
/// through the forcing.
#[test]
fn pd2_designed_red_a_page_of_a_shadow_is_not_an_error_for_what_the_forcing_discards() {
    let (from, _df) = engine();
    from.create_system_collection(DB, SHADOW).expect("the sender's shadow");
    let (m, _dm) = engine();
    pull_the_shadow(&from, &m, false).expect("the honest pull applies");
    is_the_forced_shadow(&m, "premise: the honest pull made it");
    pull_the_shadow(&from, &m, true)
        .expect("a page that says `field` for the same creation applies");
    is_the_forced_shadow(&m, "after the page that said `field`");
}

// ---- PF4: a carried options row is the normal form of its own fields (a NEW P item, a convergence fix; see FOUND 3) -------------------------------------

/// The scoped page of the shadow from `from`, its carried options row REPLACED by `forged`: the key stays the shadow's own, so the one check made today (the names the row holds derive
/// the key, register.rs:1471-1478) passes, and only a check that the row is `of_creation` of its own fields can refuse it.
fn the_shadows_page_with_its_row_replaced(
    from: &Engine,
    forged: &BaseOptions,
) -> crate::SnapshotPage {
    let mut page = from.snapshot_page(None, Some(CollectionId::derive(DB, SHADOW))).unwrap();
    let key = id_key(SHADOW);
    let row = page
        .carried
        .iter_mut()
        .find(|row| row.table == crate::CARRY_OPTIONS && row.key.to_vec() == key)
        .expect("premise: the scoped page carries the shadow's options row");
    row.value = serde_bytes::ByteBuf::from(serde_json::to_vec(forged).unwrap());
    page
}

/// A receiver that holds the shadow (and its normal row) is pulled a page whose carried row is `forged`: the page is refused, and the held row is unchanged.
fn a_forged_row_refuses_the_page(forged: BaseOptions) {
    let (from, _df) = engine();
    from.create_system_collection(DB, SHADOW).expect("the sender's shadow");
    let (m, _dm) = engine();
    m.create_system_collection(DB, SHADOW).expect("the receiver's shadow");
    assert_eq!(
        options_of(&m, SHADOW),
        Some(the_shadows_row()),
        "premise: the receiver holds the normal row"
    );
    assert_ne!(forged, the_shadows_row(), "premise: the carried row is not the normal form");
    let page = the_shadows_page_with_its_row_replaced(&from, &forged);
    let mut progress = SnapshotProgress::of_collection(CollectionId::derive(DB, SHADOW));
    let outcome = m.apply_snapshot_page(from.node_id(), &mut progress, &page);
    assert!(outcome.is_err(), "the page is refused: {outcome:?}");
    assert_eq!(
        options_of(&m, SHADOW),
        Some(the_shadows_row()),
        "and the held row is unchanged: a forged row does not rise over it"
    );
}

/// (a) A carried row for the shadow that says `field`, with an `R` and a `K`: the shape no creation route writes. Held, it would rise over the honest row (the later policy text
/// wins a tie at `Hlc::ZERO`) and be served on forever. DESIGNED RED today. Mutant PF4-M1: the normal-form check removed at `judge_carried_row` (register.rs:1471-1478).
#[test]
fn pf4a_designed_red_a_carried_row_for_a_shadow_that_says_field_refuses_the_page() {
    a_forged_row_refuses_the_page(BaseOptions {
        db: DB.to_string(),
        name: SHADOW.to_string(),
        at: Hlc::ZERO,
        merge_policy: MergePolicy::Field,
        join_retention_ms: Some(R),
        loss_limits: Some(LossLimits::DEFAULT),
        stage: None,
    });
}

/// (b) A carried row for the shadow with `at` above `Hlc::ZERO`: every shadow row is the constant `Hlc::ZERO` (register.rs:1353-1359), and a row above it rises over the honest one
/// whatever else it says. DESIGNED RED today. Mutant PF4-M1.
#[test]
fn pf4b_designed_red_a_carried_row_for_a_shadow_with_an_at_above_zero_refuses_the_page() {
    a_forged_row_refuses_the_page(BaseOptions { at: Hlc::new(5, 0), ..the_shadows_row() });
}

/// A GUARD (green today, must stay green): the rule is the normal form, not "no `field`". A plain `field` collection's row keeps its `R`, its `K` and its creation's `Hlc`, and a page that
/// carries it is applied. Without it a check that refused every `field` row, or every row with an `at`, would pass the rows above.
#[test]
fn pf4g_a_plain_field_collections_honest_row_is_carried_and_held() {
    let (from, _df) = engine();
    from.create_collection_with_policy(DB, "wide", MergePolicy::Field, None)
        .expect("a field collection");
    let honest = options_of(&from, "wide").expect("premise: the sender holds its row");
    assert_eq!(honest.merge_policy, MergePolicy::Field, "premise: the row says field");
    assert!(
        honest.join_retention_ms.is_some() && honest.loss_limits.is_some(),
        "premise: with its R and K"
    );
    assert_ne!(honest.at, Hlc::ZERO, "premise: and its creation's own Hlc");
    let (m, _dm) = engine();
    let mut progress = SnapshotProgress::of_collection(CollectionId::derive(DB, "wide"));
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        m.apply_snapshot_page(from.node_id(), &mut progress, &page)
            .expect("the honest page applies");
    }
    assert_eq!(options_of(&m, "wide"), Some(honest), "the receiver holds the sender's row");
}
