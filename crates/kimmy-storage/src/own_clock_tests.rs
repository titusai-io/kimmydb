//! A schema change fails closed when this member's own stamps would be refused by the rest of the cluster (U1, the second
//! half): b1, its logical clock leads its physical clock by more than the allowed skew; b2, a live peer said it is
//! refusing this origin's entries for their clock. Ordinary writes and the node's own system collections are not refused.
//!
//! The clock the guard reads is the engine's U1 clock, which a test moves (`set_clock_offset_for_tests`): moving it back
//! makes the logical clock, which follows the real one, lead it.

use bson::doc;
use kimmy_core::{IndexField, NodeId, ProviderConfig, VectorConfig};

use crate::{Engine, OwnStampRefusal, StorageError};

const SKEW_MS: u64 = 300_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A member whose logical clock leads its physical clock by `lead_ms` more than the bound's `margin_ms` allows.
fn leading(engine: &Engine, by_ms: u64) {
    engine.set_clock_offset_for_tests(-(by_ms as i64));
}

fn is_refused(result: crate::error::Result<()>) -> Option<OwnStampRefusal> {
    match result {
        Err(StorageError::OwnStampsRefused(refusal)) => Some(refusal),
        _ => None,
    }
}

fn a_vector_config() -> VectorConfig {
    VectorConfig {
        fields: vec!["body".into()],
        provider: ProviderConfig::Byo {},
        dim: 8,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// Every schema change a user can make, as a closure that runs it and drops the answer.
type Change<'a> = Box<dyn Fn() -> crate::error::Result<()> + 'a>;

fn every_user_schema_change(engine: &Engine) -> Vec<(&'static str, Change<'_>)> {
    vec![
        ("create_database", Box::new(move || engine.create_database("elsewhere").map(|_| ()))),
        ("drop_database", Box::new(move || engine.drop_database("elsewhere").map(|_| ()))),
        (
            "create_collection",
            Box::new(move || engine.create_collection("shop", "more").map(|_| ())),
        ),
        (
            "create_collection_with_policy",
            Box::new(move || {
                engine
                    .create_collection_with_policy(
                        "shop",
                        "policy",
                        kimmy_core::MergePolicy::Field,
                        None,
                    )
                    .map(|_| ())
            }),
        ),
        ("drop_collection", Box::new(move || engine.drop_collection("shop", "orders").map(|_| ()))),
        (
            "create_index",
            Box::new(move || {
                engine
                    .create_index("shop", "orders", vec![IndexField::ascending("n")], false, None)
                    .map(|_| ())
            }),
        ),
        ("drop_index", Box::new(move || engine.drop_index("shop", "orders", "n_1").map(|_| ()))),
        (
            "set_validator",
            Box::new(move || engine.set_validator("shop", "orders", None).map(|_| ())),
        ),
        (
            "configure_vectors",
            Box::new(move || {
                engine.configure_vectors("shop", "orders", a_vector_config()).map(|_| ())
            }),
        ),
        (
            "disable_vectors",
            Box::new(move || engine.disable_vectors("shop", "orders", false).map(|_| ())),
        ),
    ]
}

#[test]
fn a_member_whose_clock_leads_by_more_than_the_bound_refuses_every_schema_change_and_writes_nothing()
 {
    let (e, _d) = engine();
    let orders = e.create_collection("shop", "orders").unwrap();
    leading(&e, SKEW_MS + 10_000);
    assert!(
        e.clock_lead_ms() > SKEW_MS,
        "premise: the logical clock leads: {} ms",
        e.clock_lead_ms()
    );

    for (name, change) in every_user_schema_change(&e) {
        let refusal = is_refused(change())
            .unwrap_or_else(|| panic!("{name} must be refused when this member's clock leads"));
        assert!(matches!(refusal, OwnStampRefusal::ClockAhead { .. }), "{name}: {refusal:?}");
    }
    assert!(e.get_collection("shop", "more").is_err(), "nothing was written");
    assert!(e.get_collection("shop", "policy").is_err(), "nothing was written");
    assert_eq!(e.list_indexes("shop", "orders").unwrap().len(), 0, "no index was made");
    e.get_collection("shop", "orders").expect("the collection is still here: the drop was refused");

    // Ordinary writes are not refused: for a document it is the existing skew contract.
    e.insert(&orders, doc! { "_id": 1 }).expect("an ordinary write is not refused");
}

#[test]
fn a_member_whose_clock_leads_by_less_than_the_bound_changes_schema_as_usual() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    leading(&e, SKEW_MS - 60_000);
    assert!(e.clock_lead_ms() > 0 && e.clock_lead_ms() <= SKEW_MS);
    e.create_collection("shop", "more").expect("inside the bound: the existing skew contract");
    e.create_index("shop", "orders", vec![IndexField::ascending("n")], false, None).unwrap();
}

#[test]
fn the_message_of_a_leading_clock_names_how_far_and_says_to_retry_elsewhere() {
    let (e, _d) = engine();
    leading(&e, SKEW_MS + 100_000);
    let message = match e.create_collection("shop", "more") {
        Err(e @ StorageError::OwnStampsRefused(_)) => e.to_string(),
        other => panic!("expected a refusal, got {other:?}"),
    };
    assert!(message.contains("ahead") && message.contains("retry on another member"), "{message}");
    assert!(
        message.contains("400 s") || message.contains("401 s"),
        "names the lead in seconds: {message}"
    );
}

#[test]
fn the_node_still_makes_its_own_system_collections_when_its_clock_leads() {
    let (e, _d) = engine();
    leading(&e, SKEW_MS + 10_000);
    e.create_system_collection("__kimmy", "__settings")
        .expect("a member with a leading clock still creates its system collections at start");
    assert!(e.get_collection("__kimmy", "__settings").is_ok());
}

/// The guard is keyed on the call path, not on a name: a client that can change a `__` name (an administrator can change
/// the system database) is refused like any other, and the node's own creations do not come through the guarded functions.
#[test]
fn a_client_schema_change_on_a_system_name_is_refused_when_the_clock_leads() {
    let (e, _d) = engine();
    e.create_system_collection("__kimmy", "__settings").unwrap();
    leading(&e, SKEW_MS + 10_000);
    for (name, change) in [
        ("create_collection", e.create_collection("__kimmy", "__more").map(|_| ())),
        ("drop_collection", e.drop_collection("__kimmy", "__settings").map(|_| ())),
        (
            "create_index",
            e.create_index("__kimmy", "__settings", vec![IndexField::ascending("k")], false, None)
                .map(|_| ()),
        ),
        ("drop_database", e.drop_database("__kimmy").map(|_| ())),
    ] {
        assert!(
            is_refused(change).is_some(),
            "{name} on a system name must be refused by the guard when the clock leads"
        );
    }
    assert!(e.get_collection("__kimmy", "__settings").is_ok(), "nothing was dropped");
}

#[test]
fn a_peer_that_refuses_this_origin_makes_a_schema_change_fail_closed_until_it_is_due() {
    let (e, _d) = engine();
    let orders = e.create_collection("shop", "orders").unwrap();
    let peer = NodeId::generate();
    e.note_own_stamps_refused(peer, 90_000);

    let refusal = is_refused(e.create_collection("shop", "more").map(|_| ()))
        .expect("b2: a standing refusal by a live peer");
    match refusal {
        OwnStampRefusal::PeerRefused { peer: who, due_in_ms } => {
            assert_eq!(who, peer);
            assert!((80_000..=90_000).contains(&due_in_ms), "about 90 s: {due_in_ms}");
        }
        other => panic!("expected PeerRefused, got {other:?}"),
    }
    assert!(e.own_stamps_refused_for_ms() > 80_000, "the gauge");
    e.insert(&orders, doc! { "_id": 1 }).expect("an ordinary write is not refused");
    assert_eq!(
        e.clock_lead_ms(),
        0,
        "this member's own clock is not ahead: only the peer's view is"
    );
}

#[test]
fn a_later_push_that_carried_no_refusal_clears_the_peers_refusal() {
    let (e, _d) = engine();
    let peer = NodeId::generate();
    e.note_own_stamps_refused(peer, 600_000);
    assert!(is_refused(e.create_collection("shop", "a").map(|_| ())).is_some(), "premise");

    e.note_own_stamps_not_refused(peer);

    e.create_collection("shop", "b").expect("the peer took a later push: its refusal is over");
    assert_eq!(e.own_stamps_refused_for_ms(), 0);
}

#[test]
fn a_refusal_that_has_come_due_on_this_members_clock_is_dropped() {
    let (e, _d) = engine();
    let peer = NodeId::generate();
    e.note_own_stamps_refused(peer, 5_000);
    assert!(is_refused(e.create_collection("shop", "a").map(|_| ())).is_some(), "premise");

    e.set_clock_offset_for_tests(10_000); // ten seconds pass on this member's clock

    e.create_collection("shop", "b").expect("the due time passed");
    assert_eq!(e.own_stamps_refused_for_ms(), 0);
}

#[test]
fn only_the_peer_that_cleared_is_cleared() {
    let (e, _d) = engine();
    let (one, other) = (NodeId::generate(), NodeId::generate());
    e.note_own_stamps_refused(one, 600_000);
    e.note_own_stamps_refused(other, 300_000);
    e.note_own_stamps_not_refused(one);
    let refusal =
        is_refused(e.create_collection("shop", "a").map(|_| ())).expect("the other still refuses");
    assert!(
        matches!(refusal, OwnStampRefusal::PeerRefused { peer, .. } if peer == other),
        "{refusal:?}"
    );
}
