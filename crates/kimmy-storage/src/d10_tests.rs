//! D10 (ADR-272, decision 7; Q6.4 item 3): a member that knows of a register event that supersedes the physical a name answers
//! from refuses WRITES to the name, with an error the API answers `503 swap_in_progress`, until it has made a newer event visible.
//! The event may be in effect but unservable (a binding whose stage is not alive here) or known only ahead of its position. Reads
//! are not refused, a name that was never swapped is untouched, and a name this member sees absent answers as absent.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, StorageError, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

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

/// A member holding `orders` with a document and the creation of a stage of it, and the binding of that stage (not yet known to it).
fn world() -> (Engine, tempfile::TempDir, OplogEntry, Vec<tempfile::TempDir>) {
    let (a, da) = engine();
    a.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = OplogEntry {
        stamp: Stamp::new(Hlc::new(creation.stamp.hlc.wall_ms + 1, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .unwrap(),
        ),
        meta: None,
    };
    let (m, dm) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    m.apply_batch(std::slice::from_ref(&creation)).unwrap();
    (m, dm, bind, vec![da])
}

fn refused(e: Result<crate::Resolved, StorageError>) -> Option<(String, Stamp, bool)> {
    match e {
        Err(StorageError::NameSuperseded { logical, event, parked }) => {
            Some((logical, event, parked))
        }
        _ => None,
    }
}

#[test]
fn a_binding_known_ahead_of_its_position_refuses_writes_and_not_reads() {
    let (m, _dm, bind, _keep) = world();
    m.apply_batch(std::slice::from_ref(&bind)).unwrap(); // no window behind it: information
    assert_eq!(
        refused(m.resolve_for_write(DB, "orders")),
        Some(("orders".to_string(), bind.stamp, true)),
        "a write to the name is refused, naming the logical name, the event and that it is known ahead of its position"
    );
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, "orders", "a read is answered as ever");
}

#[test]
fn a_binding_in_effect_whose_stage_this_member_cannot_serve_refuses_writes() {
    let (m, _dm, bind, _keep) = world();
    // the stage is not alive here (buried), and the binding takes effect in a window: in effect, and unservable
    m.drop_collection_inner(DB, STAGE, None).unwrap();
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(
        m.resolve(DB, "orders").unwrap().physical,
        "orders",
        "premise: the name still answers from the identity"
    );
    assert_eq!(
        refused(m.resolve_for_write(DB, "orders")),
        Some(("orders".to_string(), bind.stamp, false)),
        "refused, naming the event as known"
    );
}

#[test]
fn the_refusal_ends_when_the_newer_event_is_visible() {
    let (m, _dm, bind, _keep) = world();
    m.apply_batch(std::slice::from_ref(&bind)).unwrap();
    assert!(
        refused(m.resolve_for_write(DB, "orders")).is_some(),
        "premise: refused while it is only information"
    );
    window(&m, std::slice::from_ref(&bind)); // the window releases it: in effect, and the stage is alive
    let resolved =
        m.resolve_for_write(DB, "orders").expect("the binding is visible: the write is answered");
    assert_eq!(resolved.physical, STAGE, "from the stage the name is bound to");
}

#[test]
fn a_name_that_was_never_swapped_and_an_absent_name_are_untouched() {
    let (m, _dm) = engine();
    m.create_collection(DB, "plain").unwrap();
    assert!(
        m.resolve_for_write(DB, "plain").is_ok(),
        "a name with no register event is never refused"
    );
    assert!(
        matches!(
            m.resolve_for_write(DB, "nothing"),
            Err(StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
        ),
        "an absent name answers as an absent collection"
    );
}

#[test]
fn the_refusal_names_what_the_member_is_waiting_for() {
    let (m, _dm, bind, _keep) = world();
    m.apply_batch(std::slice::from_ref(&bind)).unwrap();
    let text = m.resolve_for_write(DB, "orders").unwrap_err().to_string();
    assert!(
        text.starts_with("writes to orders are refused on this member until it has made the newer replacement of the collection visible"),
        "{text}"
    );
    assert!(text.contains("learned ahead of its position"), "{text}");
    assert!(text.ends_with("retry"), "{text}");
}
