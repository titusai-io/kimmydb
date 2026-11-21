//! The order of a member's own loss entry in its oplog (Q7 Step 4): after the entry that brought the record in, in the same transaction,
//! so a stream reads the change before the news of the loss and a token resumed at the change replays the loss. (Before, the own entry
//! was appended first, and its stamp could be below the entry it followed.)

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{ChangeEvent, CollectionMeta, Engine, WalkScope, WatchOptions, WatchScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(Duration::from_millis(20));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// `a` and `b` write field `x` of document 1 at once and take each other's entries: each derives the record of the write that lost
/// and mints an entry of its own for it, and each holds the other's entry too.
fn a_clash(a: &Engine, b: &Engine, ca: &CollectionMeta) {
    a.insert(ca, doc! { "_id": 1, "x": 0 }).unwrap();
    b.apply_batch(&everything(a)).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    a.apply_batch(&everything(b)).unwrap();
    b.apply_batch(&everything(a)).unwrap();
    a.apply_batch(&everything(b)).unwrap();
}

fn loss_entries(e: &Engine) -> Vec<OplogEntry> {
    everything(e).into_iter().filter(|x| x.kind == OpKind::LossRecord).collect()
}

fn own_loss_entries(e: &Engine) -> Vec<OplogEntry> {
    loss_entries(e).into_iter().filter(|x| x.stamp.node == e.node_id()).collect()
}

/// Every event the stream has now: the replay, then what the idle stream times out on.
async fn drain(e: &Engine, stream: &mut crate::ChangeStream) -> Vec<ChangeEvent> {
    let mut out = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(400), stream.next(e)).await
    {
        out.push(event);
    }
    out
}

fn loss_events(events: &[ChangeEvent]) -> Vec<Arc<OplogEntry>> {
    events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::Change { entry, .. } if entry.kind == OpKind::LossRecord => {
                Some(Arc::clone(entry))
            }
            _ => None,
        })
        .collect()
}

fn from_start(e: &Engine, coll: &CollectionMeta) -> crate::ChangeStream {
    e.watch(
        WatchScope::Collection(coll.id),
        WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
    )
    .unwrap()
}

/// The member tells the change, then the news of the loss it caused: the own loss entry is appended after the entry that brought the
/// record in, in the same transaction.
#[tokio::test]
async fn a_loss_is_told_after_the_change_that_caused_it() {
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    let mut stream = from_start(&a, &ca);
    let events = drain(&a, &mut stream).await;
    let kinds: Vec<(OpKind, bool)> = events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::Change { entry, .. } => {
                Some((entry.kind, entry.stamp.node == a.node_id()))
            }
            _ => None,
        })
        .collect();
    let b_write = kinds
        .iter()
        .position(|(kind, own)| *kind == OpKind::Replace && !own)
        .expect("premise: `b`'s write is told");
    let first_loss = kinds
        .iter()
        .position(|(kind, _)| *kind == OpKind::LossRecord)
        .expect("premise: a loss is told");
    assert!(first_loss > b_write, "the loss comes after the write that brought it: {kinds:?}");
}

/// The route of a record that was parked until its document got a row (a local insert folds it in): the insert is told first.
#[tokio::test]
async fn a_parked_record_folded_in_by_a_local_insert_is_told_after_the_insert() {
    let (a, b, ca, _d) = pair();
    let writer = WriterId { node: b.node_id(), epoch: b.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    let parked = OplogEntry {
        stamp: Stamp::new(Hlc::new(crate::physical_now_ms() + 1, 0), b.node_id(), b.epoch()),
        kind: OpKind::LossRecord,
        collection: ca.id,
        doc_id: Some(DocId::Int64(7)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(1)))),
        meta: None,
    };
    a.apply_batch(std::slice::from_ref(&parked)).unwrap();
    assert!(own_loss_entries(&a).is_empty(), "premise: parked, no entry of its own yet");
    later();

    a.insert(&ca, doc! { "_id": 7, "x": 1 }).unwrap();

    assert_eq!(
        own_loss_entries(&a).len(),
        1,
        "the record entered the state when the insert gave the document a row"
    );
    let mut stream = from_start(&a, &ca);
    let events = drain(&a, &mut stream).await;
    let order: Vec<OpKind> = events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::Change { entry, .. }
                if entry.kind != OpKind::LossRecord || entry.stamp.node == a.node_id() =>
            {
                Some(entry.kind)
            }
            _ => None,
        })
        .filter(|k| matches!(k, OpKind::Insert | OpKind::LossRecord))
        .collect();
    assert_eq!(
        order,
        [OpKind::Insert, OpKind::LossRecord],
        "the insert, then the news of the loss"
    );
}
