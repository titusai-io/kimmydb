//! The resume skip and a member's own loss entries (Q7 Step 4, C8): the skip of ADR-173 passes over what the issuing stream had delivered,
//! and a token ANOTHER member issued says nothing about this member's own loss entries (its stream sends only its own), so they are not
//! passed over; a token this member issued itself skips them as it skips any entry.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, ResumeToken, VersionVector};

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

/// The stream of `a` resumed from `token`: the loss events it tells.
async fn told_after(a: &Engine, coll: &CollectionMeta, token: ResumeToken) -> Vec<Arc<OplogEntry>> {
    let mut stream = a
        .watch(
            WatchScope::Collection(coll.id),
            WatchOptions { resume_after: Some(token), ..Default::default() },
        )
        .expect("the token resumes");
    loss_events(&drain(a, &mut stream).await)
}

/// A vector that says the stream that issued the token had sent everything of `a`'s up to far in the future.
fn delivered_everything_of(a: &Engine) -> VersionVector {
    let mut delivered = VersionVector::new();
    delivered.insert(a.origin(), Hlc::new(crate::physical_now_ms() + 10_000_000, 0));
    delivered
}

/// ADR-173's skip passes over what the issuing stream had delivered. A token ANOTHER member issued says nothing about the events of this
/// member's own loss entries (its stream sends only its own), so they are not passed over.
#[tokio::test]
async fn an_own_loss_entry_is_told_after_a_token_another_member_issued_whatever_its_vector_covers()
{
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    let own: Vec<_> = own_loss_entries(&a).iter().map(|e| e.stamp).collect();
    assert!(!own.is_empty(), "premise");
    let first = everything(&a).into_iter().find(|e| e.kind.is_document()).unwrap();
    let foreign = ResumeToken::issued(first.stamp, b.origin(), delivered_everything_of(&a));

    let told = told_after(&a, &ca, foreign).await;

    let mut got: Vec<_> = told.iter().map(|e| e.stamp).collect();
    got.sort();
    let mut want = own;
    want.sort();
    assert_eq!(
        got, want,
        "the own loss entries are told, though the foreign vector covers their stamps"
    );
}

/// A token this member issued itself skips what it delivered, as it skips any entry: otherwise every resume would replay the member's own
/// loss history.
#[tokio::test]
async fn an_own_loss_entry_is_skipped_after_a_token_this_member_issued_that_covers_it() {
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    assert!(!own_loss_entries(&a).is_empty(), "premise");
    let first = everything(&a).into_iter().find(|e| e.kind.is_document()).unwrap();
    let own_token = ResumeToken::issued(first.stamp, a.origin(), delivered_everything_of(&a));

    let told = told_after(&a, &ca, own_token).await;

    assert!(told.is_empty(), "its own vector covers them: they were delivered");
}
