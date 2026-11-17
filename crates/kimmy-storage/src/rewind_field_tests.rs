//! A rewind is refused for a store that holds a `field` collection, before anything is read or written: it
//! would put back a body with no merge state, which the collection cannot read.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy};

use crate::Engine;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

#[test]
fn a_rewind_over_a_store_with_a_field_collection_is_refused_and_writes_nothing() {
    let (e, _d) = engine();
    let c = e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = e.insert(&c, doc! { "_id": 1, "a": 1 }).unwrap();
    let (before_version, before) = e.get_versioned(&c, &x).unwrap().unwrap();
    // A target before the document existed: a rewind would remove it.
    let err = e.rewind_to(Hlc::new(1, 0)).expect_err("a rewind over a field collection is refused");
    assert!(
        err.to_string().contains("merge policy `field` (shop.orders)"),
        "the refusal names the collection: {err}"
    );
    let (after_version, after) =
        e.get_versioned(&c, &x).unwrap().expect("the document is still there");
    assert_eq!((before_version, before), (after_version, after), "the refusal wrote nothing");
    assert!(e.rewinds().unwrap().is_empty(), "and left no mark of a rewind");
}
