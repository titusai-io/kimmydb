//! The order of the fields one update creates at a level (q7s2u, q7s2v, q7s2w: 490 cases at the top
//! level, in `o.`, in `a.0.` and in an upsert's created document). It never depends on the order
//! the update lists them in. Two names compare by bytes, except when both begin with a byte
//! `1` to `9`: then the shorter name comes first, and names of one length compare by bytes.
//!
//! `data/new_key_order.tsv` is built from the recordings by `data/new_key_order_gen.py` (the command line
//! is in its header).

mod support;

use bson::{Bson, Document};
use kimmy_core::path::new_key_order;
use kimmy_query::update;
use proptest::prelude::*;
use std::cmp::Ordering;
use support::json;

const NOW: i64 = 1_700_000_000_000;

fn document(text: &str) -> Document {
    match json(text) {
        Bson::Document(doc) => doc,
        other => panic!("not a document: {other}"),
    }
}

/// The update applied to a stored document, or to the seed of an upsert.
fn applied(update_doc: &Document, mut doc: Document, upsert: bool) -> Result<Document, String> {
    let parsed = update::parse(update_doc).map_err(|e| e.to_string())?;
    if upsert {
        update::apply_on_insert(&parsed, &mut doc, NOW).map_err(|e| e.to_string())?;
    } else {
        update::apply(&parsed, &mut doc, NOW).map_err(|e| e.to_string())?;
    }
    Ok(doc)
}

/// The rows this build refuses until a later commit of the series: `case`, a tab, and the commit
/// that removes the row from this list.
fn exceptions() -> std::collections::HashMap<&'static str, &'static str> {
    include_str!("data/new_key_order_exceptions.txt")
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|line| line.split_once('\t').expect("a case, a tab and the commit"))
        .collect()
}

#[test]
fn every_recorded_update_stores_the_recorded_order() {
    let table = include_str!("data/new_key_order.tsv");
    let exceptions = exceptions();
    let mut rows = 0;
    let mut failures = Vec::new();
    let mut still_refused = Vec::new();
    for row in table.lines() {
        let [case, upsert, before, update_doc, after] = row.split('\t').collect::<Vec<_>>()[..]
        else {
            panic!("a row of five columns: {row}")
        };
        rows += 1;
        let got = applied(&document(update_doc), document(before), upsert == "1");
        // The text of a document keeps the order of its fields; `Document`'s equality does not.
        let failure = match got {
            Ok(got) if got.to_string() == document(after).to_string() => None,
            Ok(got) => Some(format!(
                "q7s2{case}: {update_doc}\n   ours     {got}\n   recorded {}",
                document(after)
            )),
            Err(e) => Some(format!("q7s2{case}: {update_doc}: refused: {e}")),
        };
        match (failure, exceptions.contains_key(case)) {
            (None, false) => {}
            (Some(failure), false) => failures.push(failure),
            (Some(_), true) => still_refused.push(case),
            (None, true) => failures.push(format!("q7s2{case} is in the exceptions and passes")),
        }
    }
    for case in exceptions.keys() {
        assert!(
            still_refused.contains(case),
            "an exception for a row that is not in the table: {case}"
        );
    }
    assert!(rows > 450, "{rows} rows");
    assert!(
        failures.is_empty(),
        "{} of {rows} rows differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn the_recorded_orders_are_the_comparator() {
    // The groups of q7s2v and q7s2w, each stored order, as the comparator sorts them.
    for names in [
        vec!["2", "10", "1a"],
        vec!["010", "2", "10", "1a"],
        vec!["1a", "100"],
        vec!["9x", "12345"],
        vec!["1a", "20", "100"],
        vec!["2b", "1ab"],
        vec!["1a", "2b"],
        vec!["2", "1a", "1b"],
        vec!["20", "1ab"],
        vec!["10", "2a"],
        vec!["9", "1a", "100", "10a"],
        vec!["0a", "1"],
        vec!["0", "0a"],
        vec!["00", "0a"],
        vec!["-a", "0"],
        vec!["0", "1a"],
        vec!["#h", "010"],
        vec!["010", "1a"],
        vec!["0", "010"],
        vec!["-a", "1a"],
        vec!["1", "é"],
        vec!["1", "9z", "A", "a"],
        vec!["9", ":x"],
        vec!["/x", "1"],
        vec!["01", "1"],
        vec!["0", "00"],
        vec!["001", "01", "1"],
        vec!["01", "1", "2", "a"],
        vec!["#h", "2", "10"],
        vec!["5", "4294967294", "4294967295"],
        vec!["7", "18446744073709551616"],
        vec!["2", "99999999999999999999999"],
        // q7s4k (h), cases 129 to 140: the length counts bytes (`1é` is three).
        vec!["99", "1é"],
        vec!["100", "1é"],
        vec!["1ab", "1é"],
        vec!["9", "1é"],
        vec!["10", "1é"],
        vec!["1a", "1é"],
    ] {
        // Written in the order the recording lists them, and in reverse: one stored order.
        let mut sorted = names.clone();
        sorted.sort_by(|a, b| new_key_order(a, b));
        let mut reversed: Vec<&str> = names.iter().rev().copied().collect();
        reversed.sort_by(|a, b| new_key_order(a, b));
        assert_eq!(sorted, reversed, "{names:?}");
    }
    // The orders the recordings show, spelled out.
    let sorted = |mut v: Vec<&'static str>| {
        v.sort_by(|a, b| new_key_order(a, b));
        v
    };
    assert_eq!(sorted(vec!["1a", "10", "2"]), vec!["2", "10", "1a"]);
    assert_eq!(sorted(vec!["1a", "10", "2", "010"]), vec!["010", "2", "10", "1a"]);
    assert_eq!(sorted(vec!["-a", "1"]), vec!["-a", "1"]);
    assert_eq!(sorted(vec!["1a", "100"]), vec!["1a", "100"]);
    assert_eq!(sorted(vec!["10a", "100", "1a", "9"]), vec!["9", "1a", "100", "10a"]);
    assert_eq!(sorted(vec!["001", "01", "1"]), vec!["001", "01", "1"]);
}

fn name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[-0-9a-zA-Z/:#é]{1,6}").unwrap()
}

proptest! {
    #[test]
    fn the_order_is_antisymmetric_and_transitive(a in name(), b in name(), c in name()) {
        prop_assert_eq!(new_key_order(&a, &b), new_key_order(&b, &a).reverse());
        prop_assert_eq!(new_key_order(&a, &a), Ordering::Equal);
        if new_key_order(&a, &b).is_le() && new_key_order(&b, &c).is_le() {
            prop_assert!(new_key_order(&a, &c).is_le(), "{a:?} {b:?} {c:?}");
        }
        // Equal only when the names are the same.
        prop_assert_eq!(new_key_order(&a, &b) == Ordering::Equal, a == b);
    }

    #[test]
    fn the_stored_order_is_the_same_for_every_listing_order(
        names in proptest::collection::btree_set(name(), 2..6),
        seed in any::<u64>(),
    ) {
        let names: Vec<String> =
            names.into_iter().filter(|n| n != "_id" && n != "k").collect();
        prop_assume!(names.len() >= 2);
        let stored_in = |order: &[String]| {
            let mut set = Document::new();
            for n in order {
                set.insert(n.clone(), 1);
            }
            let update_doc = bson::doc! { "$set": set };
            let over = bson::doc! { "_id": 1, "k": 1 };
            applied(&update_doc, over, false).unwrap().keys().cloned().collect::<Vec<_>>()
        };
        let mut shuffled = names.clone();
        // A deterministic shuffle by the seed.
        let mut state = seed | 1;
        for i in (1..shuffled.len()).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            shuffled.swap(i, (state >> 33) as usize % (i + 1));
        }
        let a = stored_in(&names);
        let b = stored_in(&shuffled);
        prop_assert_eq!(&a, &b, "{:?} then {:?}", names, shuffled);
        // And it is the comparator's order after the fields that were there.
        let mut want = names.clone();
        want.sort_by(|x, y| new_key_order(x, y));
        let mut expected = vec!["_id".to_string(), "k".to_string()];
        expected.extend(want);
        prop_assert_eq!(a, expected);
    }
}
