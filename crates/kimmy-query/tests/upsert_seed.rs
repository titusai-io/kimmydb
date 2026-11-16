//! The document an upsert inserts (q7s2x, 308 cases): the filter's fields first, in plain byte order
//! at every depth (`10, 1a, 2`; inside a created sub-document `o: {10, 1a, 2}`), whatever the order
//! the filter lists them in and whatever form carries them (`$eq`, a one-value `$in`, `$and`, a
//! one-branch `$or`); then the update's new fields at that level by the rule of `new_key_order`
//! (filter `{10, 9}` with `$set` `{2, 1a, 100}` stores `10, 9, 2, 1a, 100`), `$setOnInsert` the same.
//!
//! The seed is built as the executor builds it for an upserting `findAndModify` (`exec.rs`, the
//! `upsert_doc` of `find_and_modify`): `implied_equalities` and then the top-level fields in name
//! order with `_id` first, and the update applied with `apply_on_insert`.
//!
//! `data/upsert_seed.tsv` is built from the recording by `data/upsert_seed_gen.py`.

mod support;

use bson::{Bson, Document};
use kimmy_query::{filter, seed, update};
use support::json;

const NOW: i64 = 1_700_000_000_000;

fn document(text: &str) -> Document {
    match json(text) {
        Bson::Document(doc) => doc,
        other => panic!("not a document: {other}"),
    }
}

fn inserted(query: &Document, update_doc: &Document) -> Result<Document, String> {
    let parsed = filter::parse(query).map_err(|e| e.to_string())?;
    let mut doc = Document::new();
    seed::implied_equalities(&parsed, &mut doc, &mut Vec::new())?;
    let mut fields: Vec<(String, Bson)> = std::mem::take(&mut doc).into_iter().collect();
    fields.sort_by(|a, b| (a.0 != "_id", &a.0).cmp(&(b.0 != "_id", &b.0)));
    doc.extend(fields);
    let update = update::parse(update_doc).map_err(|e| e.to_string())?;
    update::apply_on_insert(&update, &mut doc, NOW).map_err(|e| e.to_string())?;
    Ok(doc)
}

#[test]
fn every_recorded_upsert_inserts_the_recorded_order() {
    let mut rows = 0;
    for row in include_str!("data/upsert_seed.tsv").lines() {
        let [case, query, update_doc, after] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of four columns: {row}")
        };
        let got = inserted(&document(query), &document(update_doc))
            .unwrap_or_else(|e| panic!("q7s2{case}: refused: {e}"));
        // The text of a document keeps the order of its fields; `Document`'s equality does not.
        assert_eq!(
            got.to_string(),
            document(after).to_string(),
            "q7s2{case}: filter {query}, update {update_doc}"
        );
        rows += 1;
    }
    assert_eq!(rows, 308, "the generator wrote 308 cases");
    println!("upsert_seed: {rows} cases");
}
