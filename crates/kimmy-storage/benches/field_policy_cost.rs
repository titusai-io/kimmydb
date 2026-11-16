//! What the `field` merge policy costs, against `document` in the same build (L34-B B2-1, after
//! Q7 Step 2): stored bytes per document, the size of an oplog entry, and the latency of an
//! insert, a replace that changes one field, a delete and a point read, at 5, 20 and 100 fields.
//!
//! The two policies are the same engine and the same table; only the collection's
//! `merge_policy` differs, so no feature is needed and the comparison is the one the design asks
//! for. The byte figures print once before the timings (they do not vary from run to run).
//!
//! Run: `cargo bench -p kimmy-storage --bench field_policy_cost`.

use std::hint::black_box;

use bson::{Bson, Document, doc};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kimmy_core::{DocId, Hlc, MergePolicy};
use kimmy_storage::{CollectionMeta, Engine, WalkScope};

const WIDTHS: [usize; 3] = [5, 20, 100];

/// A document of `n` small fields: a string, an integer or a small sub-document in turn.
fn document(id: i64, n: usize) -> Document {
    let mut d = doc! { "_id": id };
    for i in 0..n {
        let v: Bson = match i % 3 {
            0 => Bson::String(format!("value-{id}-{i}")),
            1 => Bson::Int64(id * 1000 + i as i64),
            _ => Bson::Document(doc! { "a": i as i32, "b": "x" }),
        };
        d.insert(format!("f{i:03}"), v);
    }
    d
}

fn open(policy: MergePolicy) -> (Engine, CollectionMeta, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    let coll = engine.create_collection_with_policy("bench", "c", policy, None).unwrap();
    (engine, coll, dir)
}

/// Stored bytes of the file and of the oplog entries' `body` and `meta`, for `docs` documents.
fn print_bytes() {
    println!(
        "{:<9} {:>6} {:>12} {:>12} {:>12}",
        "policy", "fields", "file B/doc", "body B/entry", "meta B/entry"
    );
    for (name, policy) in [("document", MergePolicy::Document), ("field", MergePolicy::Field)] {
        for n in WIDTHS {
            let (engine, coll, _dir) = open(policy);
            let empty = engine.storage_bytes();
            let docs = 500i64;
            for id in 0..docs {
                engine.insert(&coll, document(id, n)).unwrap();
            }
            let entries: Vec<_> = engine
                .read_oplog_from(Hlc::ZERO, 100_000, WalkScope::Background)
                .unwrap()
                .into_iter()
                .filter(|e| e.collection == coll.id && e.kind.is_document())
                .collect();
            let body: usize = entries.iter().map(|e| e.body.as_ref().map_or(0, Vec::len)).sum();
            let meta: usize = entries.iter().map(|e| e.meta.as_ref().map_or(0, Vec::len)).sum();
            let each = entries.len().max(1);
            println!(
                "{name:<9} {n:>6} {:>12} {:>12} {:>12}",
                (engine.storage_bytes().saturating_sub(empty)) / docs as u64,
                body / each,
                meta / each
            );
        }
    }
}

fn policies() -> [(&'static str, MergePolicy); 2] {
    [("document", MergePolicy::Document), ("field", MergePolicy::Field)]
}

fn bench_insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert");
    for (name, policy) in policies() {
        for n in WIDTHS {
            let (engine, coll, _dir) = open(policy);
            let mut id = 0i64;
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    id += 1;
                    engine.insert(&coll, black_box(document(id, n))).unwrap()
                });
            });
        }
    }
    g.finish();
}

fn bench_replace_one_field(c: &mut Criterion) {
    let mut g = c.benchmark_group("replace_one_field");
    for (name, policy) in policies() {
        for n in WIDTHS {
            let (engine, coll, _dir) = open(policy);
            engine.insert(&coll, document(1, n)).unwrap();
            let id = DocId::Int64(1);
            let mut k = 0i64;
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    k += 1;
                    let mut d = document(1, n);
                    d.insert("f000", format!("changed-{k}"));
                    engine.replace(&coll, &id, black_box(d), false).unwrap()
                });
            });
        }
    }
    g.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut g = c.benchmark_group("point_read");
    for (name, policy) in policies() {
        for n in WIDTHS {
            let (engine, coll, _dir) = open(policy);
            engine.insert(&coll, document(1, n)).unwrap();
            let id = DocId::Int64(1);
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| black_box(engine.get(&coll, &id).unwrap()));
            });
        }
    }
    g.finish();
}

fn bench_delete_and_insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("delete_then_insert");
    for (name, policy) in policies() {
        for n in WIDTHS {
            let (engine, coll, _dir) = open(policy);
            engine.insert(&coll, document(1, n)).unwrap();
            let id = DocId::Int64(1);
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    engine.delete(&coll, &id).unwrap();
                    engine.insert(&coll, document(1, n)).unwrap()
                });
            });
        }
    }
    g.finish();
}

fn all(c: &mut Criterion) {
    print_bytes();
    bench_insert(c);
    bench_replace_one_field(c);
    bench_get(c);
    bench_delete_and_insert(c);
}

criterion_group!(benches, all);
criterion_main!(benches);
