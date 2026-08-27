//! Concurrent and sustained writers — the two measurements M5 left open.
//!
//! The question is not "does concurrency make writes faster" — redb has one
//! writer, commits serialize, and nothing here can change that. The question
//! is what concurrency *costs*: whether four clients writing at once merely
//! share the single writer's throughput or actively lose some of it to
//! contention. The answer decides whether an operator with a slow bulk load
//! should parallelize it (harmless but pointless, or actively harmful?) and
//! what a sensible bulk-insert API can promise.
//!
//! `writers/1` doubles as the sustained-ingest baseline: back-to-back durable
//! commits from one client, which is what any client loop achieves today, and
//! the per-commit overhead a future batched API would amortize.

use std::hint::black_box;
use std::sync::Arc;

use bson::doc;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kimmy_storage::Engine;

/// Documents each writer inserts per iteration.
///
/// Large enough that thread spawn (~tens of microseconds) vanishes against
/// the commits it brackets (~milliseconds each), small enough that one
/// iteration stays under a second at every width.
const DOCS_PER_WRITER: usize = 10;

fn concurrent_writers(c: &mut Criterion) {
    writers_under(c, "writers", kimmy_storage::DurabilityClass::Durable);
}

/// The same measurement under `coalesced` durability (ADR-088): commits skip
/// their own fsync and share one per 5 ms window, so this is where the
/// single writer's flat line is expected to bend upward with concurrency.
fn coalesced_writers(c: &mut Criterion) {
    writers_under(c, "writers_coalesced", kimmy_storage::DurabilityClass::Coalesced);
}

fn writers_under(c: &mut Criterion, name: &str, class: kimmy_storage::DurabilityClass) {
    let mut group = c.benchmark_group(name);
    group.sample_size(10);

    for writers in [1usize, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements((writers * DOCS_PER_WRITER) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(writers), &writers, |b, &writers| {
            b.iter_custom(|iters| {
                // A fresh engine per timing run rather than per iteration:
                // opening redb costs more than the writes being measured, and
                // an engine that grows across iterations is exactly what a
                // sustained load looks like.
                let dir = tempfile::tempdir().unwrap();
                let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
                engine.set_durability(class, std::time::Duration::from_millis(5));
                engine.create_collection("bench", "docs").unwrap();
                let meta = engine.get_collection("bench", "docs").unwrap();

                let mut next_id = 0i64;
                let started = std::time::Instant::now();
                for _ in 0..iters {
                    let handles: Vec<_> = (0..writers)
                        .map(|_| {
                            let engine = Arc::clone(&engine);
                            let meta = meta.clone();
                            let base = next_id;
                            next_id += DOCS_PER_WRITER as i64;
                            std::thread::spawn(move || {
                                for i in 0..DOCS_PER_WRITER as i64 {
                                    let id = base + i;
                                    engine
                                        .insert(
                                            &meta,
                                            doc! {
                                                "_id": id,
                                                "sku": format!("SKU-{id:08}"),
                                                "qty": id % 500,
                                            },
                                        )
                                        .unwrap();
                                }
                            })
                        })
                        .collect();
                    for handle in handles {
                        handle.join().unwrap();
                    }
                }
                black_box(started.elapsed())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, concurrent_writers, coalesced_writers);
criterion_main!(benches);
