//! The register growth bench (ADR-272; Q6.6, the resolve-row unit). An ignored test, run by hand or through the test queue,
//! that measures what a name refreshed by `$out` many times costs to resolve as its register grows. `resolve` reads every event of the
//! name (and a collection read for each physical the events name) on every request for a swapped name, so its cost, and a read through
//! it, are expected to grow with the number of swaps `N`; the resolve row (one stored row per name holding the settle pass's decision)
//! is to flatten it. The same file runs unchanged before and after, so the two TSVs diff.
//!
//! ```text
//! KIMMY_BENCH_SWAPS=0,10,1000,10000,100000 KIMMY_BENCH_OUT=/tmp/bench.tsv \
//!   cargo test -p kimmy-storage --release --lib register_growth_bench -- --ignored --nocapture --test-threads=1
//! ```
//!
//! One fresh engine per `N`, one name `app.orders`. The register is built by the register's own door (`register::write_stage_row`,
//! `register::put`), in transactions of a few thousand rows, for the `N - 1` stages that are long buried, and by the engine's own routes
//! for the last: `create_stage`, its ten documents, its binding and the settle pass, so the name resolves to a stage that is alive. The
//! route is printed with every line (`door`; `none` for `N = 0`, an ordinary collection with no event). Per `N` and per operation:
//! 20 warm-up calls and 200 timed ones, in nanoseconds.
//!
//! The columns of the TSV: `n, op, route, median_ns, p90_ns, p99_ns, max_ns, rows, bytes, build_s`, where `rows` is the REGISTER,
//! REGISTER_INFO and REGISTER_STAGES row counts joined by `/` and `bytes` is the store file's size.

use std::io::Write;
use std::time::Instant;

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, RegisterEvent, StageInfo, StageMeta, Stamp};
use redb::{ReadableDatabase, ReadableTableMetadata};

use crate::{Engine, physical_now_ms, tables};

const DB: &str = "app";
const NAME: &str = "orders";
const WARM_UP: usize = 20;
const SAMPLES: usize = 200;
/// Rows written by one transaction of the build.
const CHUNK: u64 = 5_000;

fn stage_name(k: u64) -> String {
    format!("{NAME}{}{k:08x}", kimmy_core::swap::STAGE_NAME_MARK)
}

fn ns_of(f: &mut dyn FnMut()) -> u64 {
    let start = Instant::now();
    f();
    start.elapsed().as_nanos() as u64
}

/// `(median, p90, p99, max)` of `samples`, which it sorts.
fn stats(samples: &mut [u64]) -> (u64, u64, u64, u64) {
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    (at(0.5), at(0.9), at(0.99), samples[samples.len() - 1])
}

/// Build `n` swaps of `orders`: `n - 1` buried stages the register names, then the last, alive, with ten documents. Returns the route.
fn build(m: &Engine, n: u64) -> &'static str {
    if n == 0 {
        let coll = m.create_collection(DB, NAME).expect("the collection is made");
        for i in 0..10i64 {
            m.insert(&coll, doc! { "_id": i, "v": i }).expect("a document");
        }
        return "none";
    }
    let node = m.node_id();
    let epoch = m.epoch();
    // far enough in the past that every stamp below sorts under the last stage's, which the engine mints now
    let base = physical_now_ms().saturating_sub(2 * n + 1_000);
    let mut k = 1;
    while k < n {
        let end = (k + CHUNK).min(n);
        let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
        for j in k..end {
            let physical = stage_name(j);
            let created = Stamp::new(Hlc::new(base + 2 * j, 0), node, epoch);
            let info = StageInfo { deadline: Hlc::new(base + 2 * j + 30_000, 0) };
            crate::register::write_stage_row(&txn, DB, &physical, &StageMeta { created, info })
                .expect("the stage row");
            let bound = Stamp::new(Hlc::new(base + 2 * j + 1, 0), node, epoch);
            let put = crate::register::put(
                &txn,
                DB,
                NAME,
                bound,
                RegisterEvent::Bind { physical: physical.clone() },
            )
            .expect("the door");
            let stored = kimmy_core::register_state::Put::Stored;
            assert_eq!(put, stored, "premise: the binding is taken");
        }
        txn.commit().expect("the chunk commits");
        k = end;
    }
    let deadline = Hlc::new(physical_now_ms() + 600_000, 0);
    let last = stage_name(n);
    let stage = m.create_stage(DB, &last, MergePolicy::Document, deadline).expect("the last stage");
    for i in 0..10i64 {
        m.insert(&stage, doc! { "_id": i, "v": i }).expect("a document");
    }
    let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
    let bound = Stamp::new(Hlc::new(physical_now_ms() + 1, 0), node, epoch);
    let event = RegisterEvent::Bind { physical: last };
    let put = crate::register::put(&txn, DB, NAME, bound, event).expect("the door");
    assert_eq!(put, kimmy_core::register_state::Put::Stored, "premise: the last binding is taken");
    Engine::settle_register_in_txn(&txn, DB, NAME, Some(bound)).expect("the pass");
    txn.commit().expect("the last binding commits");
    "door"
}

/// The REGISTER, REGISTER_INFO and REGISTER_STAGES row counts.
fn register_rows(m: &Engine) -> (u64, u64, u64) {
    let txn = m.db().begin_read().expect("a read");
    let count = |table: redb::TableDefinition<'static, &'static [u8], &'static [u8]>| {
        txn.open_table(table).expect("the table opens").len().expect("it counts")
    };
    (count(tables::REGISTER), count(tables::REGISTER_INFO), count(tables::REGISTER_STAGES))
}

fn measure(m: &Engine, op: &str) -> (u64, u64, u64, u64) {
    let mut run: Box<dyn FnMut() + '_> = match op {
        "resolve" => Box::new(|| {
            m.resolve(DB, NAME).expect("the name resolves");
        }),
        "resolve_for_write" => Box::new(|| {
            m.resolve_for_write(DB, NAME).expect("the name resolves for a write");
        }),
        _ => Box::new(|| {
            let resolved = m.resolve(DB, NAME).expect("the name resolves");
            for i in 0..10i64 {
                let found = m.get(&resolved.meta, &DocId::Int64(i)).expect("the read");
                assert!(found.is_some(), "the document is there");
            }
        }),
    };
    for _ in 0..WARM_UP {
        run();
    }
    let mut samples: Vec<u64> = (0..SAMPLES).map(|_| ns_of(&mut *run)).collect();
    stats(&mut samples)
}

#[test]
#[ignore = "the register growth bench"]
fn the_register_growth_bench() {
    let swaps =
        std::env::var("KIMMY_BENCH_SWAPS").unwrap_or_else(|_| "0,10,1000,10000,100000".into());
    let mut out = std::env::var("KIMMY_BENCH_OUT")
        .ok()
        .map(|path| std::fs::File::create(path).expect("the output file is made"));
    let mut emit = |line: String| {
        eprintln!("{line}");
        if let Some(file) = out.as_mut() {
            writeln!(file, "{line}").expect("the output file is written");
        }
    };
    emit("n\top\troute\tmedian_ns\tp90_ns\tp99_ns\tmax_ns\trows\tbytes\tbuild_s".to_string());
    for n in swaps.split(',').map(|n| n.trim().parse::<u64>().expect("a count of swaps")) {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("kimmy.redb");
        let m = Engine::open(&path).expect("an engine opens");
        let started = Instant::now();
        let route = build(&m, n);
        let build_s = started.elapsed().as_secs_f64();
        // the premises: the register holds exactly `n` bindings, one stage is alive, the name answers from it, and a read returns the ten
        let (events, parked, stages) = register_rows(&m);
        assert_eq!((events, parked), (n, 0), "premise: the register holds exactly {n} events");
        assert_eq!(stages, n, "premise: it holds one stage row for each");
        let alive = m
            .list_collections(DB)
            .expect("the collections list")
            .iter()
            .filter(|c| kimmy_core::is_stage_name(&c.name))
            .count();
        assert_eq!(alive, usize::from(n > 0), "premise: exactly one stage is alive");
        let resolved = m.resolve(DB, NAME).expect("premise: the name resolves");
        let expected = if n == 0 { NAME.to_string() } else { stage_name(n) };
        assert_eq!(resolved.physical, expected, "premise: the name answers from the last stage");
        let found = (0..10i64)
            .filter(|i| m.get(&resolved.meta, &DocId::Int64(*i)).expect("the read").is_some())
            .count();
        assert_eq!(found, 10, "premise: a read returns the ten documents");
        let bytes = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
        for op in ["resolve", "resolve_for_write", "find"] {
            let (median, p90, p99, max) = measure(&m, op);
            emit(format!(
                "{n}\t{op}\t{route}\t{median}\t{p90}\t{p99}\t{max}\t{events}/{parked}/{stages}\t{bytes}\t{build_s:.3}"
            ));
        }
    }
}
