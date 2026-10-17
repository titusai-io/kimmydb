//! The write costs of design §4.3 (ADR-238), measured on the writer-hold path,
//! with decimals a writer chose against the key encoder's first table tier.
//!
//! A coefficient can be chosen (from the continued fraction of a table entry)
//! so that the first tier cannot settle its key; before the second tier, each
//! such value cost the exact path, 80 µs at `e = −6176`, and a document of 999
//! of them held the writer about 160 ms per index. These run the three shapes
//! the design bounds, in a release build, with crafted and with random values,
//! and read the hold the engine itself records:
//!
//! ```text
//! cargo test -p kimmy-storage --release --lib decimal_cost_tests -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `kimmy-core`'s `encode_cost_ceilings` measures the encoder's share alone.

use std::time::Duration;

use bson::{Bson, Document, doc};
use kimmy_core::{Hlc, IndexField};
use num_bigint::BigUint;

use crate::engine::{Engine, WriterHolder};
use crate::hold_meter::test_hooks::{LAST_HOLD, LastHold};
use crate::walk::WalkScope;

/// `5^e`'s first-tier table entry, as `kimmy-core`'s key encoder builds it:
/// the top 192 bits of `5^e`, or of `⌊2^14597 / 5^q⌋` for `e = −q`.
fn first_tier_entry(e: i16) -> BigUint {
    let top = |v: BigUint| {
        let excess = v.bits() as i64 - 192;
        if excess >= 0 { v >> excess as usize } else { v << (-excess) as usize }
    };
    let five = BigUint::from(5u32).pow(u32::from(e.unsigned_abs()));
    if e >= 0 { top(five) } else { top((BigUint::from(1u32) << 14_597usize) / five) }
}

/// `n` coefficients at an extreme exponent whose product with the first-tier
/// entry lands within `c` of a carry (`kimmy-core`'s `must_defer`): the
/// denominators of the continued fraction of `entry / 2^K`, their multiples,
/// and the semiconvergents between them. At these exponents no expansion
/// ends, so every one of them is a value the first tier hands on.
fn crafted(e: i16, n: usize) -> Vec<u128> {
    assert!(e <= -49 || e >= 104, "no expansion ends at {e}");
    let t = first_tier_entry(e);
    let limit = BigUint::from(9_999_999_999_999_999_999_999_999_999_999_999u128);
    let one = || BigUint::from(1u32);
    let mut out = std::collections::BTreeSet::new();
    for k in 64u32..=177 {
        // Continued-fraction denominators of `t / 2^k`.
        let (mut num, mut den) = (one() << k, &t % (one() << k));
        let (mut previous, mut current) = (BigUint::from(0u32), one());
        let mut qs = vec![current.clone()];
        while den != BigUint::from(0u32) && current <= limit {
            let a = &num / &den;
            (num, den) = (den.clone(), &num % &den);
            (previous, current) = (current.clone(), a * &current + previous);
            qs.push(current.clone());
        }
        for pair in qs.windows(2) {
            for j in 1u32..=49 {
                for c in [&pair[1] * j, &pair[1] * j + &pair[0]] {
                    if c > limit || c == BigUint::from(0u32) {
                        continue;
                    }
                    let p = &c * &t;
                    let cut = p.bits() - 128;
                    if (one() << cut) - (&p % (one() << cut)) < c {
                        let digits = c.to_u64_digits();
                        out.insert(
                            u128::from(digits[0])
                                | (u128::from(*digits.get(1).unwrap_or(&0)) << 64),
                        );
                    }
                }
            }
        }
    }
    assert!(out.len() >= n, "only {} crafted at e = {e}", out.len());
    // In no particular order, as random values arrive: in ascending order their
    // keys would append to the index and cost it less than random ones do.
    let mut out: Vec<u128> = out.into_iter().take(n).collect();
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for i in (1..out.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.swap(i, (state % (i as u64 + 1)) as usize);
    }
    out
}

/// An array of 999 decimals, half at `e = −6176` and half at `e = +6111`:
/// the longest array an index keys (each element and the whole array, at most
/// 1,000 keys a document) rather than files unkeyed.
fn values(chosen: bool) -> Vec<Bson> {
    static CRAFTED: std::sync::OnceLock<(Vec<u128>, Vec<u128>)> = std::sync::OnceLock::new();
    let (low, high) = if chosen {
        CRAFTED.get_or_init(|| (crafted(-6176, 500), crafted(6111, 499))).clone()
    } else {
        let mut state = 0x9E37_79B9_7F4A_7C15u128;
        let mut next = move || {
            state = state.wrapping_mul(0x2D99_787C_C2A6_2D03_A3A9_F2B6_3E1D_C2B9).wrapping_add(1);
            1 + (state >> 15) % 9_999_999_999_999_999_999_999_999_999_999_999
        };
        ((0..500).map(|_| next()).collect(), (0..499).map(|_| next()).collect())
    };
    let pack = |c: u128, e: i16| Bson::Decimal128(kimmy_core::decimal::pack(false, c, e));
    low.into_iter().map(|c| pack(c, -6176)).chain(high.into_iter().map(|c| pack(c, 6111))).collect()
}

fn document(id: i64, values: &[Bson]) -> Document {
    doc! { "_id": id, "a": Bson::Array(values.to_vec()) }
}

/// An engine with `app.docs` and, if asked, the two indexes the design's
/// ceilings name, both over the array.
fn engine(indexes: bool) -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    engine.create_collection("app", "docs").unwrap();
    if indexes {
        for field in [IndexField::ascending("a"), IndexField::descending("a")] {
            engine.create_index("app", "docs", vec![field], false, None).unwrap();
        }
    }
    (engine, dir)
}

fn last_hold() -> LastHold {
    LAST_HOLD.with(|h| h.take()).expect("the write recorded its hold")
}

/// One measured hold: its wall time and the writer thread's CPU time over it.
#[derive(Clone, Copy, Debug)]
struct Held {
    holder: WriterHolder,
    wall: Duration,
    cpu: Duration,
}

impl Held {
    fn of(hold: LastHold) -> Self {
        let cpu = hold.cpu_over_hold.expect("this platform reads a thread's CPU time");
        Held { holder: hold.holder, wall: hold.held, cpu }
    }

    /// The least of several runs, field by field: the hold with the least
    /// interference from the disk and from other processes.
    fn least(runs: impl IntoIterator<Item = Held>) -> Held {
        runs.into_iter()
            .reduce(|a, b| Held {
                holder: a.holder,
                wall: a.wall.min(b.wall),
                cpu: a.cpu.min(b.cpu),
            })
            .unwrap()
    }
}

/// The hold of each shape, with or without the two indexes:
/// `[one document, a 2 MiB insert_many, a replicated run of 1,024]`.
fn holds(values: &[Bson], indexes: bool) -> [Held; 3] {
    // A client sends JSON: count the documents in a 2 MiB body in relaxed
    // extended JSON, where a decimal is `{"$numberDecimal": "…"}`.
    let json = Bson::Document(document(0, values)).into_relaxed_extjson().to_string();
    let per_body = (2usize << 20) / json.len();

    let single = Held::least((0..3).map(|_| {
        let (engine, _dir) = engine(indexes);
        let coll = engine.get_collection("app", "docs").unwrap();
        engine.insert(&coll, document(1, values)).unwrap();
        Held::of(last_hold())
    }));
    let many = Held::least((0..3).map(|_| {
        let (engine, _dir) = engine(indexes);
        let coll = engine.get_collection("app", "docs").unwrap();
        engine
            .insert_many(&coll, (0..per_body as i64).map(|i| document(i, values)).collect())
            .unwrap();
        Held::of(last_hold())
    }));

    // A replicated run: 1,024 documents written on one member, applied on
    // another in one batch (one transaction, ADR-119). The schema goes first,
    // so the batch measured is the documents alone.
    let (source, _dir) = engine(indexes);
    let (target, _target_dir) = {
        let dir = tempfile::tempdir().unwrap();
        (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
    };
    let schema = source.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    target.apply_batch(&schema).unwrap();
    let mark = schema.last().unwrap().stamp.hlc;
    let coll = source.get_collection("app", "docs").unwrap();
    for chunk in (0..1024i64).collect::<Vec<_>>().chunks(64) {
        source.insert_many(&coll, chunk.iter().map(|&i| document(i, values)).collect()).unwrap();
    }
    // The window starts at the mark itself; keep the documents.
    let mut run = source.entries_for_peer(mark, 10_000, WalkScope::Background).unwrap().entries;
    run.retain(|entry| entry.doc_id.is_some());
    assert_eq!(run.len(), 1024, "the run is the documents alone");
    LAST_HOLD.with(|h| h.set(None));
    target.apply_batch(&run).unwrap();
    let applied = Held::of(last_hold());

    println!("  ({per_body} documents in a 2 MiB body)");
    [single, many, applied]
}

/// Crafted decimals hold the writer no longer than random ones in each shape,
/// and every figure is printed beside the design's starting ceilings.
///
/// The starting ceilings (5 ms, 0.5 s and 5 s per index) are not met by any
/// value on this path: one document of 999 random extreme decimals holds the
/// writer several milliseconds per index here, which is the index's 1,000
/// entries (one of them the whole array, about 20 KB) and the commit, not the
/// encoding, whose share `kimmy-core`'s `encode_cost_ceilings` measures at
/// about 0.08 ms. So the assertion is the one the second tier makes true: a
/// writer who chooses the coefficients gains nothing over random ones. ADR-238
/// states the measured holds.
#[test]
#[ignore]
fn crafted_decimals_hold_the_writer_no_longer_than_random_ones() {
    let shapes = ["one document of 999", "a 2 MiB insert_many", "a replicated run of 1,024"];
    let ceilings = [Duration::from_millis(5), Duration::from_millis(500), Duration::from_secs(5)];
    let (random, crafted) = (values(false), values(true));
    // Build the key encoder's tables (both tiers) before anything is timed.
    for value in random.iter().chain(&crafted) {
        kimmy_core::keyenc::encode(value).unwrap();
    }
    let mut indexed = Vec::new();
    for (label, values) in [("random", &random), ("crafted", &crafted)] {
        println!("{label} extreme decimals:");
        let bare = holds(values, false);
        let with = holds(values, true);
        for i in 0..3 {
            println!(
                "  {}: held {:?} (CPU {:?}) as {:?} with two indexes, {:?} (CPU {:?}) without; \
                 at most {:?} per index (design starting ceiling {:?})",
                shapes[i],
                with[i].wall,
                with[i].cpu,
                with[i].holder,
                bare[i].wall,
                bare[i].cpu,
                with[i].wall / 2,
                ceilings[i]
            );
        }
        indexed.push(with);
    }
    for i in 0..3 {
        let (random, crafted) = (indexed[0][i].cpu, indexed[1][i].cpu);
        // A quarter over, plus a few milliseconds of noise.
        assert!(
            crafted <= random + random / 4 + Duration::from_millis(5),
            "{}: crafted {crafted:?} against random {random:?} of CPU over the hold",
            shapes[i]
        );
    }
}
