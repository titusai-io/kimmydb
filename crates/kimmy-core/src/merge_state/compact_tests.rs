//! Tests of the compact canonical form (`compact.rs`): round trips, canonicity and refusals,
//! and its size on the shapes of the L34-B1 benchmark.

use std::collections::BTreeSet;

use super::codec::MAX_VALUE_DEPTH;
use super::tests::{
    golden_170, golden_delete, golden_empty, golden_mixed, golden_obs, golden_small,
};
use super::*;
use crate::hlc::Hlc;
use crate::ids::NodeId;

fn wid(c: u8, epoch: u64) -> WriterId {
    WriterId { node: NodeId::from_bytes([c; 16]), epoch: Epoch::from_u64(epoch) }
}
fn p(x: &str) -> Path {
    Path::flat(x).unwrap()
}
fn fl(f: &[(&str, Value)]) -> Vec<(Path, Value)> {
    f.iter().map(|(k, v)| (p(k), v.clone())).collect()
}
fn opts() -> JoinOptions {
    JoinOptions::default()
}

/// Encode, decode, and check that the bytes are exactly what the decoded state encodes to.
fn round_trip(st: &MergeState) -> Vec<u8> {
    let enc = st.encode_compact();
    assert_eq!(&enc[..4], b"QMS\x02");
    assert_eq!(&enc[4..], st.compact_bytes());
    let back = MergeState::decode_compact(&enc).unwrap();
    assert_eq!(&back, st);
    assert_eq!(back.encode_compact(), enc, "encode(decode(b)) == b");
    enc
}

#[test]
fn the_golden_states_round_trip_and_are_much_smaller() {
    for (name, st) in [
        ("empty", golden_empty()),
        ("170", golden_170()),
        ("mixed", golden_mixed()),
        ("delete", golden_delete()),
        ("obs", golden_obs()),
        ("small", golden_small()),
    ] {
        let enc = round_trip(&st);
        let verbose = st.canonical_bytes().len();
        println!("{name}: compact {} B, verbose {verbose} B", enc.len() - 4);
        if name != "empty" {
            assert!(enc.len() * 2 < verbose, "{name}: {} against {verbose}", enc.len());
        }
    }
    // The empty state: nine counts and no table entry.
    assert_eq!(golden_empty().compact_bytes(), vec![0; 9]);
}

/// The compact hash and length of the five golden states, as the reference model's independent
/// encoder computes them (`merge_state_diff::golden_vectors_match_the_model` pins the same
/// numbers). The compact form is not stored or hashed by anything yet, so these are a pin of the
/// skeleton, not a released format.
#[test]
fn the_compact_golden_hashes_agree_with_the_reference_model() {
    let hex = |b: [u8; 16]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    for (name, st, want) in [
        ("empty", golden_empty(), ("e145d6a7daf371398c7b574cd9aa42d4", 9)),
        ("170", golden_170(), ("45e14c7b5dd32f32f7cf6543335f3fb1", 213)),
        ("mixed", golden_mixed(), ("855d32c60d7c369fb409ecf4d6b6331a", 340)),
        ("delete", golden_delete(), ("95a3e75ef534ce08fd975b1224a8af76", 233)),
        ("obs", golden_obs(), ("d785253ed2c10e27e148d2a7f2f251cd", 255)),
    ] {
        assert_eq!((hex(st.compact_hash()).as_str(), st.compact_bytes().len()), want, "{name}");
    }
}

#[test]
fn every_bson_type_and_odd_names_round_trip_in_the_compact_form() {
    let mut w = Writer::new(wid(b'A', 7), opts());
    let names = ["", "$set", "a.b", "é"];
    let vals = [
        Value::Unset,
        Value::Null,
        Value::Int32(-7),
        Value::Int64(1 << 40),
        Value::Double(-0.0),
        Value::Decimal128([0x30; 16]),
        Value::String("héllo".into()),
        Value::Bool(true),
        Value::Date(-1),
        Value::ObjectId([7; 12]),
        Value::Binary { subtype: 0x80, bytes: vec![0, 1, 255] },
        Value::Timestamp { time: u32::MAX, increment: 3 },
        Value::Regex { pattern: "^a.*".into(), options: "ix".into() },
        Value::Array(vec![Value::Int64(1), Value::Array(vec![Value::Null, Value::Unset])]),
    ];
    let mut fields: Vec<(Path, Value)> =
        vals.iter().enumerate().map(|(k, v)| (p(&format!("f{k:02}")), v.clone())).collect();
    fields.extend(names.iter().map(|n| (p(n), Value::Int64(1))));
    fields.push((Path::new(["a", "b", ""]).unwrap(), Value::Int64(2)));
    w.insert(&fields, 0).unwrap();
    w.write_set(&p("f03"), Value::Double(f64::NAN), 0).unwrap();
    round_trip(&w.state);
    // loss records carry values and totals and field lists
    let mut b = Writer::new(wid(b'B', 0), opts());
    b.receive(&w.state);
    w.write_set(&p("f06"), Value::String("w".into()), 0).unwrap();
    b.write_set(&p("f06"), Value::Bool(false), 3).unwrap();
    w.receive(&b.state);
    assert!(!w.state.losses.is_empty());
    round_trip(&w.state);
    // the new loss kinds
    let mut st = w.state.clone();
    for (k, kind) in
        [LossKind::TypeClash, LossKind::ArrayClash, LossKind::RetiredWriter].into_iter().enumerate()
    {
        st.losses.insert(
            LossKey::field(kind, p("f06"), *st.lives.keys().next().unwrap()),
            LossValue::Total(k as i128 - 1),
        );
    }
    round_trip(&st);
    // and the version 1 form takes them too
    assert_eq!(MergeState::decode(&st.encode()).unwrap(), st);
}

/// Pool states from random runs: writers exchanging whole states.
fn pool(seed: u64) -> Vec<MergeState> {
    let mut x = seed | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut ws: Vec<Writer> =
        (0..3).map(|k| Writer::new(wid(b'A' + k, u64::from(k)), opts())).collect();
    let mut out = Vec::new();
    ws[0].insert(&fl(&[("x", Value::Int64(100)), ("n", Value::String("a".into()))]), 0);
    for _ in 0..60 {
        let m = (next() % 3) as usize;
        let name = ["x", "n", "y"][(next() % 3) as usize];
        let path = p(name);
        let w = &mut ws[m];
        match next() % 9 {
            0 => {
                let o = (m + 1) % 3;
                let st = ws[o].state.clone();
                ws[m].receive(&st);
            }
            1 => {
                w.write_inc(&path, (next() % 50) as i64 - 20, 0);
            }
            2 => {
                w.write_set(&path, Value::String(format!("v{}", next() % 5)), 0);
            }
            3 => {
                w.write_set(&path, Value::Double(2.5), 0);
            }
            4 => {
                w.write_unset(&path, 0);
            }
            5 => {
                w.write_delete(0);
            }
            6 => {
                w.insert(&fl(&[("x", Value::Int64(1)), ("y", Value::Null)]), (next() % 3) * 2);
            }
            7 => {
                w.upsert_soi(
                    &fl(&[("t", Value::Int64(1))]),
                    &p("v"),
                    1,
                    &fl(&[("l", Value::Null)]),
                    0,
                );
            }
            _ => {
                w.write_replace(&fl(&[("x", Value::Int64(9)), ("z", Value::Int32(1))]), 0);
            }
        }
        out.push(ws[m].state.clone());
    }
    out
}

#[test]
fn random_states_round_trip_and_equal_states_encode_equal() {
    let mut seen: Vec<(MergeState, Vec<u8>)> = Vec::new();
    for seed in 1..=12u64 {
        for st in pool(seed * 0x9E37) {
            let enc = round_trip(&st);
            assert!(enc.len() < st.encode().len(), "compact is smaller");
            seen.push((st, enc));
        }
    }
    // Distinct states, distinct bytes; equal states, equal bytes (checked on a sample).
    for (a, b) in seen.iter().step_by(7).zip(seen.iter().skip(3).step_by(11)) {
        assert_eq!(a.0 == b.0, a.1 == b.1);
    }
    // a state joined with itself or with the empty state encodes the same
    for (st, enc) in seen.iter().step_by(13) {
        let (j, _) = join(st, st, &opts());
        let (z, _) = join(st, &MergeState::default(), &opts());
        assert_eq!(&j.encode_compact(), enc);
        assert_eq!(&z.encode_compact(), enc);
    }
}

/// Mutated compact encodings: whatever still decodes re-encodes to exactly the mutated bytes.
#[test]
fn decode_compact_mutation_fuzz_keeps_canonicity() {
    let n: u64 =
        std::env::var("Q7_DECODE_FUZZ").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let mut corpus: Vec<Vec<u8>> = Vec::new();
    for seed in 1..=6u64 {
        corpus.extend(pool(seed * 31).iter().step_by(9).map(MergeState::encode_compact));
    }
    corpus.push(golden_mixed().encode_compact());
    corpus.push(golden_obs().encode_compact());
    let mut x: u64 = 0x00C0_FFEE_D00D_5EED;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let (mut accepted, mut refused) = (0u64, 0u64);
    for _ in 0..n {
        let mut b = corpus[(next() % corpus.len() as u64) as usize].clone();
        for _ in 0..1 + next() % 4 {
            let i = (next() % b.len().max(1) as u64) as usize;
            match next() % 6 {
                0 => b[i] ^= 1 << (next() % 8),
                1 => b[i] = [0u8, 1, 2, 3, 0x7f, 0x80, 0xff][(next() % 7) as usize],
                2 => b.truncate(i),
                3 => b.insert(i, next() as u8),
                4 if i + 2 <= b.len() => b.swap(i, i + 1),
                _ => {
                    if i < b.len() {
                        b.remove(i);
                    }
                }
            }
            if b.is_empty() {
                break;
            }
        }
        match MergeState::decode_compact(&b) {
            Ok(st) => {
                accepted += 1;
                assert_eq!(st.encode_compact(), b, "an accepted input is canonical");
            }
            Err(_) => refused += 1,
        }
    }
    assert!(
        refused > accepted / 4,
        "the fuzz reaches the refusals ({accepted} ok, {refused} refused)"
    );
}

#[test]
fn every_proper_prefix_and_a_trailing_byte_are_refused() {
    let enc = golden_mixed().encode_compact();
    for n in 0..enc.len() {
        assert!(MergeState::decode_compact(&enc[..n]).is_err(), "prefix {n}");
    }
    let mut more = enc.clone();
    more.push(0);
    assert_eq!(MergeState::decode_compact(&more), Err(DecodeError::Trailing(1)));
    // The version 1 decoder refuses the compact form and the other way round.
    assert_eq!(MergeState::decode(&enc), Err(DecodeError::BadHeader));
    assert_eq!(MergeState::decode_compact(&golden_mixed().encode()), Err(DecodeError::BadHeader));
}

/// Bytes by hand: header, writer table, life table, then sections as the test writes them.
struct Hand(Vec<u8>);

impl Hand {
    fn new(writers: &[WriterId], lives: &[(u64, usize)]) -> Self {
        let mut h = Hand(b"QMS\x02".to_vec());
        h.n(writers.len());
        for w in writers {
            h.0.extend_from_slice(&w.node.to_bytes());
            h.0.extend_from_slice(&w.epoch.0);
        }
        h.n(lives.len());
        for (wall, w) in lives {
            h.hlc(*wall).n(*w);
        }
        h
    }
    fn n(&mut self, n: usize) -> &mut Self {
        assert!(n < 128);
        self.0.push(n as u8);
        self
    }
    fn hlc(&mut self, wall: u64) -> &mut Self {
        self.0.extend_from_slice(&Hlc::new(wall, 0).to_bytes());
        self
    }
    fn raw(&mut self, b: &[u8]) -> &mut Self {
        self.0.extend_from_slice(b);
        self
    }
    /// `k` empty sections (a zero count each).
    fn zeros(&mut self, k: usize) -> Vec<u8> {
        self.0.extend(std::iter::repeat_n(0, k));
        self.0.clone()
    }
}

#[test]
fn tables_must_be_sorted_and_used_and_indexes_inside() {
    let (a, b) = (wid(b'A', 0), wid(b'B', 0));
    let no = Err(DecodeError::NotCanonical);
    // the minimal body over writer A, life (1, A) used once as `dead`
    let ok = |w: &[WriterId], l: &[(u64, usize)], dead: &[usize], vv: &[(usize, u64)]| {
        let mut h = Hand::new(w, l);
        h.n(0); // lives
        h.n(dead.len());
        for d in dead {
            h.n(*d);
        }
        h.n(0).n(0).n(0); // deletes, regs, slots
        h.n(vv.len());
        for (i, t) in vv {
            h.n(*i).hlc(*t);
        }
        h.zeros(1) // losses
    };
    assert!(MergeState::decode_compact(&ok(&[a], &[(1, 0)], &[0], &[(0, 3)])).is_ok());
    assert!(MergeState::decode_compact(&ok(&[a, b], &[(1, 0), (2, 1)], &[0, 1], &[])).is_ok());
    // writers out of order, duplicated
    assert_eq!(MergeState::decode_compact(&ok(&[b, a], &[], &[], &[(0, 1), (1, 1)])), no);
    assert_eq!(MergeState::decode_compact(&ok(&[a, a], &[], &[], &[(0, 1), (1, 1)])), no);
    // lives out of order, duplicated
    assert_eq!(MergeState::decode_compact(&ok(&[a], &[(2, 0), (1, 0)], &[0, 1], &[])), no);
    assert_eq!(MergeState::decode_compact(&ok(&[a], &[(1, 0), (1, 0)], &[0, 1], &[])), no);
    // an unused writer, an unused life
    assert_eq!(MergeState::decode_compact(&ok(&[a, b], &[(1, 0)], &[0], &[])), no);
    assert_eq!(MergeState::decode_compact(&ok(&[a], &[(1, 0), (2, 0)], &[0], &[])), no);
    // a dead set out of order, duplicated
    assert_eq!(MergeState::decode_compact(&ok(&[a], &[(1, 0), (2, 0)], &[1, 0], &[])), no);
    assert_eq!(MergeState::decode_compact(&ok(&[a], &[(1, 0)], &[0, 0], &[])), no);
    // a version vector out of order
    assert_eq!(MergeState::decode_compact(&ok(&[a, b], &[], &[], &[(1, 1), (0, 1)])), no);
    // indexes outside their tables
    assert_eq!(
        MergeState::decode_compact(&ok(&[a], &[(1, 0)], &[1], &[])),
        Err(DecodeError::BadIndex)
    );
    assert_eq!(
        MergeState::decode_compact(&ok(&[a], &[], &[], &[(1, 1)])),
        Err(DecodeError::BadIndex)
    );
}

#[test]
fn varints_must_be_shortest_and_fit() {
    let a = wid(b'A', 0);
    // a count of one spelled `0x81 0x00`
    let mut h = Hand::new(&[a], &[(1, 0)]);
    h.n(0).raw(&[0x81, 0x00]).n(0); // lives: 0; dead: an overlong count of 1 ...
    let bytes = h.zeros(6);
    assert_eq!(MergeState::decode_compact(&bytes), Err(DecodeError::NotCanonical));
    // a count that runs 11 bytes
    let mut h = Hand::new(&[a], &[(1, 0)]);
    h.n(0).raw(&[0xff; 10]).raw(&[0x01]);
    assert!(MergeState::decode_compact(&h.zeros(6)).is_err());
    // a 10-byte count that overflows u64 (last group 2)
    let mut h = Hand::new(&[a], &[(1, 0)]);
    h.n(0).raw(&[0x80; 9]).raw(&[0x02]);
    assert_eq!(MergeState::decode_compact(&h.zeros(6)), Err(DecodeError::NotCanonical));
}

/// A state with one counter slot of the given total, for the total bound.
fn slot_state(total: i128) -> MergeState {
    let mut w = Writer::new(wid(b'A', 0), opts());
    w.upsert_inc(&p("x"), 1, 0).unwrap();
    let mut st = w.state;
    for s in st.slots.values_mut() {
        s.total = total;
    }
    st
}

#[test]
fn totals_round_trip_across_the_range_and_past_the_bound_are_refused() {
    for t in [
        0,
        1,
        -1,
        63,
        64,
        -64,
        -65,
        1 << 40,
        -(1 << 40),
        i128::from(i64::MAX),
        MAX_TOTAL,
        -MAX_TOTAL,
    ] {
        let st = slot_state(t);
        assert_eq!(MergeState::decode_compact(&st.encode_compact()).unwrap(), st, "{t}");
    }
    for t in [MAX_TOTAL + 1, -MAX_TOTAL - 1, i128::MAX, i128::MIN] {
        assert_eq!(
            MergeState::decode_compact(&slot_state(t).encode_compact()),
            Err(DecodeError::TotalOutOfRange),
            "{t}"
        );
    }
}

#[test]
fn the_two_spellings_of_a_stamp_and_a_stray_flag_bit_are_refused() {
    // A register whose stamp is the life's and whose generation is its own: the flags say so.
    let mut w = Writer::new(wid(b'A', 0), opts());
    w.insert(&fl(&[("s", Value::String("t".into()))]), 0).unwrap();
    let st = w.state.clone();
    let enc = st.encode_compact();
    assert_eq!(MergeState::decode_compact(&enc).unwrap(), st);
    // find the flags byte: after `lives`, `dead`, `deletes` come `regs`: 1, life 0, the path (1 1 's')
    let at = enc.windows(5).position(|x| x == [1, 0, 1, 1, b's']).expect("the register") + 5;
    assert_eq!(enc[at], 8 | 1, "stamp is the life's, generation started by the register");
    for flags in [0x80u8, 0x10 | 8 | 1, 3 | 8] {
        let mut bad = enc.clone();
        bad[at] = flags;
        assert!(
            matches!(MergeState::decode_compact(&bad), Err(DecodeError::BadTag(_))),
            "{flags:#x}"
        );
    }
    // the long spelling of the same state: flags 1 (stamp written out), then the 11-byte stamp
    let mut long = enc[..at].to_vec();
    long.push(1);
    let life = st.lives.keys().next().unwrap();
    long.extend_from_slice(&life.hlc.to_bytes());
    long.push(0); // writer index 0
    long.extend_from_slice(&enc[at + 1..]);
    assert_eq!(MergeState::decode_compact(&long), Err(DecodeError::NotCanonical));
}

#[test]
fn a_value_nested_too_deep_is_refused_and_the_limit_is_accepted() {
    let nest = |depth: usize| {
        let mut v = Value::Int64(0);
        for _ in 0..depth {
            v = Value::Array(vec![v]);
        }
        v
    };
    for (depth, ok) in [(MAX_VALUE_DEPTH, true), (MAX_VALUE_DEPTH + 1, false)] {
        let mut w = Writer::new(wid(b'A', 0), opts());
        w.insert(&fl(&[("a", nest(depth))]), 0).unwrap();
        let r = MergeState::decode_compact(&w.state.encode_compact());
        assert_eq!(r.is_ok(), ok, "{depth}");
        if !ok {
            assert_eq!(r, Err(DecodeError::TooDeep));
        }
    }
}

// ---- size: the L34-B1 shapes ------------------------------------------------------------------

fn lcg_str(seed: u64, len: usize) -> String {
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (0..len)
        .map(|_| {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (b'a' + ((x >> 33) % 26) as u8) as char
        })
        .collect()
}

/// The B1 bench body: `n` fields, n-2 strings (sized so the BSON document is about `target`
/// bytes) and the counters `cnt0`, `cnt1`.
fn body(n: usize, target: usize) -> Vec<(String, Value)> {
    let w = if n > 100 { 3 } else { 2 };
    let ns = n - 2;
    let size = |f: &[(String, Value)]| {
        let mut d = bson::Document::new();
        for (k, v) in f {
            match v {
                Value::String(s) => d.insert(k.clone(), s.clone()),
                Value::Int64(i) => d.insert(k.clone(), *i),
                _ => unreachable!(),
            };
        }
        d.to_vec().unwrap().len()
    };
    let mut fields: Vec<(String, Value)> =
        (0..ns).map(|i| (format!("f{i:0w$}"), Value::String(String::new()))).collect();
    fields.push(("cnt0".into(), Value::Int64(1_000)));
    fields.push(("cnt1".into(), Value::Int64(2_000)));
    let extra = target.saturating_sub(size(&fields));
    for (i, f) in fields.iter_mut().take(ns).enumerate() {
        f.1 = Value::String(lcg_str(i as u64 + 7, extra / ns + usize::from(i < extra % ns)));
    }
    fields
}

fn refs(f: &[(String, Value)]) -> Vec<(Path, Value)> {
    f.iter().map(|(k, v)| (p(k), v.clone())).collect()
}

fn b1_wid(n: u8) -> WriterId {
    WriterId { node: NodeId::from_bytes([n; 16]), epoch: Epoch::from_u64(u64::from(n)) }
}

/// The B1 histories on three writers: fresh (insert, then each writer `$inc`s both counters),
/// aged (ten more sequential updates) and clashed (three fields set concurrently by all).
fn b1_state(fields: &[(String, Value)], history: &str) -> MergeState {
    let o = JoinOptions::new(86_400_000, i64::MAX).unwrap();
    let mut ws: Vec<Writer> = (1..=3).map(|n| Writer::new(b1_wid(n), o.clone())).collect();
    let strings: Vec<String> =
        fields.iter().filter(|f| matches!(f.1, Value::String(_))).map(|f| f.0.clone()).collect();
    ws[0].insert(&refs(fields), 0).unwrap();
    let (c0, c1) = (p("cnt0"), p("cnt1"));
    for k in 0..3 {
        if k > 0 {
            let st = ws[k - 1].state.clone();
            ws[k].receive(&st);
        }
        ws[k].write_inc(&c0, 1, 0).unwrap();
        ws[k].write_inc(&c1, 1, 0).unwrap();
    }
    if history == "fresh" {
        return ws[2].state.clone();
    }
    let same_len = |name: &str, tag: &str, seed: u64| {
        let len = fields
            .iter()
            .find(|f| f.0 == name)
            .map_or(8, |f| if let Value::String(x) = &f.1 { x.len() } else { 8 });
        let mut x = format!("{tag}{}", lcg_str(seed, len));
        x.truncate(len.max(1));
        Value::String(x)
    };
    let mut cur = 2usize;
    for u in 0..10usize {
        let nxt = (cur + 1) % 3;
        let st = ws[cur].state.clone();
        ws[nxt].receive(&st);
        if u == 4 {
            ws[nxt].write_set(&c0, Value::Int64(500), 0).unwrap();
        } else if u % 3 == 2 {
            let q = if u % 2 == 0 { &c0 } else { &c1 };
            ws[nxt].write_inc(q, 1 + u as i64, 0).unwrap();
        } else {
            let q = &strings[(u * 7 + 3) % strings.len()];
            ws[nxt].write_set(&p(q), same_len(q, &format!("u{u}-"), u as u64 + 1), 0).unwrap();
        }
        cur = nxt;
    }
    let s = ws[cur].state.clone();
    for w in ws.iter_mut() {
        w.receive(&s);
    }
    if history == "aged" {
        return ws[0].state.clone();
    }
    for k in 0..3usize {
        let q = &strings[(2 + k * 5) % strings.len()];
        for (i, w) in ws.iter_mut().enumerate() {
            let v = same_len(q, &format!("c{k}{i}-"), (k * 3 + i) as u64 + 50);
            w.write_set(&p(q), v, 0).unwrap();
        }
    }
    let (sb, sc) = (ws[1].state.clone(), ws[2].state.clone());
    ws[0].receive(&sb);
    ws[0].receive(&sc);
    assert!(!ws[0].state.losses.is_empty());
    ws[0].state.clone()
}

/// The canonical sizes on the B1 shapes, version 1 against the compact form. Prints the table
/// (`cargo test -p kimmy-core --lib b1_shapes -- --nocapture`).
#[test]
fn b1_shapes_compact_against_verbose() {
    let mut rows = Vec::new();
    for (n, target) in [(20usize, 934usize), (200, 2211)] {
        let fields = body(n, target);
        let payload: usize = fields
            .iter()
            .map(|f| match &f.1 {
                Value::String(s) => s.len(),
                _ => 8,
            })
            .sum();
        for history in ["fresh", "aged", "clashed"] {
            let st = b1_state(&fields, history);
            let (verbose, compact) = (st.canonical_bytes().len(), st.compact_bytes().len());
            assert_eq!(MergeState::decode_compact(&st.encode_compact()).unwrap(), st);
            assert!(compact * 3 < verbose, "{n} {history}: {compact} against {verbose}");
            rows.push(format!(
                "| {n} | {history} | {verbose} | {compact} | {:.1}x | {} |",
                verbose as f64 / compact as f64,
                compact + 16 - payload
            ));
        }
    }
    println!(
        "| fields | history | verbose B | compact B | ratio | compact metadata B (+16 B hash, no values) |"
    );
    for r in rows {
        println!("{r}");
    }
}

#[test]
fn the_compact_form_sets_do_not_depend_on_insertion_order() {
    // The same set of writers and lives reached by two orders encodes alike.
    let fields = body(20, 934);
    let st = b1_state(&fields, "fresh");
    let mut shuffled = MergeState::default();
    for (k, v) in st.regs.iter().rev() {
        shuffled.regs.insert(k.clone(), v.clone());
    }
    shuffled.lives = st.lives.clone();
    shuffled.slots = st.slots.iter().rev().map(|(k, v)| (k.clone(), v.clone())).collect();
    shuffled.vv = st.vv.clone();
    assert_eq!(shuffled.compact_bytes(), st.compact_bytes());
    let used: BTreeSet<WriterId> = st.vv.keys().copied().collect();
    assert_eq!(used.len(), 3);
}
