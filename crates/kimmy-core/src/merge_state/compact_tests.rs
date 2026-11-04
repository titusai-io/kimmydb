//! Tests of the compact canonical form (`compact.rs`): round trips, canonicity and refusals,
//! and its size on the shapes of the L34-B1 benchmark.

use std::collections::{BTreeMap, BTreeSet};

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
    assert_eq!(&enc[..4], b"QMS\x02", "the version byte is 2 for every state");
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
    w.insert(&fields, 0).unwrap();
    w.write_set(&Path::new(["a", "b", ""]).unwrap(), Value::Int64(2), 0).unwrap();
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
            LossValue::Total((k as i128 - 1).into()),
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
    // states with an order state: the entries of the reference model's scenarios, and pooled runs
    // with the order state fed (so that the damage reaches section 10)
    let ord_corpus = ord_corpus();
    let ords_in_corpus = ord_corpus.len();
    corpus.extend(ord_corpus);
    let mut x: u64 = 0x00C0_FFEE_D00D_5EED;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let (mut accepted, mut refused, mut accepted_with_ords) = (0u64, 0u64, 0usize);
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
                accepted_with_ords += usize::from(!st.ords.is_empty());
                assert_eq!(st.encode_compact(), b, "an accepted input is canonical");
                // and nothing panics on the read side or the join of what was accepted
                let o = opts();
                let _ = st.ordered_document(&o);
                let _ = join(&st, &st, &o).0.compact_hash();
            }
            Err(_) => refused += 1,
        }
    }
    assert!(
        refused > accepted / 4,
        "the fuzz reaches the refusals ({accepted} ok, {refused} refused)"
    );
    eprintln!(
        "compact decode fuzz: {n} inputs, {accepted} accepted ({accepted_with_ords} with an order state), {refused} refused; {ords_in_corpus} order states in the corpus"
    );
    assert!(
        ords_in_corpus > 100 && accepted_with_ords > 100,
        "{ords_in_corpus} {accepted_with_ords}"
    );
}

#[test]
fn every_proper_prefix_and_a_trailing_byte_are_refused() {
    let enc = golden_mixed().encode_compact();
    for n in 0..enc.len() {
        assert!(MergeState::decode_compact(&enc[..n]).is_err(), "prefix {n}");
    }
    // A byte after the losses begins section 10: a count of 0 there is an empty order state, which
    // is refused (any other count promises entries that are not there).
    let mut more = enc.clone();
    more.push(0);
    assert_eq!(MergeState::decode_compact(&more), Err(DecodeError::OrdSectionEmpty));
    more[enc.len()] = 1;
    assert!(MergeState::decode_compact(&more).is_err());
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
    // the minimal body over writer A, life (1, A) used once as `dead` (and so listed in `lives`:
    // a dead life the state does not list is refused)
    let ok = |w: &[WriterId], l: &[(u64, usize)], dead: &[usize], vv: &[(usize, u64)]| {
        let mut h = Hand::new(w, l);
        let listed: BTreeSet<usize> = dead.iter().copied().collect();
        h.n(listed.len()); // lives
        for l in &listed {
            h.n(*l).n(0).n(0).n(1); // the life, no deletes seen, no field order, an upsert
        }
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
    assert!(
        MergeState::decode_compact(&ok(&[a, b], &[(1, 0), (2, 1)], &[0, 1], &[(0, 1), (1, 2)]))
            .is_ok()
    );
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
        s.total = total.into();
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
        // (the writers refuse a value this deep, so put it in a register by hand)
        let mut w = Writer::new(wid(b'A', 0), opts());
        w.insert(&fl(&[("a", Value::Null)]), 0).unwrap();
        w.state.regs.values_mut().next().unwrap().value = nest(depth);
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

// ---- documents inside arrays and the map marker ------------------------------------------

fn d(f: &[(&str, Value)]) -> Value {
    Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
}

/// A state whose register `a` holds `v`.
fn state_with(v: Value) -> MergeState {
    let mut w = Writer::new(wid(b'A', 0), opts());
    w.insert(&fl(&[("a", v)]), 0).unwrap();
    w.state
}

/// Where in the compact bytes of `st` the byte pattern `pat` first occurs.
fn find(bytes: &[u8], pat: &[u8]) -> usize {
    bytes.windows(pat.len()).position(|w| w == pat).expect("pattern in the bytes")
}

#[test]
fn a_document_is_legal_inside_an_array_at_any_depth_in_the_compact_form() {
    let nested = Value::Array(vec![
        d(&[("x", Value::Int64(1)), ("", Value::Null)]),
        Value::Array(vec![d(&[("deep", Value::Array(vec![d(&[("z", Value::Bool(true))])]))])]),
        Value::Int32(3),
    ]);
    for v in [Value::Array(vec![d(&[])]), nested] {
        let st = state_with(v.clone());
        assert_eq!(st.regs.values().next().unwrap().value, v);
        round_trip(&st);
        // a lost value may hold one too
        let mut st = st;
        let life = *st.lives.keys().next().unwrap();
        st.losses.insert(LossKey::field(LossKind::Clash, p("a"), life), LossValue::Value(v));
        round_trip(&st);
    }
}

#[test]
fn version_one_cannot_hold_a_document_and_refuses() {
    let st = state_with(Value::Array(vec![d(&[("x", Value::Int64(1))])]));
    assert!(!st.v1_can_hold());
    assert_eq!(st.try_canonical_bytes(), Err(V1CannotHold));
    assert_eq!(st.try_encode(), Err(V1CannotHold));
    // the compact bytes are not a version 1 stream
    assert_eq!(MergeState::decode(&st.encode_compact()), Err(DecodeError::BadHeader));
    // a state with arrays but no document still takes both forms
    let plain = state_with(Value::Array(vec![Value::Int64(1), Value::Array(vec![])]));
    assert!(plain.v1_can_hold());
    assert_eq!(plain.try_encode().unwrap(), plain.encode());
    assert_eq!(MergeState::decode(&plain.encode()).unwrap(), plain);
    // the tag is unknown to version 1: patch a document tag into a version 1 array
    let mut bytes = plain.encode();
    let at = find(&bytes, &[4, 0, 0, 0, 0, 0, 0, 0, 2, 1]);
    bytes[at + 9] = 16;
    assert_eq!(MergeState::decode(&bytes), Err(DecodeError::BadTag(16)));
}

#[test]
#[should_panic(expected = "version 1 cannot hold a document")]
fn the_infallible_version_one_encoder_panics_on_a_document() {
    let st = state_with(Value::Array(vec![d(&[])]));
    let _ = st.canonical_bytes();
}

#[test]
fn a_document_is_refused_as_a_register_by_the_checks_and_the_decoder() {
    let doc = d(&[("x", Value::Int64(1))]);
    assert_eq!(doc.check_register(), Err(InvalidValue::DocumentAsRegister));
    assert_eq!(Value::Array(vec![doc.clone()]).check_register(), Ok(()));
    let life = MStamp { hlc: Hlc::new(1, 0), writer: wid(b'A', 0) };
    let reg = |v: Value| Reg::new(life, v, BTreeMap::new(), None, false);
    assert_eq!(reg(doc.clone()), Err(InvalidValue::DocumentAsRegister));
    assert!(reg(Value::Array(vec![doc.clone()])).is_ok());
    // a writer expands one (a sub-document is a map, never a register)
    let mut w = Writer::new(wid(b'A', 0), opts());
    w.insert(&[(p("a"), doc.clone())], 0).unwrap();
    assert!(w.state.regs.values().all(|r| r.value.check_register().is_ok()));
    assert_eq!(w.state.regs[&(*w.state.lives.keys().next().unwrap(), p("a"))].value, Value::Map);
    // ... and refuses the marker itself, and a repeated field name
    assert!(w.write_set(&p("m"), Value::Map, 0).is_none());
    let twice = Value::Document(vec![("k".into(), Value::Null), ("k".into(), Value::Null)]);
    assert!(w.write_set(&p("m"), twice, 0).is_none());
    // the decoder: turn the array tag of a register into the document tag, and a loss value
    let enc = state_with(Value::Array(vec![doc.clone()])).encode_compact();
    let at = find(&enc, &[4, 1, 16, 1, 1, b'x']);
    let mut bad = enc.clone();
    bad[at] = 16;
    assert_eq!(MergeState::decode_compact(&bad), Err(DecodeError::BadTag(16)));
    // a document nested past the depth limit is refused, like an array
    let mut deep = Value::Int64(0);
    for _ in 0..MAX_VALUE_DEPTH + 1 {
        deep = Value::Array(vec![d(&[("k", deep)])]);
    }
    assert_eq!(deep.check_register(), Err(InvalidValue::TooDeep));
}

#[test]
fn the_map_marker_is_a_register_value_and_never_an_array_element() {
    assert_eq!(Value::Map.check_register(), Ok(()));
    assert_eq!(Value::Array(vec![Value::Map]).check_register(), Err(InvalidValue::MapMarker));
    assert_eq!(
        Value::Array(vec![d(&[("m", Value::Map)])]).check_register(),
        Err(InvalidValue::MapMarker)
    );
    let mut st = state_with(Value::Int64(1));
    st.regs.values_mut().next().unwrap().value = Value::Map;
    let enc = round_trip(&st);
    assert_eq!(MergeState::decode(&st.encode()).unwrap(), st, "version 1 takes the marker");
    // tag 5 inside an array is refused by both decoders
    let arr = state_with(Value::Array(vec![Value::Int64(1)]));
    let mut bad = arr.encode_compact();
    let at = find(&bad, &[4, 1, 1]);
    bad[at + 2] = 5;
    assert_eq!(MergeState::decode_compact(&bad), Err(DecodeError::BadTag(5)));
    let mut bad1 = arr.encode();
    let at = find(&bad1, &[4, 0, 0, 0, 0, 0, 0, 0, 1, 1]);
    bad1[at + 9] = 5;
    assert_eq!(MergeState::decode(&bad1), Err(DecodeError::BadTag(5)));
    assert!(enc.len() > 4);
}

#[test]
fn a_document_in_an_array_is_equal_structurally_type_strict_and_in_field_order() {
    let a = Value::Array(vec![d(&[("x", Value::Int64(1)), ("y", Value::Double(0.0))])]);
    assert_eq!(a, a.clone());
    assert!(Value::same(Some(&a), &a.clone()));
    let other = |v: Value| Value::same(Some(&a), &v);
    // field order matters, for the state's equality and for the write diff
    let swapped = Value::Array(vec![d(&[("y", Value::Double(0.0)), ("x", Value::Int64(1))])]);
    assert_ne!(a, swapped);
    assert!(!other(swapped.clone()));
    // a type difference matters (Int32 against Int64)
    assert!(!other(Value::Array(vec![d(&[("x", Value::Int32(1)), ("y", Value::Double(0.0))])])));
    // a name, a field count, a document against an array
    assert!(!other(Value::Array(vec![d(&[("x", Value::Int64(1)), ("z", Value::Double(0.0))])])));
    assert!(!other(Value::Array(vec![d(&[("x", Value::Int64(1))])])));
    assert!(!other(Value::Array(vec![Value::Array(vec![Value::Int64(1), Value::Double(0.0)])])));
    // and the writer treats a changed field order as a change: the register is rewritten
    let mut w = Writer::new(wid(b'A', 0), opts());
    w.insert(&fl(&[("a", a.clone())]), 0).unwrap();
    assert!(w.write_set(&p("a"), a, 0).is_none(), "the same document is a no-op");
    assert!(w.write_set(&p("a"), swapped, 0).is_some());
}

#[test]
fn receiving_a_state_with_a_document_in_an_array_works_and_reports_the_change() {
    // The "changed" verdict does not go through the version 1 bytes, which cannot hold it.
    let a = state_with(Value::Array(vec![d(&[("x", Value::Int64(1))])]));
    let mut b = Writer::new(wid(b'B', 0), opts());
    assert!(b.receive_report(&a).changed);
    assert!(!b.receive_report(&a).changed);
    assert_eq!(b.state, a);
}

/// A table index is not a count: `decode_compact` once refused (`Truncated`) the encoding of a
/// state whose last writer was index 12 or more, because it bounded the index by what was left of
/// the input. Honest states of 12, 13 and 40 writers, built by writers.
#[test]
fn a_state_of_many_writers_round_trips_in_the_compact_form() {
    let o = JoinOptions::new(1_000_000_000, i64::MAX).unwrap();
    for n in [12u8, 13, 40] {
        let mut ws: Vec<Writer> = (0..n)
            .map(|k| {
                let id =
                    WriterId { node: NodeId::from_bytes([k + 1; 16]), epoch: Epoch::from_u64(0) };
                Writer::new(id, o.clone())
            })
            .collect();
        let x = Path::flat("x").unwrap();
        ws[0].insert(&[(x.clone(), Value::Int64(1))], 0);
        let first = ws[0].state.clone();
        let mut all = first.clone();
        for w in ws.iter_mut().skip(1) {
            w.receive(&first);
            w.write_inc(&x, 1, 0);
            all = join(&all, &w.state, &o).0;
        }
        assert_eq!(MergeState::decode(&all.encode()).as_ref(), Ok(&all), "version 1, {n} writers");
        let c = all.encode_compact();
        assert_eq!(MergeState::decode_compact(&c).as_ref(), Ok(&all), "compact, {n} writers");
    }
}

/// The same for every table size from 1 to 300 writers (past `MAX_WRITERS`: a received state past
/// the cap is kept, so it must also decode), with a counter slot and a version vector entry per
/// writer, in both forms, and the accepted bytes encode back to themselves.
#[test]
fn every_table_of_one_to_three_hundred_writers_round_trips() {
    let life = MStamp { hlc: Hlc::new(1, 0), writer: wid(0, 0) };
    for n in 1..=300usize {
        let mut s = MergeState::default();
        s.lives.insert(
            life,
            Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Upsert },
        );
        for k in 0..n {
            let w = WriterId {
                node: NodeId::from_bytes([
                    (k / 250) as u8,
                    (k % 250) as u8 + 1,
                    7,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ]),
                epoch: Epoch::from_u64(0),
            };
            let at = MStamp { hlc: Hlc::new(2 + k as u64, 0), writer: w };
            s.slots.insert(
                (life, p("x"), w, None),
                Slot { stamp: at, total: Num::Int64(k as i128 + 1) },
            );
            s.vv.insert(w, at.hlc);
        }
        s.vv.insert(life.writer, life.hlc);
        assert_eq!(s.writers().len(), n + 1);
        let c = s.encode_compact();
        let back = MergeState::decode_compact(&c).unwrap_or_else(|e| panic!("{n} writers: {e:?}"));
        assert_eq!(back, s, "compact, {n} writers");
        assert_eq!(back.encode_compact(), c, "canonical, {n} writers");
        assert_eq!(MergeState::decode(&s.encode()).as_ref(), Ok(&s), "version 1, {n} writers");
    }
}

/// D1 (the review of `a2306348`): the life table, like the writer table, is read by index. One
/// writer does `k` rounds of insert and delete and then inserts again; the live document is in
/// the newest life, which is the last entry of the table, index `k`, with fewer than `k` bytes
/// of the state after it from `k = 29`. The decoder that bounded an index by the input left
/// refused this state as truncated. Built with `Writer` calls, in both forms, with the compact
/// re-encode identity.
#[test]
fn the_life_table_of_one_to_three_hundred_lives_round_trips() {
    let o = opts();
    for k in 1..=300usize {
        let mut w = Writer::new(
            WriterId { node: NodeId::from_bytes([1; 16]), epoch: Epoch::from_u64(0) },
            o.clone(),
        );
        for _ in 0..k {
            assert!(w.insert(&fl(&[("x", Value::Int64(1))]), 0).is_some());
            assert!(w.write_delete(0).is_some());
        }
        assert!(w.insert(&fl(&[("x", Value::Int64(9))]), 0).is_some());
        let s = &w.state;
        assert_eq!(s.lives.len(), k + 1, "{k} rounds: one life per insert");
        let newest =
            s.regs.keys().map(|(l, _)| *l).max().expect("the live insert holds a register");
        assert_eq!(
            s.lives.keys().position(|l| *l == newest),
            Some(k),
            "{k}: the newest life is index k"
        );
        let c = s.encode_compact();
        let back = MergeState::decode_compact(&c).unwrap_or_else(|e| panic!("{k} rounds: {e:?}"));
        assert_eq!(&back, s, "compact, {k} rounds");
        assert_eq!(back.encode_compact(), c, "canonical, {k} rounds");
        assert_eq!(MergeState::decode(&s.encode()).as_ref(), Ok(s), "version 1, {k} rounds");
    }
}

// ---- the order state (section 10) ------------------------------------------------------------

use super::ord_tests::{goldens, state as golden_state};
use super::prop_tests::{nested_ord_pools, ord_pools};

/// The encodings that carry an order state, for the damage fuzz: the writer entries of the
/// reference model's scenarios and every fifth state of pooled runs with the order state fed.
fn ord_corpus() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let g = goldens();
    for sc in g["scenarios"].as_object().unwrap().values() {
        out.extend(
            sc["entries"].as_array().unwrap().iter().map(|e| golden_state(e).encode_compact()),
        );
    }
    let o = opts();
    for pool in ord_pools(6, 24, &o).iter().chain(&nested_ord_pools(6, 26, &o)) {
        out.extend(
            pool.iter().step_by(5).filter(|s| !s.ords.is_empty()).map(MergeState::encode_compact),
        );
    }
    assert!(out.iter().all(|b| MergeState::decode_compact(b).is_ok_and(|s| !s.ords.is_empty())));
    out
}

/// A state of one upsert life `(1, A)` with a version vector at 5 and the given order entries.
fn with_ords(ords: &[((u64, OrdAt), FieldOrd)]) -> MergeState {
    let a = wid(b'A', 0);
    let mut s = MergeState::default();
    let l = MStamp { hlc: Hlc::new(1, 0), writer: a };
    s.lives.insert(l, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Upsert });
    s.vv.insert(a, Hlc::new(5, 0));
    for ((_, at), x) in ords {
        s.ords.insert((l, at.clone()), x.clone());
    }
    s
}

fn st(wall: u64, w: WriterId) -> MStamp {
    MStamp { hlc: Hlc::new(wall, 0), writer: w }
}

/// An order entry with every part: a `u` floor with the bit, two firsts, `last` and `lvl`.
fn full_ord(a: WriterId, b: WriterId) -> FieldOrd {
    FieldOrd {
        floor: Some(Floor::U { stamp: st(2, a), prev: Hlc::new(1, 0), others: true }),
        firsts: [(a, (st(3, a), 0)), (b, (st(4, b), 300))].into(),
        last: Some((st(5, b), 1)),
        lvl: Some(st(4, a)),
    }
}

/// The frozen layout of section 10, byte by byte, written independently of the encoder.
#[test]
fn an_order_state_is_section_ten_after_the_losses() {
    let (a, b) = (wid(b'A', 0), wid(b'B', 0));
    let mut s = with_ords(&[((1, OrdAt::Field(p("x"))), full_ord(a, b))]);
    s.vv.insert(b, Hlc::new(5, 0));
    // header, writers A B, life table [(1, A)], then the sections
    let mut h = Hand::new(&[a, b], &[(1, 0)]);
    h.n(1).n(0).n(0).n(0).n(1); // lives: the life, no deletes seen, no field order, an upsert
    h.n(0).n(0).n(0).n(0); // dead, deletes, regs, slots
    h.n(2).n(0).hlc(5).n(1).hlc(5); // the version vector
    h.n(0); // losses
    h.n(1); // section 10: one entry
    h.n(0).n(1).n(1).raw(b"x"); // life 0, the path x
    h.n(3).hlc(2).n(0).hlc(1); // the floor: u with the bit, its stamp (2, A), prev 1
    h.n(2).n(0).hlc(3).n(0).n(0); // firsts: A at (3, A) position 0
    h.n(1).hlc(4).n(1).raw(&[0xac, 0x02]); //                B at (4, B) position 300
    h.n(3); // last and lvl
    h.hlc(5).n(1).n(1); // last at (5, B), position 1
    h.hlc(4).n(0); // lvl at (4, A)
    assert_eq!(s.encode_compact(), h.0, "section 10, as the frozen layout says");
    assert_eq!(MergeState::decode_compact(&h.0), Ok(s.clone()));
    // the same state without the order state is the same bytes with nothing after the losses
    let mut bare = s.clone();
    bare.ords.clear();
    let e = bare.encode_compact();
    assert_eq!(&e[..4], b"QMS\x02");
    assert!(h.0.starts_with(&e) && h.0.len() > e.len());
    // the root, a replace floor, no firsts, no last and no lvl: the shortest entry
    let r = with_ords(&[(
        (1, OrdAt::Root),
        FieldOrd { floor: Some(Floor::R { stamp: st(5, a) }), ..FieldOrd::default() },
    )]);
    let mut h = Hand::new(&[a], &[(1, 0)]);
    h.n(1).n(0).n(0).n(0).n(1).n(0).n(0).n(0).n(0).n(1).n(0).hlc(5).n(0);
    h.n(1).n(0).n(0); // section 10: one entry, life 0, the root (a segment count of 0)
    h.n(2).hlc(5).n(0); // the floor: r at (5, A), no bit and no prev
    h.n(0).n(0); // no firsts, no last and no lvl
    assert_eq!(r.encode_compact(), h.0);
    assert_eq!(MergeState::decode_compact(&h.0), Ok(r));
    // entries come in the key order of the map: the root before the paths, a path by its segments
    let level = FieldOrd { lvl: Some(st(2, a)), ..FieldOrd::default() };
    let both = with_ords(&[
        ((1, OrdAt::Field(p("b"))), level.clone()),
        ((1, OrdAt::Root), level.clone()),
        ((1, OrdAt::Field(p("a"))), level),
    ]);
    round_trip(&both);
}

/// What section 10 must refuse, by the decoder's own checks.
#[test]
fn section_ten_is_decoded_strictly() {
    let (a, b) = (wid(b'A', 0), wid(b'B', 0));
    // a body of one writer A (and B when `two`), the life (1, A), a vv of 5, then section 10 by `ords`
    let body = |version: u8, two: bool, listed: bool, vv: u64, ords: &dyn Fn(&mut Hand)| {
        let ws: &[WriterId] = if two { &[a, b] } else { &[a] };
        let mut h = Hand::new(ws, &[(1, 0)]);
        h.0[3] = version;
        h.n(usize::from(listed));
        if listed {
            h.n(0).n(0).n(0).n(1);
        }
        h.n(0).n(0).n(0).n(0);
        if two {
            h.n(2).n(0).hlc(vv).n(1).hlc(vv);
        } else {
            h.n(1).n(0).hlc(vv);
        }
        h.n(0);
        ords(&mut h);
        h.0
    };
    let entry = |h: &mut Hand, floor: &[u8], rest: &[u8]| {
        h.n(0).n(1).n(1).raw(b"x").raw(floor).raw(rest);
    };
    let ok_floor = [2u8, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0]; // r at (5, A)
    let good = |h: &mut Hand| {
        h.n(1);
        entry(h, &ok_floor, &[0, 0]);
    };
    assert!(MergeState::decode_compact(&body(2, false, true, 5, &good)).is_ok());
    // no section at all is a state without an order state; a section of count 0 is refused; bytes
    // after a section are trailing input; the version byte is 2 and nothing else
    assert!(
        MergeState::decode_compact(&body(2, false, true, 5, &|_| {}))
            .is_ok_and(|s| s.ords.is_empty())
    );
    assert_eq!(
        MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(0);
        })),
        Err(DecodeError::OrdSectionEmpty)
    );
    assert_eq!(
        MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            good(h);
            h.n(0);
        })),
        Err(DecodeError::Trailing(1))
    );
    for version in [0u8, 1, 3, 4, 0xff] {
        assert_eq!(
            MergeState::decode_compact(&body(version, false, true, 5, &good)),
            Err(DecodeError::BadHeader),
            "version {version}"
        );
    }
    // the floor byte: 0 to 3 only, so the update bit on a replace floor has no spelling
    for bad in [4u8, 5, 6, 7, 0x80, 0xff] {
        let r = MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1);
            let mut f = ok_floor.to_vec();
            f[0] = bad;
            entry(h, &f, &[0, 0]);
        }));
        assert_eq!(r, Err(DecodeError::OrdBadFloor(bad)), "floor byte {bad}");
    }
    // a `u` floor takes its `prev` (10 bytes) after the stamp; an `r` floor does not
    let u_floor: Vec<u8> = [&[1u8][..], &ok_floor[1..], &[0u8; 10]].concat();
    let mut u_floor_bit = u_floor.clone();
    u_floor_bit[0] = 3;
    for f in [&u_floor, &u_floor_bit] {
        // (5, A) is not above its own floor, so the candidates are none: normal
        let r = MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1);
            entry(h, f, &[0, 0]);
        }));
        assert!(r.is_ok(), "{r:?}");
    }
    // the flags byte: bits 0 and 1 only
    for flags in [4u8, 5, 0x80] {
        let r = MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1);
            entry(h, &[0], &[0, flags]);
        }));
        assert_eq!(r, Err(DecodeError::BadTag(flags)));
    }
    // keys out of order or twice, and the same inside the firsts
    let two_entries = |first: &[u8], second: &[u8]| {
        body(2, false, true, 5, &|h| {
            h.n(2).n(0).n(1).n(1).raw(first).raw(&ok_floor).raw(&[0, 0]);
            h.n(0).n(1).n(1).raw(second).raw(&ok_floor).raw(&[0, 0]);
        })
    };
    assert!(MergeState::decode_compact(&two_entries(b"x", b"y")).is_ok());
    assert_eq!(
        MergeState::decode_compact(&two_entries(b"y", b"x")),
        Err(DecodeError::NotCanonical)
    );
    assert_eq!(
        MergeState::decode_compact(&two_entries(b"x", b"x")),
        Err(DecodeError::NotCanonical)
    );
    let firsts = |w1: usize, w2: usize| {
        body(2, true, true, 5, &|h| {
            h.n(1).n(0).n(1).n(1).raw(b"x").n(0).n(2);
            h.n(w1).hlc(5).n(0).n(0).n(w2).hlc(5).n(0).n(0).n(0);
        })
    };
    assert!(MergeState::decode_compact(&firsts(0, 1)).is_ok());
    assert_eq!(MergeState::decode_compact(&firsts(1, 0)), Err(DecodeError::NotCanonical));
    assert_eq!(MergeState::decode_compact(&firsts(0, 0)), Err(DecodeError::NotCanonical));
    assert_eq!(MergeState::decode_compact(&firsts(0, 2)), Err(DecodeError::BadIndex));
    // a life index outside the table, a root that is a path of no segments is the root
    assert_eq!(
        MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1).n(1).n(1).n(1).raw(b"x").raw(&[0, 0, 0]);
        })),
        Err(DecodeError::BadIndex)
    );
    // a segment with a NUL byte
    assert_eq!(
        MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1).n(0).n(1).n(1).raw(b"\0").raw(&[0, 0, 0]);
        })),
        Err(DecodeError::BadPath)
    );
    // a position in more bytes than it needs; one past a u64
    for pos in [&[0x80u8, 0x00][..], &[0xff; 10]] {
        let r = MergeState::decode_compact(&body(2, false, true, 5, &|h| {
            h.n(1).n(0).n(1).n(1).raw(b"x").n(0).n(1).n(0).hlc(5).n(0).raw(pos).n(0);
        }));
        assert_eq!(r, Err(DecodeError::NotCanonical), "{pos:?}");
    }
    // a candidate the floor voids; the same entry with the candidate above the floor
    let voided = |at: u64| {
        body(2, false, true, 5, &|h| {
            h.n(1).n(0).n(1).n(1).raw(b"x");
            h.n(1).hlc(2).n(0).hlc(0); // a `u` floor at (2, A)
            h.n(1).n(0).hlc(at).n(0).n(0); // one first at (at, A)
            h.n(0);
        })
    };
    assert_eq!(MergeState::decode_compact(&voided(2)), Err(DecodeError::OrdNotNormal));
    assert_eq!(MergeState::decode_compact(&voided(1)), Err(DecodeError::OrdNotNormal));
    assert!(MergeState::decode_compact(&voided(3)).is_ok());
    let void_last = body(2, false, true, 5, &|h| {
        h.n(1).n(0).n(1).n(1).raw(b"x");
        h.n(2).hlc(3).n(0); // an `r` floor at (3, A)
        h.n(0).n(1).hlc(2).n(0).n(0); // no firsts, a last at (2, A)
    });
    assert_eq!(MergeState::decode_compact(&void_last), Err(DecodeError::OrdNotNormal));
    // an entry of a life that `lives` does not list
    assert_eq!(
        MergeState::decode_compact(&body(2, false, false, 5, &good)),
        Err(DecodeError::OrdLifeNotListed)
    );
    // a version vector that does not cover a stamp of the entry
    let behind = |floor: bool, first: bool, last: bool, lvl: bool| {
        body(2, false, true, 4, &|h| {
            h.n(1).n(0).n(1).n(1).raw(b"x");
            if floor {
                h.n(2).hlc(5).n(0);
            } else {
                h.n(0);
            }
            h.n(usize::from(first));
            if first {
                h.n(0).hlc(5).n(0).n(0);
            }
            h.n(u8::from(last) as usize | (usize::from(lvl) << 1));
            if last {
                h.hlc(5).n(0).n(0);
            }
            if lvl {
                h.hlc(5).n(0);
            }
        })
    };
    for (f, c, l, v) in [
        (true, false, false, false),
        (false, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        assert_eq!(
            MergeState::decode_compact(&behind(f, c, l, v)),
            Err(DecodeError::VersionVectorBehind),
            "{f} {c} {l} {v}"
        );
    }
}

/// The end of the input delimits section 10: a proper prefix of an encoding that carries an order
/// state is refused, EXCEPT the prefix that ends where the losses end, which is the encoding of the
/// same state without its order state; a byte after the section is trailing input; the other form
/// refuses the compact tag.
#[test]
fn an_order_states_prefixes_are_refused_but_the_one_that_ends_at_the_losses() {
    let (a, b) = (wid(b'A', 0), wid(b'B', 0));
    let mut s = with_ords(&[
        ((1, OrdAt::Root), FieldOrd { lvl: Some(st(2, a)), ..FieldOrd::default() }),
        ((1, OrdAt::Field(p("x"))), full_ord(a, b)),
    ]);
    s.vv.insert(b, Hlc::new(5, 0));
    let enc = round_trip(&s);
    let mut bare = s.clone();
    bare.ords.clear();
    let bare_enc = bare.encode_compact();
    assert!(enc.starts_with(&bare_enc), "the tables are the same, so the sections 1 to 9 are");
    for n in 0..enc.len() {
        match MergeState::decode_compact(&enc[..n]) {
            Err(_) => assert_ne!(n, bare_enc.len(), "the prefix that ends at the losses decodes"),
            Ok(x) => {
                assert_eq!(n, bare_enc.len(), "prefix {n} decodes");
                assert_eq!(x, bare);
            }
        }
    }
    let mut more = enc.clone();
    more.push(0);
    assert_eq!(MergeState::decode_compact(&more), Err(DecodeError::Trailing(1)));
    assert_eq!(MergeState::decode(&enc), Err(DecodeError::BadHeader));
    assert!(!s.v1_can_hold());
    assert_eq!(s.try_encode(), Err(V1CannotHold));
    assert_eq!(s.try_canonical_bytes(), Err(V1CannotHold));
    // the hash separates the state with its order state from the one without
    assert_ne!(bare.compact_hash(), s.compact_hash());
    assert_eq!(bare.compact_bytes(), bare_enc[4..]);
}

/// The entries of the reference model's scenarios (states with real order entries, cells, lives
/// and all) round trip in the compact form and re-encode to themselves, and the checks of the
/// decoder accept them.
#[test]
fn the_models_scenario_states_round_trip_in_the_compact_form() {
    let g = goldens();
    let (mut n, mut with_ords) = (0, 0);
    for sc in g["scenarios"].as_object().unwrap().values() {
        for e in sc["entries"].as_array().unwrap() {
            let st = golden_state(e);
            assert!(st.check_decoded().is_ok());
            round_trip(&st);
            n += 1;
            with_ords += usize::from(!st.ords.is_empty());
        }
    }
    assert!(n > 90 && with_ords * 10 > n * 9, "{n} states, {with_ords} with an order state");
}
