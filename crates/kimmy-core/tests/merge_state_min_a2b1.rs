//! The model's exhaustive class MIN-A2B1, replayed through `Writer`s.
//!
//! The reviewed model's brute-force checker (`brute.py`, `suite6.brute_pinned`, `pins/MIN-A2B1.json`) takes the 18
//! operations below, writer A with two of them and writer B with one, every causal pattern of who-has-seen-what (34,992
//! histories) and every stamp order that extends causality (58,320 linear extensions), and runs the model on each. Its
//! pinned counts: 34,992 histories, 58,320 serial orders enumerated, 57,742 stamp orders run (the 416 histories in which an
//! operation can never succeed are not compared), 56,687 of them "ok" (the model's ordered document is the outcome of some
//! serial order).
//!
//! `tests/data/gen_min_a2b1.py` runs that checker once, unchanged, through a hook, and writes `tests/data/min_a2b1.txt`: for
//! each history its operations, its receives, the FNV-1a 64 hash of the model's final ORDERED document after each stamp order,
//! and the number of its stamp orders the checker found ok. This test builds the same linear extensions, replays every stamp
//! order through `Writer`s (the stamps the model gave, an operation that is a no-op or refused leaves its writer's state as
//! the entry its readers get), and requires the same ordered document, values and field order, for every one of the 57,742.
//! A mismatch prints the history and the stamp order.
//!
//! What it pins: the four counts above (the first three are counted here, the fourth is the checker's own classification,
//! read from the fixture), and one hash per stamp order. The checker's "ok" classification needs the serial reference
//! executor, which only the model has; the Writer is held to the model's document, and the model's documents are held to the
//! serial orders by the model's own pins.

use std::collections::BTreeMap;

use kimmy_core::hlc::Hlc;
use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, MStamp, MergeState, OrderedValue, Path, UpdateError, UpdateOp, Value,
    Writer, WriterId, join,
};

const FIXTURE: &str = include_str!("data/min_a2b1.txt");

const MODEL6_SHA256: &str = "72da18ebbf3bb383d0741d1f10deb4265cb884dd960d49047913204016ebab3d";
const PULLMODEL6_SHA256: &str = "e68fbe0470ffdaf4a9406573c5fd1ead7f5fe2ee2fe39b76a6a3eb1a0734bf1b";
const BRUTE_SHA256: &str = "0e29ba34b19933a74d26d8f6809910fc5e4d26f700104c43bdd57f5d63f82dec";

const OPS: [&str; 18] = [
    "setm",
    "swap",
    "repl2",
    "seta",
    "unseta",
    "setc",
    "setmr",
    "unsetmp",
    "unsetm",
    "unsetb",
    "inca",
    "incd",
    "setmany",
    "setnull",
    "setmscalar",
    "incnew",
    "upd",
    "upd2",
];

fn np(dotted: &str) -> Path {
    Path::new(dotted.split('.')).unwrap()
}
fn i(n: i64) -> Value {
    Value::Int64(n)
}
fn doc(f: &[(&str, Value)]) -> Value {
    Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
}
fn fields(f: &[(&str, Value)]) -> Vec<(Path, Value)> {
    f.iter().map(|(k, v)| (np(k), v.clone())).collect()
}

fn wid(c: char) -> WriterId {
    WriterId { node: NodeId::from_bytes([c as u8; 16]), epoch: Epoch::from_u64(0) }
}

/// One operation of the class on a writer, with the stamp `st`. True when it wrote.
fn apply(w: &mut Writer, op: usize, st: MStamp) -> bool {
    let mut stamp = move || st;
    let r = match OPS[op] {
        "setm" => w.write_set_stamped(&np("m"), doc(&[("q", i(1)), ("p", i(2))]), &mut stamp),
        "swap" => w.write_replace_stamped(
            &fields(&[("b", i(2)), ("a", i(1)), ("m", doc(&[("p", i(1)), ("q", i(2))]))]),
            &mut stamp,
        ),
        "repl2" => w.write_replace_stamped(&fields(&[("c", i(1)), ("a", i(1))]), &mut stamp),
        "seta" => w.write_set_stamped(&np("a"), i(5), &mut stamp),
        "unseta" => w.write_unset_stamped(&np("a"), &mut stamp),
        "setc" => w.write_set_stamped(&np("c"), i(7), &mut stamp),
        "setmr" => w.write_set_stamped(&np("m.r"), i(3), &mut stamp),
        "unsetmp" => w.write_unset_stamped(&np("m.p"), &mut stamp),
        "unsetm" => w.write_unset_stamped(&np("m"), &mut stamp),
        "unsetb" => w.write_unset_stamped(&np("b"), &mut stamp),
        "inca" => w.write_inc_stamped(&np("a"), 1, &mut stamp),
        "incd" => w.write_inc_stamped(&np("d"), 1, &mut stamp),
        "setnull" => w.write_set_stamped(&np("b"), Value::Null, &mut stamp),
        "setmscalar" => w.write_set_stamped(&np("m"), i(5), &mut stamp),
        "incnew" => w.write_inc_stamped(&np("k.n"), 1, &mut stamp),
        update => {
            let ops = match update {
                "setmany" => vec![UpdateOp::Set(np("d"), i(1)), UpdateOp::Set(np("c"), i(2))],
                "upd" => vec![
                    UpdateOp::Set(np("d"), i(1)),
                    UpdateOp::Unset(np("a")),
                    UpdateOp::Inc(np("e"), i(1)),
                ],
                "upd2" => vec![UpdateOp::Inc(np("a"), i(1)), UpdateOp::Set(np("m.r"), i(3))],
                o => panic!("operation {o}"),
            };
            return match w.write_update_stamped(&ops, &mut stamp) {
                Ok(r) => r.is_some(),
                Err(UpdateError::Refused(_)) => false,
                Err(e) => panic!("the stamp {st:?} was refused: {e}"),
            };
        }
    };
    r.expect("the stamp is above everything the writer has seen").is_some()
}

/// A node of a history: (writer index, operation index in the writer).
type Node = (usize, usize);

struct History {
    /// The operations of A (two) and B (one), as indexes into `OPS`.
    ops: [Vec<usize>; 2],
    /// Before node (w, j), the writer holds the first `c` operations of `k` and their past: (k, c), in order.
    recv: BTreeMap<Node, Vec<(usize, usize)>>,
    /// The model's hash of the final ordered document after each stamp order, and how many were found ok.
    hashes: Vec<u64>,
    ok: usize,
    line: String,
}

impl History {
    fn nodes(&self) -> Vec<Node> {
        (0..2).flat_map(|w| (0..self.ops[w].len()).map(move |j| (w, j))).collect()
    }

    fn preds(&self, n: Node) -> Vec<Node> {
        let mut ps = Vec::new();
        if n.1 > 0 {
            ps.push((n.0, n.1 - 1));
        }
        for &(k, c) in self.recv.get(&n).into_iter().flatten() {
            ps.push((k, c - 1));
        }
        ps
    }

    /// Every linear extension of the happens-before order, in the checker's order (`History.linear_extensions`).
    fn extensions(&self) -> Vec<Vec<Node>> {
        fn rec(
            h: &History,
            nodes: &[Node],
            done: &mut Vec<Node>,
            order: &mut Vec<Node>,
            out: &mut Vec<Vec<Node>>,
        ) {
            if order.len() == nodes.len() {
                out.push(order.clone());
                return;
            }
            for &x in nodes {
                if !done.contains(&x) && h.preds(x).iter().all(|p| done.contains(p)) {
                    done.push(x);
                    order.push(x);
                    rec(h, nodes, done, order, out);
                    order.pop();
                    done.retain(|d| *d != x);
                }
            }
        }
        let nodes = self.nodes();
        let mut out = Vec::new();
        rec(self, &nodes, &mut Vec::new(), &mut Vec::new(), &mut out);
        out
    }
}

fn parse(line: &str) -> History {
    let mut parts = line.split(';');
    let idx: Vec<usize> = parts.next().unwrap().split(',').map(|x| x.parse().unwrap()).collect();
    let recv_s = parts.next().unwrap();
    let hashes_s = parts.next().unwrap();
    let ok = parts.next().unwrap().parse().unwrap();
    let who = |c: &str| match c {
        "A" => 0,
        "B" => 1,
        _ => panic!("writer {c}"),
    };
    let mut recv: BTreeMap<Node, Vec<(usize, usize)>> = BTreeMap::new();
    for tok in recv_s.split(',').filter(|t| !t.is_empty()) {
        let (at, from) = tok.split_once(':').unwrap();
        recv.entry((who(&at[..1]), at[1..].parse().unwrap()))
            .or_default()
            .push((who(&from[..1]), from[1..].parse().unwrap()));
    }
    let hashes = (0..hashes_s.len() / 16)
        .map(|k| u64::from_str_radix(&hashes_s[16 * k..16 * k + 16], 16).unwrap())
        .collect();
    History {
        ops: [idx[..2].to_vec(), idx[2..].to_vec()],
        recv,
        hashes,
        ok,
        line: line.to_string(),
    }
}

fn fnv(text: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// The model's `doc_text` (gen_min_a2b1.py): `{k:v,...}` in field order, `i<int>` for a number, `n` for a null.
fn text(d: &[(String, OrderedValue)]) -> String {
    let fields: Vec<String> = d
        .iter()
        .map(|(k, v)| {
            let v = match v {
                OrderedValue::Doc(kids) => text(kids),
                OrderedValue::Leaf(Value::Int64(n)) => format!("i{n}"),
                OrderedValue::Leaf(Value::Int32(n)) => format!("i{n}"),
                OrderedValue::Leaf(Value::Null) => "n".to_string(),
                OrderedValue::Leaf(o) => panic!("the class has no value {o:?}"),
            };
            format!("{k}:{v}")
        })
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// The model's final ordered document for a stamp order, replayed: the join of the base and both writers' final states in
/// two orders must agree (the model asserts it), and its text is returned.
fn replay(h: &History, pi: &[Node], base: &MergeState, o: &JoinOptions) -> String {
    let mut ws = [
        Writer::from_state(wid('A'), base.clone(), o.clone()),
        Writer::from_state(wid('B'), base.clone(), o.clone()),
    ];
    let mut entries: BTreeMap<Node, MergeState> = BTreeMap::new();
    let mut t = 1u64;
    for &(w, j) in pi {
        t += 1;
        for &(k, c) in h.recv.get(&(w, j)).into_iter().flatten() {
            let e = entries[&(k, c - 1)].clone();
            ws[w].receive_report(&e);
        }
        let st = MStamp { hlc: Hlc::new(t, 0), writer: ws[w].id };
        apply(&mut ws[w], h.ops[w][j], st);
        entries.insert((w, j), ws[w].state.clone());
    }
    let docs: Vec<String> = [[0usize, 1], [1, 0]]
        .iter()
        .map(|order| {
            let mut s = base.clone();
            for &w in order {
                s = join(&s, &ws[w].state, o).0;
            }
            text(&s.ordered_document(o).expect("the document is alive"))
        })
        .collect();
    assert_eq!(docs[0], docs[1], "the join order changes the document");
    docs[0].clone()
}

#[test]
fn the_fixture_is_the_reviewed_checkers() {
    let head: Vec<&str> = FIXTURE.lines().take_while(|l| l.starts_with('#')).collect();
    for want in [MODEL6_SHA256, PULLMODEL6_SHA256, BRUTE_SHA256] {
        assert!(head.iter().any(|l| l.ends_with(want)), "{want} is not in the fixture's header");
    }
    let ops = head.iter().find(|l| l.starts_with("# ops ")).unwrap();
    assert_eq!(&ops[6..], OPS.join(","));
}

#[test]
fn every_stamp_order_of_the_class_gives_the_models_ordered_document() {
    let o = JoinOptions::default();
    let mut first = Writer::new(wid('A'), o.clone());
    first
        .insert(&fields(&[("a", i(1)), ("b", i(2)), ("m", doc(&[("p", i(1)), ("q", i(2))]))]), 0)
        .unwrap();
    let base = first.state.clone();
    let (mut histories, mut serial, mut run, mut ok, mut uncompared) = (0, 0, 0, 0, 0);
    let mut bad: Vec<String> = Vec::new();
    for line in FIXTURE.lines().filter(|l| !l.starts_with('#')) {
        let h = parse(line);
        let exts = h.extensions();
        histories += 1;
        serial += exts.len();
        if h.hashes.is_empty() {
            uncompared += 1; // no valid serial order: the checker runs no stamp order
            continue;
        }
        assert_eq!(h.hashes.len(), exts.len(), "{}", h.line);
        ok += h.ok;
        for (pi, want) in exts.iter().zip(&h.hashes) {
            run += 1;
            let got = replay(&h, pi, &base, &o);
            if fnv(&got) != *want {
                let names: Vec<String> =
                    pi.iter().map(|(w, j)| format!("{}{j}", ["A", "B"][*w])).collect();
                let ops: Vec<String> = h
                    .ops
                    .iter()
                    .enumerate()
                    .map(|(w, v)| {
                        format!(
                            "{}: {}",
                            ["A", "B"][w],
                            v.iter().map(|k| OPS[*k]).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect();
                bad.push(format!(
                    "history [{}] receives {:?}, stamp order {}: the Writer gives {got} ({:016x}), the model {want:016x}  (fixture line {})",
                    ops.join("; "),
                    h.recv,
                    names.join(" "),
                    fnv(&got),
                    h.line
                ));
            }
        }
    }
    for b in bad.iter().take(5) {
        eprintln!("MISMATCH {b}");
    }
    // the checker's pinned counts (pins/MIN-A2B1.json): histories, serial orders enumerated, stamp orders run, ok
    assert_eq!((histories, serial, run, ok), (34_992, 58_320, 57_742, 56_687));
    assert_eq!(uncompared, 416, "histories with no valid serial order");
    assert!(bad.is_empty(), "{} stamp orders differ from the model", bad.len());
}
