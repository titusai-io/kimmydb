//! Differential test of `kimmy_core::merge_state` on NESTED documents against the stage A Python
//! reference model.
//!
//! `tests/merge_state_sim/reference_model/q7diff_gen6.py` (run with `python3 -I`) drives the
//! vendored stage A model (`model6_stageA.py`, `pullmodel6_stageA.py`, byte for byte the files of
//! the stage A snapshot, never edited) through random scripts of nested writes: `$set` at any depth
//! of scalars, arrays and sub-documents, `$unset` of leaves and subtrees, `$inc` below maps,
//! replaces, inserts, upserting `$inc`, deletes, writes below a scalar (which both sides refuse),
//! whole-state joins and loss entries, retentions from 3 to 10^9, and the real Int64 bound or a
//! lowered one. This test replays every script on `merge_state::Writer`s and compares, after every
//! step and for every member: the visible document (every present path, a map marked, the order of
//! a level not compared), the hidden figure, and a digest of the whole state (lives, deletes, dead
//! lives, registers with their observed totals and generations, counter slots, version vector, loss
//! records, as sorted text lines hashed together); every write's stamp (or refusal or no-op); every
//! join's `changed` flag and fresh loss keys; every loss entry's acceptance; and, after a final
//! full-mesh sync, the recursive harness oracle (`merge_state_sim/oracle.rs`) on the Rust log
//! against the model's oracle on the Python log.
//!
//! THE RESTRICTED SUBSET. The generator draws only what stage A2 does not change and what the Rust
//! side implements; none of these is lifted here: nulls (Null versus Unset), a map-creating `$inc`
//! and any leaf write under an absent map (the implicit marker), multi-operator updates (one
//! stamp), restore and covered-entry cases, order state and field order (no order-only write is
//! drawn and the order of a level is not compared), and the absorbed counter rule. The `same`
//! patch (a no-op is a write that leaves the stored representation byte-identical: doubles by
//! bit pattern) is applied by the generator to the imported module, never to the vendored copy.
//! Where the Rust side and the reference disagree OUTSIDE the restriction, the Rust side is checked
//! against the oracle, and the disagreement is a finding.
//!
//! ODD FIELD NAMES. The sim and the join-law tests take the names no dotted spelling can carry (the
//! empty name, `_`, a name with a dot); this differential takes only ONE of them, the empty name
//! (at the top and below a map: `a.` is the path `a`, ""), because the model's paths are dotted
//! strings and its whole-document record key is `_`: a dot in a name cannot be spelled, and `_`
//! would share its loss key with the whole document. The reasons, and why the empty name is safe
//! although the model keys the root's order state by "" too, are in
//! `reference_model/q7diff_gen6.py`, at `ODD_LEAVES`.
//!
//! `Q7DIFF_NESTED_RUNS=<n>` sets the number of scripts (default 300). `python3` must be on the
//! PATH: if it is not, the tests FAIL (they do not skip). No `merge-mutants` feature is needed
//! for the replay; with it, the test `rust_mutants_are_caught_by_the_replay` turns each nested
//! mutation switch on and requires the replay to disagree.

#[path = "common/fingerprint.rs"]
mod fingerprint;
#[path = "merge_state_sim/oracle.rs"]
#[allow(dead_code)]
mod oracle;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, LifeKind, LossKey, LossKind, LossValue, MStamp, MergeState, Num, Path,
    Value, Writer, WriterId, Written,
};
use serde_json::{Value as J, json};

fn wid(c: char, e: u64) -> WriterId {
    WriterId { node: NodeId::from_bytes([c as u8; 16]), epoch: Epoch::from_u64(e) }
}

fn epoch_of(st: &MStamp) -> u64 {
    u64::from_be_bytes(st.writer.epoch.0[10..18].try_into().unwrap())
}

fn node_of(st: &MStamp) -> char {
    st.writer.node.to_bytes()[0] as char
}

fn js(st: &MStamp) -> J {
    json!([st.hlc.wall_ms, node_of(st).to_string(), epoch_of(st)])
}

/// The digest form of a stamp (the generator prints the same).
fn fs(st: &MStamp) -> String {
    format!("{}:{}:{}", st.hlc.wall_ms, node_of(st), epoch_of(st))
}

fn fw(w: &WriterId) -> String {
    let e = u64::from_be_bytes(w.epoch.0[10..18].try_into().unwrap());
    format!("{}:{}", w.node.to_bytes()[0] as char, e)
}

fn kind_s(k: LossKind) -> &'static str {
    match k {
        LossKind::Clash => "clash",
        LossKind::DeleteWon => "deleteWon",
        LossKind::InsertLost => "insertLost",
        LossKind::RetiredWriter => "retiredWriter",
        LossKind::TypeClash => "typeClash",
        LossKind::ArrayClash => "arrayClash",
    }
}

fn jk(k: &LossKey) -> J {
    json!([kind_s(k.kind), k.at.to_string(), js(&k.stamp)])
}

/// Loss keys in the generator's order: kind name, field, stamp.
fn sort_keys(mut v: Vec<LossKey>) -> Vec<LossKey> {
    v.sort_by_key(|k| {
        (
            kind_s(k.kind),
            k.at.to_string(),
            k.stamp.hlc.wall_ms,
            node_of(&k.stamp),
            epoch_of(&k.stamp),
        )
    });
    v
}

fn jv(v: &Value) -> J {
    match v {
        Value::Map => json!(["m"]),
        Value::Int64(i) => json!(["i", i.to_string()]),
        Value::Double(d) => json!(["d", format!("{:016x}", d.to_bits())]),
        Value::String(s) => json!(["s", s]),
        Value::Array(xs) => json!(["a", xs.iter().map(jv).collect::<Vec<_>>()]),
        other => panic!("the reference model has no {other:?}"),
    }
}

fn unjv(v: &J) -> Value {
    let a = v.as_array().unwrap();
    match a[0].as_str().unwrap() {
        "i" => Value::Int64(a[1].as_str().unwrap().parse().unwrap()),
        "d" => {
            Value::Double(f64::from_bits(u64::from_str_radix(a[1].as_str().unwrap(), 16).unwrap()))
        }
        "s" => Value::String(a[1].as_str().unwrap().to_string()),
        "a" => Value::Array(a[1].as_array().unwrap().iter().map(unjv).collect()),
        "m" => Value::Document(
            a[1].as_array()
                .unwrap()
                .iter()
                .map(|kv| (kv[0].as_str().unwrap().to_string(), unjv(&kv[1])))
                .collect(),
        ),
        t => panic!("bad tag {t}"),
    }
}

fn jdoc(d: &Option<BTreeMap<Path, Value>>) -> J {
    match d {
        None => J::Null,
        Some(m) => J::Object(m.iter().map(|(k, v)| (k.to_string(), jv(v))).collect()),
    }
}

/// The script names a path by a dotted string; its names contain no dot, so the segments are the
/// parts (the reference model cannot represent a name with a dot).
fn pth(s: &str) -> Path {
    Path::new(s.split('.')).expect("a valid path")
}

/// A document value as the top-level fields a write takes.
fn top_fields(v: &J) -> Vec<(Path, Value)> {
    match unjv(v) {
        Value::Document(fs) => fs.into_iter().map(|(k, v)| (Path::flat(&k).unwrap(), v)).collect(),
        other => panic!("a document was expected, not {other:?}"),
    }
}

// ---- the digest of a state ---------------------------------------------------------------

fn fv(v: &Value) -> String {
    match v {
        Value::Unset => "u".into(),
        Value::Map => "m".into(),
        Value::Int64(i) => format!("i:{i}"),
        Value::Double(d) => format!("d:{:016x}", d.to_bits()),
        Value::String(s) => format!("s:{}", serde_json::to_string(s).unwrap()),
        Value::Array(xs) => format!("a[{}]", xs.iter().map(fv).collect::<Vec<_>>().join(",")),
        other => panic!("the reference model has no {other:?}"),
    }
}

fn total(n: &Num) -> String {
    match n {
        Num::Int32(i) | Num::Int64(i) => i.to_string(),
        Num::Decimal128(_) => panic!("the reference model has no decimals"),
    }
}

fn fg(g: &Option<MStamp>) -> String {
    g.as_ref().map_or("-".into(), fs)
}

fn fobs<'a>(it: impl Iterator<Item = (&'a (MStamp, WriterId, Option<MStamp>), String)>) -> String {
    let mut v: Vec<String> =
        it.map(|(k, val)| format!("{}|{}|{}={val}", fs(&k.0), fw(&k.1), fg(&k.2))).collect();
    v.sort();
    format!("[{}]", v.join(","))
}

fn digest_lines(s: &MergeState) -> Vec<String> {
    let mut out = Vec::new();
    for (l, v) in &s.lives {
        let mut seen: Vec<String> = v.seen.iter().map(fs).collect();
        seen.sort();
        let kind = match v.kind {
            LifeKind::Insert => "insert",
            LifeKind::Upsert => "upsert",
        };
        out.push(format!("life {} seen[{}] {kind}", fs(l), seen.join(",")));
    }
    for l in &s.dead {
        out.push(format!("dead {}", fs(l)));
    }
    for (d, vv) in &s.deletes {
        let mut v: Vec<String> =
            vv.iter().map(|(w, h)| format!("{}={}", fw(w), h.wall_ms)).collect();
        v.sort();
        out.push(format!("del {} vv[{}]", fs(d), v.join(",")));
    }
    for ((l, p), r) in &s.regs {
        let obs = fobs(r.obs.iter().map(|(k, (st, n))| (k, format!("{},{}", fs(st), total(n)))));
        let g = r.generation.as_ref().map_or("-".to_string(), |g| {
            format!("{}/seen{}", fs(&g.stamp), fobs(g.seen.iter().map(|(k, n)| (k, total(n)))))
        });
        out.push(format!(
            "reg {} {p} {} {} obs{obs} gen={g} soi={}",
            fs(l),
            fs(&r.stamp),
            fv(&r.value),
            u8::from(r.soi)
        ));
    }
    for ((l, p, w, g), sl) in &s.slots {
        out.push(format!(
            "slot {} {p} {} {} {} {}",
            fs(l),
            fw(w),
            fg(g),
            fs(&sl.stamp),
            total(&sl.total)
        ));
    }
    for (w, h) in &s.vv {
        out.push(format!("vv {} {}", fw(w), h.wall_ms));
    }
    for (k, val) in &s.losses {
        let vs = match val {
            LossValue::Value(v) => fv(v),
            LossValue::Total(n) => format!("i:{}", total(n)),
            LossValue::Fields(ps) => {
                let mut v: Vec<String> = ps.iter().map(|p| p.to_string()).collect();
                v.sort();
                format!("f[{}]", v.join(","))
            }
        };
        out.push(format!("loss {} {} {} {vs}", kind_s(k.kind), k.at, fs(&k.stamp)));
    }
    out.sort();
    out
}

fn rotl(x: u64, r: u32) -> u64 {
    x.rotate_left(r)
}

fn splitmix(z: u64) -> u64 {
    let z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A 128-bit hash of the text of a digest (the generator has the same function; it is a
/// comparison tool, not a format).
fn hash128(data: &[u8]) -> String {
    let (p1, p2) = (0x9E37_79B1_85EB_CA87_u64, 0xC2B2_AE3D_27D4_EB4F_u64);
    let (mut a, mut b) = (0x243F_6A88_85A3_08D3_u64, 0x1319_8A2E_0370_7344_u64);
    for chunk in data.chunks(8) {
        let mut w = [0u8; 8];
        w[..chunk.len()].copy_from_slice(chunk);
        let v = u64::from_be_bytes(w);
        a = rotl((a ^ v).wrapping_mul(p1), 29);
        b = (rotl(b, 31) ^ v).wrapping_mul(p2);
        a ^= b >> 17;
        b ^= a >> 23;
    }
    let n = data.len() as u64;
    let x = splitmix(a ^ n);
    let y = splitmix(b ^ rotl(n, 32) ^ x);
    format!("{x:016x}{y:016x}")
}

fn digest(s: &MergeState) -> String {
    hash128(digest_lines(s).join("\n").as_bytes())
}

// ---- replaying a script ------------------------------------------------------------------

struct Mem {
    w: Writer,
    contained: BTreeSet<MStamp>,
}

impl Mem {
    fn logged(&mut self, wr: Option<Written>, log: &mut oracle::Log) -> Option<MStamp> {
        let wr = wr?;
        log.insert(wr.stamp, (self.contained.clone(), wr.record));
        self.contained.insert(wr.stamp);
        Some(wr.stamp)
    }
}

fn snap(ms: &[Mem]) -> Vec<J> {
    ms.iter()
        .map(|m| {
            json!({
                "v": jdoc(&m.w.visible()),
                "hid": J::Object(m.w.hidden_all().iter().map(|(p, h)| (p.to_string(), J::String(h.to_string()))).collect()),
                "dg": digest(&m.w.state),
            })
        })
        .collect()
}

fn opt_stamp_j(s: Option<MStamp>) -> J {
    s.map_or(J::Null, |s| js(&s))
}

fn sorted_heads(h: impl IntoIterator<Item = (Path, MStamp)>) -> Vec<J> {
    // the model spells the whole document `_`; the oracle spells it so as never to be a field's name
    let mut v: Vec<(Path, MStamp)> = h
        .into_iter()
        .map(|(p, st)| {
            (if p == oracle::whole_document() { Path::flat("_").unwrap() } else { p }, st)
        })
        .collect();
    v.sort_by_key(|(p, st)| (p.to_string(), st.hlc.wall_ms, node_of(st), epoch_of(st)));
    v.iter().map(|(p, st)| json!([p.to_string(), js(st)])).collect()
}

/// With `Q7DIFF_EXPLAIN=1`: the digest lines the model has for member `k` after step `i` of the
/// script, against `rust`, as the lines only one side has.
fn explain(sc: &J, i: usize, k: usize, rust: &[String]) -> String {
    if std::env::var("Q7DIFF_EXPLAIN").is_err() {
        return String::new();
    }
    let seed = sc["seed"].as_u64().unwrap().to_string();
    let out =
        python(&["--explain".into(), seed, i.to_string()]).wait_with_output().expect("python3 ran");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let mut members = text.split("== member ").skip(1);
    let py: Vec<&str> = members.nth(k).map_or(Vec::new(), |m| m.lines().skip(1).collect());
    let only_rust: Vec<&String> = rust.iter().filter(|l| !py.contains(&l.as_str())).collect();
    let only_py: Vec<&&str> = py.iter().filter(|l| !rust.contains(&l.to_string())).collect();
    format!("\n  only in rust: {only_rust:#?}\n  only in the model: {only_py:#?}")
}

/// What the replay of one script found.
struct Replay {
    bad: Vec<String>,
    ops: BTreeMap<String, usize>,
    /// The fingerprint of the script: every member's state and reads after every step, and every
    /// step's result (see `common/fingerprint.rs`).
    fp: u64,
}

/// Replay one script with `opts`; the mismatches against the model's answers.
fn run_script(sc: &J, tweak: &dyn Fn(&mut JoinOptions)) -> Replay {
    let ret = sc["ret"].as_u64().unwrap();
    let bound = sc["bound"].as_i64().unwrap();
    let nm = sc["members"].as_u64().unwrap() as usize;
    let mut opts = JoinOptions::new(ret, bound).expect("a valid bound");
    // the stage-A model has no field order: a write that only reorders is a no-op there
    opts.set_ord_reorder_is_noop(true);
    tweak(&mut opts);
    let mut ms: Vec<Mem> = "ABCD"
        .chars()
        .take(nm)
        .map(|c| Mem { w: Writer::new(wid(c, 0), opts.clone()), contained: BTreeSet::new() })
        .collect();
    let mut log = oracle::Log::new();
    let mut rep = Replay { bad: Vec::new(), ops: BTreeMap::new(), fp: 0 };
    let mut fp = fingerprint::Fp::new();
    let ops = sc["ops"].as_array().unwrap();
    let exp = sc["expect"]["steps"].as_array().unwrap();
    for (i, (op, want)) in ops.iter().zip(exp).enumerate() {
        let kind = op[0].as_str().unwrap();
        *rep.ops.entry(kind.to_string()).or_default() += 1;
        let m = |k: usize| op[k].as_u64().unwrap() as usize;
        let res: J = match kind {
            "insert" => {
                let r = ms[m(1)].w.insert(&top_fields(&op[2]), op[3].as_u64().unwrap());
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "upsert" => {
                let r = ms[m(1)].w.upsert_inc(
                    &pth(op[2].as_str().unwrap()),
                    op[3].as_i64().unwrap(),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "replace" => {
                let r = ms[m(1)].w.write_replace(&top_fields(&op[2]), op[3].as_u64().unwrap());
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "inc" => {
                let r = ms[m(1)].w.write_inc(
                    &pth(op[2].as_str().unwrap()),
                    op[3].as_i64().unwrap(),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "set" => {
                let r = ms[m(1)].w.write_set(
                    &pth(op[2].as_str().unwrap()),
                    unjv(&op[3]),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "unset" => {
                let r =
                    ms[m(1)].w.write_unset(&pth(op[2].as_str().unwrap()), op[3].as_u64().unwrap());
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "delete" => {
                let r = ms[m(1)].w.write_delete(op[2].as_u64().unwrap());
                opt_stamp_j(ms[m(1)].logged(r, &mut log))
            }
            "join" => {
                let (r, s) = (m(1), m(2));
                let other = ms[s].w.state.clone();
                let oc = ms[s].contained.clone();
                let rec = ms[r].w.receive_report(&other);
                ms[r].contained.extend(oc);
                json!({"changed": rec.changed, "fresh": sort_keys(rec.fresh).iter().map(jk).collect::<Vec<_>>()})
            }
            "loss" => {
                let (r, s, pick) = (m(1), m(2), m(3));
                let keys = sort_keys(ms[s].w.state.losses.keys().cloned().collect());
                if keys.is_empty() {
                    J::Null
                } else {
                    let key = keys[pick % keys.len()].clone();
                    let val = ms[s].w.state.losses[&key].clone();
                    let added = ms[r].w.receive_loss(key.clone(), val);
                    json!({"key": jk(&key), "added": added})
                }
            }
            t => panic!("bad op {t}"),
        };
        if res != want["res"] {
            rep.bad.push(format!("step {i} {op}: result rust {res} python {}", want["res"]));
            return rep;
        }
        fp.text(&res.to_string());
        for m in &ms {
            fp.state(&m.w.state, &m.w.opts);
        }
        let got = snap(&ms);
        let wa = want["after"].as_array().unwrap();
        for (k, (g, w)) in got.iter().zip(wa).enumerate() {
            if g != w {
                let field = ["v", "hid", "dg"].into_iter().find(|f| g[f] != w[f]).unwrap();
                let lines = digest_lines(&ms[k].w.state);
                rep.bad.push(format!(
                    "step {i} {op}: member {k}: {field} differs: rust {} python {}\n  rust state lines:\n    {}{}",
                    g[field],
                    w[field],
                    lines.join("\n    "),
                    if field == "dg" { explain(sc, i, k, &lines) } else { String::new() }
                ));
                return rep;
            }
        }
    }
    // final full-mesh sync
    for _ in 0..12 {
        let before: Vec<Vec<u8>> = ms.iter().map(|m| m.w.state.compact_bytes()).collect();
        for r in 0..nm {
            for s in 0..nm {
                if r != s {
                    let other = ms[s].w.state.clone();
                    let oc = ms[s].contained.clone();
                    ms[r].w.receive_report(&other);
                    ms[r].contained.extend(oc);
                }
            }
        }
        if ms.iter().map(|m| m.w.state.compact_bytes()).collect::<Vec<_>>() == before {
            break;
        }
    }
    for m in &ms {
        fp.state(&m.w.state, &m.w.opts);
    }
    rep.fp = fp.value();
    let fin = &sc["expect"]["final"];
    let got = snap(&ms);
    if J::Array(got.clone()) != fin["after"] {
        let k = (0..nm).find(|k| got[*k] != fin["after"][*k]).unwrap();
        rep.bad.push(format!("final sync, member {k}: rust {} python {}", got[k], fin["after"][k]));
        return rep;
    }
    // the oracles: the harness's Rust port on the Rust log, against the model's on the Python log
    let po = &fin["oracle"];
    let (doc, alive, dels) = oracle::oracle(&log, ret, bound);
    let mut cmp = |what: &str, r: J, p: &J| {
        if r != *p {
            rep.bad.push(format!("oracle {what}: rust {r} python {p}"));
        }
    };
    cmp("doc", jdoc(&doc), &po["doc"]);
    let mut av: Vec<MStamp> = alive.iter().copied().collect();
    av.sort_by_key(|s| (s.hlc.wall_ms, node_of(s), epoch_of(s)));
    cmp("alive", J::Array(av.iter().map(js).collect()), &po["alive"]);
    let mut dv = dels.clone();
    dv.sort_by_key(|s| (s.hlc.wall_ms, node_of(s), epoch_of(s)));
    cmp("dels", J::Array(dv.iter().map(js).collect()), &po["dels"]);
    let must = oracle::loss_oracle(&log, &alive, &dels, doc.as_ref(), ret);
    cmp("must", J::Array(sorted_heads(must)), &po["must"]);
    let hid = oracle::hidden_oracle(&log, ret);
    cmp(
        "hidden",
        J::Object(hid.iter().map(|(p, h)| (p.to_string(), J::String(h.to_string()))).collect()),
        &po["hidden"],
    );
    let mut ksa: Vec<Vec<J>> =
        oracle::killed_superseded_any(&log, ret).into_iter().map(sorted_heads).collect();
    ksa.sort_by_key(|v| {
        v.iter()
            .map(|x| {
                (
                    x[0].as_str().unwrap().to_string(),
                    x[1][0].as_u64().unwrap(),
                    x[1][1].as_str().unwrap().to_string(),
                    x[1][2].as_u64().unwrap(),
                )
            })
            .collect::<Vec<_>>()
    });
    cmp("ksa", json!(ksa), &po["ksa"]);
    let have: BTreeSet<LossKey> = ms[0].w.state.losses.keys().cloned().collect();
    let have_sorted = sort_keys(have.iter().cloned().collect());
    cmp("have", J::Array(have_sorted.iter().map(jk).collect()), &po["have"]);
    let mut stale: Vec<(LossKind, Path, MStamp)> = oracle::stale_records(&have, &log, ret);
    stale.sort_by_key(|(k, p, s)| {
        (kind_s(*k), p.to_string(), s.hlc.wall_ms, node_of(s), epoch_of(s))
    });
    cmp(
        "stale",
        J::Array(stale.iter().map(|(k, p, s)| json!([kind_s(*k), p.to_string(), js(s)])).collect()),
        &po["stale"],
    );
    let unsound: Vec<J> = have_sorted
        .iter()
        .filter(|k| !oracle::sound_record(k, &log, &alive, &dels, ret))
        .map(jk)
        .collect();
    cmp("unsound", J::Array(unsound), &po["unsound"]);
    rep
}

// ---- running the generator ---------------------------------------------------------------

fn generator() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/merge_state_sim/reference_model/q7diff_gen6.py")
}

fn python(args: &[String]) -> std::process::Child {
    Command::new("python3")
        .arg("-I")
        .arg(generator())
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "merge_state_diff_nested needs `python3` on the PATH (standard library only) to \
                 run the reference model, and could not start it: {e}"
            )
        })
}

fn workers() -> usize {
    std::thread::available_parallelism().map_or(2, |n| n.get()).clamp(1, 4)
}

/// A fresh scratch directory, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let dir =
            std::env::temp_dir().join(format!("q7diffn-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Generate `n` scripts (seeds 0..n) into `dir`, `workers()` python processes at a time.
fn generate(dir: &std::path::Path, n: u64, plant: Option<&str>) {
    let w = workers() as u64;
    let per = n.div_ceil(w);
    let mut kids = Vec::new();
    for k in 0..w {
        let (first, count) = (k * per, per.min(n.saturating_sub(k * per)));
        if count == 0 {
            continue;
        }
        let mut args = vec![dir.display().to_string(), count.to_string(), first.to_string()];
        if let Some(p) = plant {
            args.extend(["--plant".to_string(), p.to_string()]);
        }
        kids.push((args.clone(), python(&args)));
    }
    for (args, kid) in kids {
        let out = kid.wait_with_output().expect("python3 ran");
        assert!(
            out.status.success(),
            "q7diff_gen6.py {args:?} failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

struct Verdict {
    scripts: usize,
    steps: usize,
    fails: Vec<String>,
    ops: BTreeMap<String, usize>,
    /// The sum of the scripts' fingerprints, so that it does not depend on the threads.
    fp: u64,
}

/// Replay every script of `dir` on the Rust side, in `workers()` threads.
fn replay(dir: &std::path::Path, tweak: &(dyn Fn(&mut JoinOptions) + Sync)) -> Verdict {
    let mut paths: Vec<PathBuf> =
        std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    let chunk = paths.len().div_ceil(workers()).max(1);
    let parts: Vec<Verdict> = std::thread::scope(|sc| {
        let hs: Vec<_> = paths
            .chunks(chunk)
            .map(|ps| {
                sc.spawn(move || {
                    let mut v = Verdict {
                        scripts: 0,
                        steps: 0,
                        fails: Vec::new(),
                        ops: BTreeMap::new(),
                        fp: 0,
                    };
                    for p in ps {
                        let sc: J = serde_json::from_str(&std::fs::read_to_string(p).unwrap())
                            .expect("a script");
                        v.scripts += 1;
                        v.steps += sc["ops"].as_array().unwrap().len();
                        let r = run_script(&sc, tweak);
                        v.fp = v.fp.wrapping_add(r.fp);
                        for (k, n) in r.ops {
                            *v.ops.entry(k).or_default() += n;
                        }
                        if !r.bad.is_empty() {
                            v.fails.push(format!("{}:\n{}", p.display(), r.bad.join("\n")));
                        }
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("replay thread")).collect()
    });
    let mut all = Verdict { scripts: 0, steps: 0, fails: Vec::new(), ops: BTreeMap::new(), fp: 0 };
    for v in parts {
        all.scripts += v.scripts;
        all.steps += v.steps;
        all.fails.extend(v.fails);
        all.fp = all.fp.wrapping_add(v.fp);
        for (k, n) in v.ops {
            *all.ops.entry(k).or_default() += n;
        }
    }
    all
}

/// The fingerprint of the default 300 scripts. Taken before the performance pass of Q7 step 0 and
/// re-pinned once, when the odd field names (the empty name, a name with a dot) joined the
/// generator's pools: the pools grew, the code did not change. The old code (`8e36e8de`) gives this
/// value on the new pools, as the new code does (the equivalence test of the D7 pass, retired when the order state changed the join, pinned it). Any
/// other change to it is a change of behaviour, never to be re-pinned for speed.
const PINNED_300: u64 = 0xf05c_9aba_cf02_eb9c;

fn runs() -> u64 {
    std::env::var("Q7DIFF_NESTED_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(300)
}

#[test]
fn nested_differential_against_the_python_model() {
    let n = runs();
    let scratch = Scratch::new("run");
    generate(&scratch.0, n, None);
    let v = replay(&scratch.0, &|_| {});
    println!(
        "nested differential: {} scripts, {} steps, ops {:?}: {} scripts disagree",
        v.scripts,
        v.steps,
        v.ops,
        v.fails.len()
    );
    println!("nested differential fingerprint ({n} scripts): {:#018x}", v.fp);
    if n == 300 {
        assert_eq!(v.fp, PINNED_300, "a state or a read changed");
    }
    assert_eq!(v.scripts as u64, n, "every script was replayed");
    for f in v.fails.iter().take(5) {
        println!("MISMATCH {f}");
    }
    assert!(v.fails.is_empty(), "{} scripts disagree with the Python model", v.fails.len());
}

/// Generate `scripts` scripts from the model with `plant` applied: the replay must disagree.
fn planted(plant: &str, scripts: u64) {
    let scratch = Scratch::new(plant);
    generate(&scratch.0, scripts, Some(plant));
    let v = replay(&scratch.0, &|_| {});
    println!("planted {plant}: {} of {} scripts disagree", v.fails.len(), v.scripts);
    assert!(
        !v.fails.is_empty(),
        "the planted change {plant} was not detected in {scripts} scripts"
    );
}

// The self-check of the test itself: a switch of the model's own flipped by the generator must make
// the replay disagree, or the comparison above would be vacuous. The seeds are fixed, so these are
// exact.

#[test]
fn a_planted_scalar_that_hides_newer_children_is_detected() {
    planted("scalar-hides-newer", 150);
}

#[test]
fn a_planted_marker_that_hides_its_children_is_detected() {
    planted("marker-hides", 60);
}

#[test]
fn a_planted_rewrite_of_unchanged_leaves_is_detected() {
    planted("rewrite-unchanged", 60);
}

/// The Rust mutation switches the nested replay must notice, by name. A switch that this replay
/// cannot see is listed in `mutants.rs` with the grader that does.
#[cfg(feature = "merge-mutants")]
#[test]
fn rust_mutants_are_caught_by_the_replay() {
    type Switch = fn(&mut JoinOptions);
    let cases: [(&str, Switch); 6] = [
        ("a scalar hides every child, even a newer one", |o| o.set_type_ignores_recency(true)),
        ("a map marker hides its children", |o| o.set_marker_hides(true)),
        ("no typeClash is reported", |o| o.set_no_type_clash(true)),
        ("two arrays are a plain clash", |o| o.set_array_clash_as_clash(true)),
        ("a sub-document write rewrites every leaf", |o| o.set_rewrite_unchanged(true)),
        ("an unset of a map unsets its direct children only", |o| o.set_unset_shallow(true)),
    ];
    let scratch = Scratch::new("mut");
    let n = runs().min(300);
    generate(&scratch.0, n, None);
    let base = replay(&scratch.0, &|_| {});
    assert!(base.fails.is_empty(), "the unmutated replay agrees: {:?}", base.fails.first());
    for (what, sw) in cases {
        let v = replay(&scratch.0, &sw);
        println!("rust mutant `{what}`: {} of {} scripts disagree", v.fails.len(), v.scripts);
        assert!(!v.fails.is_empty(), "the replay missed the Rust mutant: {what}");
    }
}
