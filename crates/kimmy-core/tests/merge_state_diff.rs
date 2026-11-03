//! Differential test of `kimmy_core::merge_state` against the Python reference model.
//!
//! `tests/merge_state_sim/reference_model/q7diff_gen.py` (run with `python3 -I`) drives the
//! reference model through random scripts: every write kind, whole-state joins, loss entries,
//! backups and restores with a new epoch, retentions from 3 to 10^9, the real Int64 bound or a
//! lowered one, and values with strings, doubles, `-0.0`, `2.0`, Int64-bound numbers and `None`.
//! This test replays every script on `merge_state::Writer`s and compares, after every step and for
//! every member: the state hash of the canonical bytes, their length, the visible document and
//! the hidden figure; every write's stamp (or refusal); every join's `changed` flag and fresh loss
//! keys; every loss entry's acceptance; and, after a final full-mesh sync, the harness oracle
//! (`merge_state_sim/oracle.rs`) on the Rust log against the model's oracle on the Python log.
//!
//! The Python side encodes its state in the Rust canonical format (`codec.rs` ported), so the
//! hashes also pin the format against an independent implementation, and the same for the compact
//! form (`compact.rs`, written again in Python from its documentation: `ch` and `clen`);
//! `golden_vectors_match_the_model` does the same for the golden vectors of `merge_state/tests.rs`.
//!
//! `Q7DIFF_RUNS=<n>` sets the number of scripts (default 300, about a minute in a debug build).
//! `python3` must be on the PATH: if it is not, the tests FAIL (they do not skip).
//!
//! No `merge-mutants` feature is needed: nothing here reads a mutation switch.

#[path = "merge_state_sim/oracle.rs"]
#[allow(dead_code)]
mod oracle;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, LossKey, LossKind, MStamp, MergeState, Path, Value, Writer, WriterId,
    Written,
};
use serde_json::{Value as J, json};

fn wid(c: char, e: u64) -> WriterId {
    WriterId { node: NodeId::from_bytes([c as u8; 16]), epoch: Epoch::from_u64(e) }
}

fn js(st: &MStamp) -> J {
    let e = u64::from_be_bytes(st.writer.epoch.0[10..18].try_into().unwrap());
    let c = st.writer.node.to_bytes()[0] as char;
    json!([st.hlc.wall_ms, c.to_string(), e])
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

fn jv(v: &Value) -> J {
    match v {
        Value::Unset => json!(["n"]),
        Value::Int64(i) => json!(["i", i.to_string()]),
        Value::Double(d) => json!(["d", format!("{:016x}", d.to_bits())]),
        Value::String(s) => json!(["s", s]),
        // The flat reference model has these four types only; the other BSON types are
        // tested in `merge_state::tests` and the codec tests.
        other => panic!("the reference model has no {other:?}"),
    }
}

fn unjv(v: &J) -> Value {
    let a = v.as_array().unwrap();
    match a[0].as_str().unwrap() {
        "n" => Value::Unset,
        "i" => Value::Int64(a[1].as_str().unwrap().parse().unwrap()),
        "d" => {
            Value::Double(f64::from_bits(u64::from_str_radix(a[1].as_str().unwrap(), 16).unwrap()))
        }
        "s" => Value::String(a[1].as_str().unwrap().to_string()),
        t => panic!("bad tag {t}"),
    }
}

fn jdoc(d: &Option<BTreeMap<Path, Value>>) -> J {
    match d {
        None => J::Null,
        Some(m) => J::Object(m.iter().map(|(k, v)| (k.to_string(), jv(v))).collect()),
    }
}

/// The script names a path by a dotted string; its names contain no dot, so the segments are
/// the parts (the reference model cannot represent a name with a dot).
fn pth(s: &str) -> Path {
    Path::new(s.split('.')).expect("a valid path")
}

fn fields(v: &J) -> Vec<(Path, Value)> {
    v.as_array().unwrap().iter().map(|kv| (pth(kv[0].as_str().unwrap()), unjv(&kv[1]))).collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

struct Mem {
    name: char,
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
            let cb = m.w.state.canonical_bytes();
            json!({
                "h": hex(&m.w.state.state_hash()),
                "len": cb.len(),
                "ch": hex(&m.w.state.compact_hash()),
                "clen": m.w.state.compact_bytes().len(),
                "v": jdoc(&m.w.visible()),
                "hid": J::Object(m.w.hidden_all().iter().map(|(p, h)| (p.to_string(), J::String(h.to_string()))).collect()),
            })
        })
        .collect()
}

fn opt_stamp_j(s: Option<MStamp>) -> J {
    s.map_or(J::Null, |s| js(&s))
}

fn sorted_heads(h: impl IntoIterator<Item = (Path, MStamp)>) -> Vec<J> {
    let mut v: Vec<(Path, MStamp)> = h.into_iter().collect();
    v.sort();
    v.iter().map(|(p, st)| json!([p.to_string(), js(st)])).collect()
}

/// Returns a list of mismatch descriptions (empty when the script agrees).
fn run_script(sc: &J) -> (Vec<String>, BTreeMap<&'static str, usize>) {
    let ret = sc["ret"].as_u64().unwrap();
    let bound = sc["bound"].as_i64().unwrap();
    let nm = sc["members"].as_u64().unwrap() as usize;
    let opts = JoinOptions::new(ret, bound).expect("a valid bound");
    let mut ms: Vec<Mem> = "ABCD"
        .chars()
        .take(nm)
        .map(|c| Mem {
            name: c,
            w: Writer::new(wid(c, 0), opts.clone()),
            contained: BTreeSet::new(),
        })
        .collect();
    let mut saved: Vec<Option<(MergeState, BTreeSet<MStamp>)>> = vec![None; nm];
    let mut log = oracle::Log::new();
    let mut bad = Vec::new();
    let mut stats: BTreeMap<&'static str, usize> = BTreeMap::new();
    let ops = sc["ops"].as_array().unwrap();
    let exp = sc["expect"]["steps"].as_array().unwrap();
    for (i, (op, want)) in ops.iter().zip(exp).enumerate() {
        let kind = op[0].as_str().unwrap();
        *stats
            .entry(match kind {
                "insert" => "insert",
                "upsert" => "upsert",
                "upsert_soi" => "upsert_soi",
                "replace" => "replace",
                "inc" => "inc",
                "set" => "set",
                "unset" => "unset",
                "delete" => "delete",
                "join" => "join",
                "loss" => "loss",
                "backup" => "backup",
                _ => "restore",
            })
            .or_default() += 1;
        let res: J = match kind {
            "insert" => {
                let m = op[1].as_u64().unwrap() as usize;
                let f = fields(&op[2]);
                let r = ms[m].w.insert(&f, op[3].as_u64().unwrap());
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "upsert" => {
                let m = op[1].as_u64().unwrap() as usize;
                let r = ms[m].w.upsert_inc(
                    &pth(op[2].as_str().unwrap()),
                    op[3].as_i64().unwrap(),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "upsert_soi" => {
                let m = op[1].as_u64().unwrap() as usize;
                let f = fields(&op[2]);
                let s = fields(&op[5]);
                let inc_path = pth(op[3].as_str().unwrap());
                let sk = op[6].as_u64().unwrap();
                // as the harness does it: the creation, logged, then the increment, logged
                let created = ms[m].w.upsert_soi_create(&f, &inc_path, &s, sk);
                match ms[m].logged(created, &mut log) {
                    None => J::Null,
                    Some(s1) => {
                        let incd =
                            ms[m].w.upsert_inc_on(s1, &inc_path, op[4].as_i64().unwrap(), sk);
                        opt_stamp_j(ms[m].logged(incd, &mut log))
                    }
                }
            }
            "replace" => {
                let m = op[1].as_u64().unwrap() as usize;
                let f = fields(&op[2]);
                let r = ms[m].w.write_replace(&f, op[3].as_u64().unwrap());
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "inc" => {
                let m = op[1].as_u64().unwrap() as usize;
                let r = ms[m].w.write_inc(
                    &pth(op[2].as_str().unwrap()),
                    op[3].as_i64().unwrap(),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "set" => {
                let m = op[1].as_u64().unwrap() as usize;
                let r = ms[m].w.write_set(
                    &pth(op[2].as_str().unwrap()),
                    unjv(&op[3]),
                    op[4].as_u64().unwrap(),
                );
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "unset" => {
                let m = op[1].as_u64().unwrap() as usize;
                let r = ms[m].w.write_unset(&pth(op[2].as_str().unwrap()), op[3].as_u64().unwrap());
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "delete" => {
                let m = op[1].as_u64().unwrap() as usize;
                let r = ms[m].w.write_delete(op[2].as_u64().unwrap());
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "join" => {
                let (r, s) = (op[1].as_u64().unwrap() as usize, op[2].as_u64().unwrap() as usize);
                let other = ms[s].w.state.clone();
                let oc = ms[s].contained.clone();
                let rec = ms[r].w.receive_report(&other);
                ms[r].contained.extend(oc);
                json!({"changed": rec.changed, "fresh": rec.fresh.iter().map(jk).collect::<Vec<_>>()})
            }
            "loss" => {
                let (r, s) = (op[1].as_u64().unwrap() as usize, op[2].as_u64().unwrap() as usize);
                let pick = op[3].as_u64().unwrap() as usize;
                let keys: Vec<LossKey> = ms[s].w.state.losses.keys().cloned().collect();
                if keys.is_empty() {
                    J::Null
                } else {
                    let key = keys[pick % keys.len()].clone();
                    let val = ms[s].w.state.losses[&key].clone();
                    let added = ms[r].w.receive_loss(key.clone(), val);
                    json!({"key": jk(&key), "added": added})
                }
            }
            "backup" => {
                let m = op[1].as_u64().unwrap() as usize;
                saved[m] = Some((ms[m].w.state.clone(), ms[m].contained.clone()));
                J::Null
            }
            "restore" => {
                let (m, o, ep) = (
                    op[1].as_u64().unwrap() as usize,
                    op[2].as_u64().unwrap() as usize,
                    op[3].as_u64().unwrap(),
                );
                match saved[m].clone() {
                    None => J::Null,
                    Some((st, ct)) => {
                        let other = ms[m].w.state.clone();
                        let oc = ms[m].contained.clone();
                        ms[o].w.receive_report(&other);
                        ms[o].contained.extend(oc);
                        ms[m].w.state = st;
                        ms[m].contained = ct;
                        ms[m].w.id = wid(ms[m].name, ep);
                        J::Bool(true)
                    }
                }
            }
            t => panic!("bad op {t}"),
        };
        if res != want["res"] {
            bad.push(format!("step {i} {op}: result rust {res} python {}", want["res"]));
            return (bad, stats);
        }
        let got = snap(&ms);
        let wa = want["after"].as_array().unwrap();
        for (k, (g, w)) in got.iter().zip(wa).enumerate() {
            if g != w {
                bad.push(format!(
                    "step {i} {op}: member {k}: rust {g} python {w}\n  rust state: {:?}",
                    ms[k].w.state
                ));
                return (bad, stats);
            }
        }
    }
    // final full-mesh sync
    for _ in 0..12 {
        let before: Vec<Vec<u8>> = ms.iter().map(|m| m.w.state.canonical_bytes()).collect();
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
        if ms.iter().map(|m| m.w.state.canonical_bytes()).collect::<Vec<_>>() == before {
            break;
        }
    }
    let fin = &sc["expect"]["final"];
    let got = snap(&ms);
    if J::Array(got.clone()) != fin["after"] {
        bad.push(format!("final sync: rust {got:?} python {}", fin["after"]));
        return (bad, stats);
    }
    // the oracles: the harness's Rust port on the Rust log, against model5 on the Python log
    let po = &fin["oracle"];
    let (doc, alive, dels) = oracle::oracle(&log, ret, bound);
    let mut cmp = |what: &str, r: J, p: &J| {
        if r != *p {
            bad.push(format!("oracle {what}: rust {r} python {p}"));
        }
    };
    cmp("doc", jdoc(&doc), &po["doc"]);
    cmp("alive", J::Array(alive.iter().map(js).collect()), &po["alive"]);
    let mut dv = dels.clone();
    dv.sort();
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
    cmp("have", J::Array(have.iter().map(jk).collect()), &po["have"]);
    let mut stale: Vec<(LossKind, Path, MStamp)> = oracle::stale_records(&have, &log, ret);
    stale.sort();
    cmp(
        "stale",
        J::Array(stale.iter().map(|(k, p, s)| json!([kind_s(*k), p.to_string(), js(s)])).collect()),
        &po["stale"],
    );
    let unsound: Vec<J> = have
        .iter()
        .filter(|k| !oracle::sound_record(k, &log, &alive, &dels, ret))
        .map(jk)
        .collect();
    cmp("unsound", J::Array(unsound), &po["unsound"]);
    (bad, stats)
}

// ---- running the generator ---------------------------------------------------------------

fn generator() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/merge_state_sim/reference_model/q7diff_gen.py")
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
                "merge_state_diff needs `python3` on the PATH (standard library only) to run the \
                 reference model, and could not start it: {e}"
            )
        })
}

fn run_python(args: &[String]) -> String {
    let out = python(args).wait_with_output().expect("python3 ran");
    assert!(
        out.status.success(),
        "q7diff_gen.py {args:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
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
        let dir = std::env::temp_dir().join(format!("q7diff-{tag}-{}-{nanos}", std::process::id()));
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
            "q7diff_gen.py {args:?} failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

struct Verdict {
    scripts: usize,
    steps: usize,
    fails: Vec<String>,
    ops: BTreeMap<&'static str, usize>,
}

/// Replay every script of `dir` on the Rust side, in `workers()` threads.
fn replay(dir: &std::path::Path) -> Verdict {
    let mut paths: Vec<PathBuf> =
        std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    let chunk = paths.len().div_ceil(workers()).max(1);
    let parts: Vec<Verdict> = std::thread::scope(|sc| {
        let hs: Vec<_> = paths
            .chunks(chunk)
            .map(|ps| {
                sc.spawn(move || {
                    let mut v =
                        Verdict { scripts: 0, steps: 0, fails: Vec::new(), ops: BTreeMap::new() };
                    for p in ps {
                        let sc: J = serde_json::from_str(&std::fs::read_to_string(p).unwrap())
                            .expect("a script");
                        v.scripts += 1;
                        v.steps += sc["ops"].as_array().unwrap().len();
                        let (bad, st) = run_script(&sc);
                        for (k, n) in st {
                            *v.ops.entry(k).or_default() += n;
                        }
                        if !bad.is_empty() {
                            v.fails.push(format!("{}:\n{}", p.display(), bad.join("\n")));
                        }
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("replay thread")).collect()
    });
    let mut all = Verdict { scripts: 0, steps: 0, fails: Vec::new(), ops: BTreeMap::new() };
    for v in parts {
        all.scripts += v.scripts;
        all.steps += v.steps;
        all.fails.extend(v.fails);
        for (k, n) in v.ops {
            *all.ops.entry(k).or_default() += n;
        }
    }
    all
}

#[test]
fn differential_against_the_python_model() {
    let n: u64 = std::env::var("Q7DIFF_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let scratch = Scratch::new("run");
    generate(&scratch.0, n, None);
    let v = replay(&scratch.0);
    println!(
        "differential: {} scripts, {} steps, ops {:?}: {} scripts disagree",
        v.scripts,
        v.steps,
        v.ops,
        v.fails.len()
    );
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
    let v = replay(&scratch.0);
    println!("planted {plant}: {} of {} scripts disagree", v.fails.len(), v.scripts);
    assert!(
        !v.fails.is_empty(),
        "the planted change {plant} was not detected in {scripts} scripts"
    );
}

// The self-check of the test itself: a one-line change planted in the model must make the replay
// disagree, or the comparison above would be vacuous. The seeds are fixed, so these are exact.

#[test]
fn planted_hidden_figure_change_is_detected() {
    planted("hidden-le", 60);
}

#[test]
fn planted_missing_saturation_is_detected() {
    planted("no-saturation", 60);
}

#[test]
fn planted_zero_counter_rule_is_detected() {
    planted("zero-absent", 60);
}

#[test]
fn planted_type_blind_diff_is_detected() {
    // `$set 0.0` over a stored `0` is rare: it needs a few hundred scripts.
    planted("same-ignores-type", 300);
}

/// The golden vectors of `merge_state/tests.rs`, hashed by the model's independent
/// implementation of the canonical encoding and the hash.
#[test]
fn golden_vectors_match_the_model() {
    let out = run_python(&["--golden".to_string()]);
    let got: J = serde_json::from_str(out.trim()).expect("json");
    let want = json!({
        "empty": ["f8d0ff0ab1a622e0a611a762fe2b45fb", 56],
        "170": ["42b0c20d1b0c26903625be81f0a105a0", 670],
        "mixed": ["34ce6696c01c0c70255c54445944f39c", 1401],
        "delete": ["0b4d37ac9b577c417603e7c9bce83933", 542],
        "obs": ["f96f244c2b3198f724fcea0eafec32c7", 1325],
        // the compact form (compact.rs), hashed with the same mix; compact_tests.rs pins the same
        "empty-compact": ["e145d6a7daf371398c7b574cd9aa42d4", 9],
        "170-compact": ["45e14c7b5dd32f32f7cf6543335f3fb1", 213],
        "mixed-compact": ["855d32c60d7c369fb409ecf4d6b6331a", 340],
        "delete-compact": ["95a3e75ef534ce08fd975b1224a8af76", 233],
        "obs-compact": ["d785253ed2c10e27e148d2a7f2f251cd", 255],
    });
    assert_eq!(got, want);
}
