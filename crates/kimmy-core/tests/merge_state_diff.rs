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
//! ODD FIELD NAMES. The vendored A2.3 model reads a dot in a name as a path separator and keys the root of a replace by the
//! empty name, so this differential no longer draws the empty name or a name with a dot (two plain extra names take their
//! place in the "odd" writes). They are covered by the sim (`the_sim_workload_holds_odd_names`, with a floor on their
//! count), the nested differential (`ODD_LEAVES`), the join-law pools (`the_pools_hold_odd_names`) and the unit tests
//! (`odd_field_names_are_distinct_cells_and_round_trip`, `dotted_names_and_nested_paths_are_different_cells_and_round_trip`,
//! `odd_names_are_one_segment_paths_through_write_join_visible_and_both_codecs`,
//! `every_bson_type_and_odd_names_round_trip_in_the_compact_form`).
//!
//! NULLS. A stored null is drawn (insert, replace, set): the model's `None` is a present null (A2), the writer's
//! `Value::Null`. An unset spelled as a value (`Value::Unset`) is drawn in insert, replace and set, and the generator gives the
//! model its own spelling: `$unset` of the path, or the field left out of the document. The writer stores no register for an
//! `Unset` field of an insert (or of the creation of an upsert), as the model's absent field; `q4_set_compares_value_and_type`,
//! `q4_replace_compares_value_and_type`, `an_unset_spelled_as_a_value_is_an_unset_for_the_order_state_too` and
//! `an_unset_field_in_an_insert_or_an_upsert_creation_stores_nothing` cover the writer's side.
//!
//! `Q7DIFF_RUNS=<n>` sets the number of scripts (default 300, about a minute in a debug build).
//! `python3` must be on the PATH: if it is not, the tests FAIL (they do not skip).
//!
//! The model is the vendored A2.3 one (`reference_model/MODEL-HASHES.txt`), run with `ABSORBED = True` (the counter cell) and no implicit
//! marker, as the Rust side is today. THE (h, len) COLUMNS: a state with an absorbed pair has no version 1 spelling, so the model's `h`
//! and `len` are null for it and the Rust side must answer `V1CannotHold` for exactly those states (both directions); for every other
//! state `h` and `len` are compared exactly, and the compact `ch` and `clen` for all of them. A write that only reorders the fields is a write in the model and in the writer.
//! The replay compares the bytes and hashes of states WITHOUT their order state (`fingerprint::without_ords`: the
//! generator's encoders write an empty order section); the order state meets the model in `merge_state_ord_diff`.

#[path = "common/fingerprint.rs"]
mod fingerprint;
#[path = "merge_state_sim/oracle.rs"]
#[allow(dead_code)]
mod oracle;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, LossKey, LossKind, MStamp, MergeState, Path, Value, Writer, WriterId,
    Written,
};
use serde_json::{Value as J, json};

/// The precondition under which a creation base folds nothing (the counter cell): a creation (an insert or
/// an upsert) is refused while any life is alive, so at an accepted creation the writer holds no counter
/// slot in an alive life. Asserted by the harness at every accepted creation; there is no assert in the
/// product code.
fn slot_in_alive_life(w: &Writer) -> bool {
    let alive = w.alive();
    w.state.slots.keys().any(|k| alive.contains(&k.0))
}

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
        Value::Null => json!(["z"]),
        Value::Int64(i) => json!(["i", i.to_string()]),
        Value::Double(d) => json!(["d", format!("{:016x}", d.to_bits())]),
        Value::String(s) => json!(["s", s]),
        // The flat reference model has these five types only (an unset, a null, an integer, a double, a string); the other BSON types are
        // tested in `merge_state::tests` and the codec tests.
        other => panic!("the reference model has no {other:?}"),
    }
}

fn unjv(v: &J) -> Value {
    let a = v.as_array().unwrap();
    match a[0].as_str().unwrap() {
        "n" => Value::Unset,
        "z" => Value::Null,
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

/// The script names a field of the flat model: one segment, whatever the name (it may be empty,
/// or hold a dot: the flat model keeps a name as one string and never splits it). Not `_`: see
/// `ODD` in `reference_model/q7diff_gen.py`, which names the names the flat model takes and why
/// it cannot take `_` (the sim and the join-law tests have it).
fn pth(s: &str) -> Path {
    Path::flat(s).expect("a valid name")
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

/// The states the replay compared, and how many of them have no version 1 spelling (an absorbed pair).
static STATES: AtomicUsize = AtomicUsize::new(0);
static NO_V1: AtomicUsize = AtomicUsize::new(0);

fn snap(ms: &[Mem]) -> Vec<J> {
    ms.iter()
        .map(|m| {
            let st = fingerprint::without_ords(&m.w.state);
            // THE (h, len) RULE. Version 1 cannot spell a state with an absorbed pair (`V1CannotHold`), and
            // the model's v1 column is null for exactly those states: the two are compared as part of the
            // snapshot, so `V1CannotHold` happens exactly when the model state has an absorbed store, in both
            // directions. The compact columns are compared for every state.
            let has_pair = st.slots.values().any(|s| s.absorbed.is_some());
            let v1 = st.try_canonical_bytes();
            assert_eq!(
                v1.is_err(),
                has_pair,
                "the differential's states are Int64 counters: version 1 refuses a state exactly for its pair"
            );
            STATES.fetch_add(1, Ordering::Relaxed);
            if has_pair {
                NO_V1.fetch_add(1, Ordering::Relaxed);
            }
            json!({
                "h": v1.as_ref().map_or(J::Null, |_| json!(hex(&st.try_state_hash().unwrap()))),
                "len": v1.as_ref().map_or(J::Null, |cb| json!(cb.len())),
                "ch": hex(&st.compact_hash()),
                "clen": st.compact_bytes().len(),
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
    // the model spells the whole document `_`; the oracle spells it so as never to be a field's name
    let mut v: Vec<(Path, MStamp)> = h
        .into_iter()
        .map(|(p, st)| {
            (if p == oracle::whole_document() { Path::flat("_").unwrap() } else { p }, st)
        })
        .collect();
    v.sort();
    v.iter().map(|(p, st)| json!([p.to_string(), js(st)])).collect()
}

/// Returns a list of mismatch descriptions (empty when the script agrees).
fn run_script(sc: &J) -> (Vec<String>, BTreeMap<&'static str, usize>, u64) {
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
    // every member's state and reads after every step, and every step's result
    let mut fp = fingerprint::Fp::new();
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
                let held = slot_in_alive_life(&ms[m].w);
                let r = ms[m].w.insert(&f, op[3].as_u64().unwrap());
                assert!(
                    r.is_none() || !held,
                    "an insert was accepted over a slot of an alive life"
                );
                opt_stamp_j(ms[m].logged(r, &mut log))
            }
            "upsert" => {
                let m = op[1].as_u64().unwrap() as usize;
                let held = slot_in_alive_life(&ms[m].w);
                let r = ms[m].w.upsert_inc(
                    &pth(op[2].as_str().unwrap()),
                    op[3].as_i64().unwrap(),
                    op[4].as_u64().unwrap(),
                );
                assert!(
                    r.is_none() || !held,
                    "an upsert was accepted over a slot of an alive life"
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
                let held = slot_in_alive_life(&ms[m].w);
                let created = ms[m].w.upsert_soi_create(&f, &inc_path, &s, sk);
                assert!(
                    created.is_none() || !held,
                    "an upsert creation was accepted over a slot of an alive life"
                );
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
            return (bad, stats, 0);
        }
        fp.text(&res.to_string());
        for m in &ms {
            fp.state(&m.w.state, &m.w.opts);
        }
        let got = snap(&ms);
        let wa = want["after"].as_array().unwrap();
        for (k, (g, w)) in got.iter().zip(wa).enumerate() {
            if g != w {
                bad.push(format!(
                    "step {i} {op}: member {k}: rust {g} python {w}\n  rust state: {:?}",
                    ms[k].w.state
                ));
                return (bad, stats, 0);
            }
        }
    }
    // final full-mesh sync
    for _ in 0..12 {
        let before: Vec<Vec<u8>> =
            ms.iter().map(|m| fingerprint::without_ords(&m.w.state).compact_bytes()).collect();
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
        if ms
            .iter()
            .map(|m| fingerprint::without_ords(&m.w.state).compact_bytes())
            .collect::<Vec<_>>()
            == before
        {
            break;
        }
    }
    for m in &ms {
        fp.state(&m.w.state, &m.w.opts);
    }
    let fin = &sc["expect"]["final"];
    let got = snap(&ms);
    if J::Array(got.clone()) != fin["after"] {
        bad.push(format!("final sync: rust {got:?} python {}", fin["after"]));
        return (bad, stats, 0);
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
    (bad, stats, fp.value())
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
    /// The sum of the scripts' fingerprints, so that it does not depend on the threads.
    fp: u64,
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
                        let (bad, st, fp) = run_script(&sc);
                        v.fp = v.fp.wrapping_add(fp);
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
///
/// RE-PINNED for item O2 (the Writer feeds the order state): 0x55c8_39db_ba2a_e84a to 0x2aee_26c6_deda_603d. The fingerprint folds the
/// compact bytes and hash of the state WITH its order state (section 10) and the document in the order the order state gives it, which
/// the fingerprint did not hold before; the version 1 bytes, the hashes, the visible documents, every stamp and refusal are still compared
/// with the model, with the order state left out, and did not change (the Group A pin was the same figure).
///
/// RE-PINNED for the D8 re-vendor (the A2.3 model, the flat generator's new draws): 0x2aee_26c6_deda_603d to 0xfbde_c4a1_8eb1_bb13.
///
/// RE-PINNED when the generator drew an Unset field in an insert again (the Writer drops such a field): 0xfbde_c4a1_8eb1_bb13 to
/// 0x5f0f_3ecf_ab90_7bba. The scripts changed (an insert draws one more value); the code under test did not change for the re-pin.
const PINNED_300: u64 = 0x5f0f_3ecf_ab90_7bba;

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
    println!("differential fingerprint ({n} scripts): {:#018x}", v.fp);
    let (states, no_v1) = (STATES.load(Ordering::Relaxed), NO_V1.load(Ordering::Relaxed));
    println!(
        "(h, len): {states} member-step states compared, {no_v1} without a version 1 spelling (an absorbed pair), {} exact on version 1",
        states - no_v1
    );
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
        // the state has absorbed pairs: no version 1 spelling (the old golden was of the register obs map)
        "obs": J::Null,
        // the compact form (compact.rs), hashed with the same mix; compact_tests.rs pins the same
        "empty-compact": ["6a9f06d361f4fb8c789c079f6563280a", 10],
        "170-compact": ["abedfb140b687b3671abd3023efa93a8", 215],
        "mixed-compact": ["d368f0edd26e1ed5e300dd97df80065e", 338],
        "delete-compact": ["e695a485390d23417c6a5fd1d8f9ca41", 235],
        "obs-compact": ["3894def7793a573b2dad9a4de56bf7cc", 252],
    });
    assert_eq!(got, want);
}
