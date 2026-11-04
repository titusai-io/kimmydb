//! The Python-vs-Rust differential: replays the JSONL traces of `reference_model/q6diff_gen.py` (the tracer of the vendored
//! `swapmodel6a.py`) event by event on the Rust harness. After every event it compares the entries the event minted and the state
//! of the acting member (of every member at the structural events and every 20 events); before `finish` it compares the counters;
//! at the end it compares the verdict of `finish`. The first difference is reported with the event number, the event, the member
//! and the path of the differing field, and stops that trace. Python decides: a difference is a finding, never an exclusion.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::entry::*;
use crate::flags::flags_for;
use crate::member::{DocOp, Member};
use crate::oracle::finish;
use crate::relay::{pair, take_quiesce};
use crate::rng::Rng;
use crate::snapshot::{land_page_in, snapshot_begin_in};
use crate::world::World;

/// Counters the model keeps and the Rust port does not carry (statistics for the refusal-duration tables and the two route
/// probes of phase G); excluded from the counter comparison and from nothing else.
pub const PY_ONLY_STATS: [&str; 8] = [
    "refusal_episodes",
    "refusal_episodes_info",
    "refusal_open_at_steps_end",
    "refusal_open_at_steps_end_info",
    "refusal_open_at_run_end",
    "refusal_open_at_run_end_info",
    "hold_taken",
    "repair_events",
];

/// FNV-1a over entry ids (each id + 1 is one input): the tracer's `fnv`.
pub fn fnv<I: IntoIterator<Item = usize>>(ids: I) -> u64 {
    let mut x: u64 = 1469598103934665603;
    for v in ids {
        x = (x ^ (v as u64 + 1)).wrapping_mul(1099511628211);
    }
    x
}

fn sorted_ids<I: IntoIterator<Item = usize>>(ids: I) -> Vec<usize> {
    let mut v: Vec<usize> = ids.into_iter().collect();
    v.sort();
    v
}

fn origin_of(s: &Stamp) -> usize {
    s.writer.node.to_bytes()[0] as usize - b'A' as usize
}

fn stamp_v(w: &World, s: &Stamp) -> Value {
    if *s == neg() { json!([-1, ""]) } else { json!([hlc_of(s), w.name(origin_of(s))]) }
}

fn body_v(b: &Body) -> Value {
    match b {
        None => Value::Null,
        Some(v) => json!(v.as_ref()),
    }
}

fn lk_v(w: &World, k: &LKey) -> Value {
    let mut v = vec![json!(k.key)];
    if let Some(s) = &k.stamp {
        v.push(stamp_v(w, s));
        if let Some(m) = k.minter {
            v.push(json!(w.name(m)));
        }
    }
    Value::Array(v)
}

fn sj(mut items: Vec<Value>) -> Value {
    items.sort_by_key(|x| x.to_string());
    Value::Array(items)
}

fn entry_v(w: &World, e: &Entry) -> Value {
    let mut d = json!({"idx": e.idx, "kind": e.kind.name(), "origin": w.name(e.origin), "hlc": e.hlc, "staging": e.staging});
    let o = d.as_object_mut().unwrap();
    if e.kind == Kind::Loss {
        o.insert("lkey".into(), lk_v(w, &e.lkey.unwrap()));
        o.insert("body".into(), body_v(&e.body));
        o.insert(
            "extra".into(),
            match &e.extra {
                Extra::Phys(p) => json!(p),
                _ => Value::Null,
            },
        );
        o.insert("phys".into(), json!(""));
    } else {
        o.insert("phys".into(), json!(e.phys));
        o.insert("key".into(), if e.kind == Kind::Doc { json!(e.key) } else { Value::Null });
        o.insert("body".into(), body_v(&e.body));
        o.insert(
            "opk".into(),
            match e.opk {
                Some(k) => json!(format!("{k:?}")),
                None => Value::Null,
            },
        );
        o.insert(
            "dl".into(),
            if matches!(e.kind, Kind::Create | Kind::Bind) { json!(e.dl) } else { Value::Null },
        );
        o.insert(
            "extra".into(),
            match (&e.kind, &e.extra) {
                (Kind::Lcreate, Extra::Phys(p)) => json!(p),
                _ => Value::Null,
            },
        );
    }
    d
}

fn pkind(k: PKind) -> &'static str {
    match k {
        PKind::Id => "id",
        PKind::Stage => "stage",
        PKind::Rc => "rc",
    }
}

/// The state of a member as the tracer's `member_j` writes it.
pub fn member_view(w: &World, m: &Member) -> Value {
    let phys: Vec<Value> = m
        .phys
        .iter()
        .map(|(n, p)| {
            let docs: Vec<Value> = p.docs.iter().map(|(k, v)| json!([k, stamp_v(w, &v.stamp), body_v(&v.body)])).collect();
            json!([n, {"kind": pkind(p.kind), "alive": p.alive, "kill": p.kill.as_ref().map(|s| stamp_v(w, s)),
                "kept": p.kept, "missing": p.missing, "orphan": p.orphan, "docs": docs}])
        })
        .collect();
    let join = m.join.as_ref().map(|j| {
        let mut created: Vec<&String> = j.created.iter().collect();
        created.sort();
        json!({"src": w.name(j.src), "pages": j.pages, "granted": j.granted, "created": created,
            "events": j.pl.events, "vec": sj(j.pl.vec.iter().map(|(o, h)| json!([w.name(*o), h])).collect()),
            "held_n": j.pl.held.len(), "held_h": fnv(j.pl.held.iter().copied()), "appended_h": fnv(j.pl.appended.iter().copied())})
    });
    let mut abandoned: Vec<&String> = m.abandoned.iter().collect();
    abandoned.sort();
    json!({
        "name": m.name, "serving": m.serving(), "cur": m.cur.as_ref().map(|c| c.idx), "vanished": m.vanished, "hlc": m.hlc,
        "phys": phys, "evl": m.evl,
        "bmap": sj(m.bmap.iter().map(|(n, s)| json!([n, stamp_v(w, s)])).collect()),
        "abandoned": abandoned,
        "learned": sj(m.learned.iter().map(|(n, s)| json!([n, s])).collect()),
        "loss": m.loss.iter().map(|(k, b)| json!([lk_v(w, k), body_v(b)])).collect::<Vec<_>>(),
        "loss_pending": m.loss_pending.iter().map(|(k, b)| json!([lk_v(w, k), body_v(b)])).collect::<Vec<_>>(),
        "loss_phys": sj(m.loss_phys.iter().map(|(k, p)| json!([lk_v(w, k), p])).collect()),
        "events_emitted": sj(m.events_emitted.iter().filter(|(_, n)| **n != 0).map(|(k, n)| {
            json!([[k.0, k.2.as_ref().map(|s| stamp_v(w, s))], n])
        }).collect()),
        "pend_tombs": sj(m.pend_tombs.iter().map(|(n, s)| json!([n, stamp_v(w, s)])).collect()),
        "held_n": m.cv.count(), "held_h": fnv(m.cv.iter()),
        "appended_n": m.appended.len(), "appended_h": fnv(m.appended.iter().copied()),
        "dochist_n": m.dochist.len(), "dochist_h": fnv(m.dochist.iter().copied()),
        "hist_kept_h": fnv(sorted_ids(m.hist_kept.iter().copied())),
        "recv_entry_h": fnv(sorted_ids(m.recv_entry.iter().copied())),
        "deferred": m.deferred.iter().map(|e| e.idx).collect::<Vec<_>>(),
        "info": sorted_ids(m.info.keys().copied()),
        "held_records": sj(m.held_records.iter().map(|(p, k, s)| json!([p, k, stamp_v(w, s)])).collect()),
        "bare_state": sj(m.bare_state.iter().map(|(p, k, s)| json!([p, k, stamp_v(w, s)])).collect()),
        "state_held": sj(m.state_held.iter().map(|(o, h)| json!([w.name(*o), h])).collect()),
        "join": join, "inv": m.inv.iter().collect::<Vec<_>>(), "refused": m.refused, "flips": m.flips, "last_served": m.last_served,
    })
}

/// The first difference between two values, as `path: python <> rust`.
pub fn first_diff(path: &str, py: &Value, rs: &Value) -> Option<String> {
    match (py, rs) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, va) in a {
                match b.get(k) {
                    Some(vb) => {
                        if let Some(d) = first_diff(&format!("{path}.{k}"), va, vb) {
                            return Some(d);
                        }
                    }
                    None => return Some(format!("{path}.{k}: python {va} <> rust (absent)")),
                }
            }
            b.keys()
                .find(|k| !a.contains_key(*k))
                .map(|k| format!("{path}.{k}: python (absent) <> rust {}", b[k]))
        }
        (Value::Array(a), Value::Array(b)) => {
            for i in 0..a.len().min(b.len()) {
                if let Some(d) = first_diff(&format!("{path}[{i}]"), &a[i], &b[i]) {
                    return Some(d);
                }
            }
            (a.len() != b.len()).then(|| {
                format!(
                    "{path}: length python {} <> rust {} (python {py} / rust {rs})",
                    a.len(),
                    b.len()
                )
            })
        }
        _ => (py != rs).then(|| format!("{path}: python {py} <> rust {rs}")),
    }
}

pub struct Outcome {
    pub events: usize,
    pub member_views: usize,
}

fn op_of(s: &str) -> DocOp {
    match s {
        "insert" => DocOp::Insert,
        "update" => DocOp::Update,
        "delete" => DocOp::Delete,
        _ => panic!("op {s}"),
    }
}

fn jobkind(s: &str) -> JobKind {
    match s {
        "good" => JobKind::Good,
        "late" => JobKind::Late,
        "crash-abandon" => JobKind::CrashAbandon,
        "vanish" => JobKind::Vanish,
        _ => panic!("job kind {s}"),
    }
}

fn res_idx(r: &Option<EntryRef>) -> Value {
    r.as_ref().map(|e| json!(e.idx)).unwrap_or(Value::Null)
}

/// Replay one trace file. `Err` carries the first disagreement.
pub fn replay_file(path: &Path) -> Result<Outcome, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut lines = text.lines();
    let head: Value =
        serde_json::from_str(lines.next().ok_or("empty trace")?).map_err(|e| e.to_string())?;
    let name = path.file_name().unwrap().to_string_lossy().to_string();
    assert_eq!(head["sha256_16"], "40a44c8838f55988", "{name}: the trace is of the vendored model");
    let members: Vec<(String, i64)> = head["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m[0].as_str().unwrap().to_string(), m[1].as_i64().unwrap()))
        .collect();
    let names: Vec<String> = members.iter().map(|(n, _)| n.clone()).collect();
    let mode = head["flags"].as_str().unwrap().to_string();
    let mut w = World::new(
        names.clone(),
        flags_for(&mode, "v6"),
        Rng::new(0),
        head["D"].as_i64().unwrap(),
        false,
    );
    let mut ms: Vec<Member> =
        members.iter().enumerate().map(|(i, (n, s))| Member::new(i, n, *s)).collect();
    let mut jobs: Vec<Job> = Vec::new();
    let idx = |n: &str| names.iter().position(|x| x == n).unwrap_or_else(|| panic!("member {n}"));
    let mut out = Outcome { events: 0, member_views: 0 };
    for (lineno, line) in lines.enumerate() {
        let ev: Value =
            serde_json::from_str(line).map_err(|e| format!("{name}:{}: {e}", lineno + 2))?;
        let fail = |what: String| -> String {
            let mut short = ev.clone();
            if let Some(o) = short.as_object_mut() {
                o.remove("ms");
                o.remove("new");
            }
            format!("{name}: FIRST DISAGREEMENT at event {} ({short}): {what}", lineno + 1)
        };
        if ev["k"] == "check_inv" {
            let i = idx(ev["m"].as_str().unwrap());
            ms[i].check_inv();
            let rs: Vec<&&str> = ms[i].inv.iter().collect();
            if json!(rs) != ev["inv"] {
                return Err(format!(
                    "{name}: FIRST DISAGREEMENT at event {} ({ev}): the invariants of {}: rust {rs:?}",
                    lineno + 1,
                    ms[i].name
                ));
            }
            out.events += 1;
            continue;
        }
        w.clock = ev["clock"].as_i64().unwrap();
        w.step = ev["step"].as_i64().unwrap();
        let n0 = w.all.len();
        let kind = ev["k"].as_str().unwrap();
        let s = |k: &str| ev[k].as_str().unwrap().to_string();
        let res: Value = match kind {
            "local" => {
                let m = idx(&s("m"));
                res_idx(&ms[m].local_doc(
                    &mut w,
                    ev["key"].as_u64().unwrap() as u32,
                    op_of(&s("op")),
                ))
            }
            "ldrop" => res_idx(&ms[idx(&s("m"))].local_ldrop(&mut w)),
            "lcreate" => res_idx(&ms[idx(&s("m"))].local_lcreate(&mut w)),
            "out" => {
                let m = idx(&s("m"));
                let keys: Vec<u32> = ev["keys"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|k| k.as_u64().unwrap() as u32)
                    .collect();
                let job = ms[m].start_out(
                    &mut w,
                    keys,
                    jobkind(&s("kind")),
                    ev["bind_step"].as_i64().unwrap(),
                );
                let r = json!({"job": jobs.len(), "stage": job.stage});
                jobs.push(job);
                r
            }
            "bind" => {
                let m = idx(&s("m"));
                let live = ms.iter().filter(|x| !x.vanished).count();
                let mut job = jobs[ev["job"].as_u64().unwrap() as usize].clone();
                let r = ms[m].do_bind(&mut w, &mut job, live);
                jobs[ev["job"].as_u64().unwrap() as usize] = job;
                res_idx(&r)
            }
            "take_from" => {
                let (d, sr) = (idx(&s("dst")), idx(&s("src")));
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                let (dm, sm) = pair(&mut ms, d, sr);
                dm.take_from(&mut w, sm, &e, ev["quiesce"].as_bool().unwrap());
                Value::Null
            }
            "take" => {
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                ms[idx(&s("dst"))].take(&mut w, &e);
                Value::Null
            }
            "take_quiesce" => {
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                take_quiesce(&mut ms, idx(&s("dst")), &mut w, &e);
                Value::Null
            }
            "snap_begin" => {
                if let Some(p) = ev["pages"].as_array() {
                    w.forced_pages =
                        Some(p.iter().map(|x| x.as_str().unwrap().to_string()).collect());
                }
                let r = snapshot_begin_in(
                    &mut ms,
                    idx(&s("dst")),
                    idx(&s("src")),
                    &mut w,
                    ev["lazy"].as_bool().unwrap(),
                );
                w.forced_pages = None;
                json!(r)
            }
            "land_page" => json!(land_page_in(&mut ms, idx(&s("m")), &mut w)),
            "relay_tomb" => {
                if let Some(p) = ev["phys"].as_str() {
                    w.forced_choice = Some(p.to_string());
                }
                let (d, sr) = (idx(&s("dst")), idx(&s("src")));
                let (dm, sm) = pair(&mut ms, d, sr);
                let r = dm.relay_tomb(&mut w, sm);
                w.forced_choice = None;
                json!(r)
            }
            "set_pages" => {
                let order: Vec<String> = ev["pages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_str().unwrap().to_string())
                    .collect();
                ms[idx(&s("m"))].join.as_mut().expect("a join in progress").pages = order;
                Value::Null
            }
            "settle" => {
                ms[idx(&s("m"))].settle(&mut w);
                Value::Null
            }
            "vanish" => {
                let m = idx(&s("m"));
                ms[m].vanished = true;
                w.stats.bump("writer_vanished");
                Value::Null
            }
            "lost" => {
                w.lost = ev["lost"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_u64().unwrap() as usize)
                    .collect();
                Value::Null
            }
            "pre_finish" => {
                let py: BTreeMap<String, i64> = ev["stats"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| (p[0].as_str().unwrap().to_string(), p[1].as_i64().unwrap()))
                    .filter(|(k, _)| !PY_ONLY_STATS.contains(&k.as_str()))
                    .collect();
                let rs: BTreeMap<String, i64> = w
                    .stats
                    .0
                    .iter()
                    .filter(|(k, v)| **v != 0 && !PY_ONLY_STATS.contains(&k.as_str()))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                if py != rs {
                    let keys: BTreeSet<&String> = py.keys().chain(rs.keys()).collect();
                    let d: Vec<String> = keys
                        .into_iter()
                        .filter(|k| py.get(*k) != rs.get(*k))
                        .map(|k| format!("{k}: python {:?} <> rust {:?}", py.get(k), rs.get(k)))
                        .collect();
                    return Err(fail(format!("counters before finish: {}", d.join("; "))));
                }
                Value::Null
            }
            "final" => {
                let r = finish(&mut w, &mut ms);
                let py: BTreeMap<String, i64> = ev["verdict"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| (p[0].as_str().unwrap().to_string(), p[1].as_i64().unwrap()))
                    .collect();
                // the model's `div_replay_stamp_order` is the Rust `div_replay`; its `true_late_writes_allowed` is the required pool
                let rename = |k: &str| -> String {
                    match k {
                        "div_replay_stamp_order" => "div_replay".to_string(),
                        "true_late_writes_allowed" => "late_writes_required_pool".to_string(),
                        k => k.to_string(),
                    }
                };
                let mut bad = Vec::new();
                for (k, v) in &py {
                    if k.starts_with("inv_") {
                        continue;
                    }
                    let got = r.get(&rename(k)).copied().unwrap_or(0);
                    if got != *v {
                        bad.push(format!("{k}: python {v} <> rust {got}"));
                    }
                }
                let py_inv: BTreeSet<&String> =
                    py.keys().filter(|k| k.starts_with("inv_")).collect();
                let rs_inv: BTreeSet<&String> =
                    r.keys().filter(|k| k.starts_with("inv_") && r[*k] != 0).collect();
                if py_inv != rs_inv {
                    bad.push(format!("invariants: python {py_inv:?} <> rust {rs_inv:?}"));
                }
                if !bad.is_empty() {
                    return Err(fail(format!("the verdict of finish: {}", bad.join("; "))));
                }
                Value::Null
            }
            other => return Err(fail(format!("unknown event kind {other}"))),
        };
        if kind != "final" && kind != "pre_finish" && kind != "lost" {
            if matches!(
                kind,
                "local"
                    | "ldrop"
                    | "lcreate"
                    | "out"
                    | "bind"
                    | "snap_begin"
                    | "land_page"
                    | "relay_tomb"
            ) && res != ev["res"]
            {
                return Err(fail(format!("result: python {} <> rust {res}", ev["res"])));
            }
            let new_py = ev["new"].as_array().unwrap();
            let new_rs: Vec<Value> = w.all[n0..].iter().map(|e| entry_v(&w, e)).collect();
            if new_py.len() != new_rs.len() {
                return Err(fail(format!(
                    "entries minted: python {} <> rust {}",
                    Value::Array(new_py.clone()),
                    Value::Array(new_rs)
                )));
            }
            for (p, r) in new_py.iter().zip(&new_rs) {
                if let Some(d) = first_diff("new", p, r) {
                    return Err(fail(format!("a minted entry: {d}")));
                }
            }
            for mv in ev["ms"].as_array().unwrap() {
                let i = idx(mv["name"].as_str().unwrap());
                let rv = member_view(&w, &ms[i]);
                if let Some(d) = first_diff(&format!("member {}", ms[i].name), mv, &rv) {
                    return Err(fail(d));
                }
                out.member_views += 1;
            }
        }
        out.events += 1;
    }
    Ok(out)
}

/// Every `.jsonl` file of a directory, sorted.
pub fn traces_in(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    v.sort();
    v
}
