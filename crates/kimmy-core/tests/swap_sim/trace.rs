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
use crate::world::{Policy, World};

/// Counters only the `field` policy keeps (the covered-entry skip, the loss records its joins produce); not in the model, so
/// excluded from the counter comparison of a `field` replay and from nothing else.
pub const FIELD_ONLY_STATS: [&str; 3] = ["covered_skips", "q7_records_fresh", "same_write_held"];

/// The counters of the Rust-only emulations (the route tests' live page read, `Repair::Replay`, the push, the product routes' held
/// entries and DF-1's marker): the model has none of them, so they are left out of the counter comparison (they count, they decide
/// nothing the model decides).
// (Rust-only INVARIANT, not a counter, therefore not in this list: `loss_record_outside_coverage_prefix`, the Q6 design page's 'Records' rule that a loss
// record is an ordinary entry inside its appender's coverage prefix; the Python model does not check it (next model round adds it); v6 trips it
// on neither side, so the replay's verdict comparison is unaffected)
pub const RUST_ONLY_STATS: [&str; 13] = [
    // item Q / rule S: counters of the harness only (the model has no such names)
    "rule_s_negative_cases",
    "rule_s_negative_appended",
    "marks_dropped_superseded",
    "page_drop_information_only",
    "replay_entries",
    "push_without_window",
    "push_carries_q6_kind_out_of_position",
    "df1_loser_superseded_by_uncovered",
    "gap1_nonstaging_candidates",
    "gap1_nonstaging_not_appended",
    "join_restarted_dead_sender",
    "dead_sender_join_waiting",
    "dead_sender_join_open_at_end",
];

thread_local! {
    /// ITEM Q: while set, a replay does not compare with the model; it runs the events under the given `product_marks` setting and
    /// pushes the DOCUMENT-LAYER view after every event (and the verdict at the end) into the vector.
    pub static COLLECT: std::cell::RefCell<Option<(bool, Vec<String>)>> = const { std::cell::RefCell::new(None) };
}

fn collect_mode() -> Option<bool> {
    COLLECT.with(|c| c.borrow().as_ref().map(|x| x.0))
}

fn collect_push(line: String) {
    COLLECT.with(|c| {
        if let Some((_, v)) = c.borrow_mut().as_mut() {
            v.push(line);
        }
    });
}

/// The document-layer outcome of every member: what it serves, the documents of its alive physicals, its loss records, the events it
/// emitted, its invariants. Bookkeeping (held sets, coverage, the vector, `appended`) is NOT in it.
fn doc_layer_view(ms: &[Member]) -> String {
    let mut out = String::new();
    for m in ms {
        let docs: Vec<String> = m
            .phys
            .iter()
            .map(|(n, p)| {
                format!(
                    "{n}:{}:{:?}",
                    p.alive,
                    p.docs.iter().map(|(k, v)| (*k, v.stamp, v.body.is_some())).collect::<Vec<_>>()
                )
            })
            .collect();
        let mut loss: Vec<String> = m.loss.keys().map(|k| format!("{k:?}")).collect();
        loss.sort();
        let mut emitted: Vec<String> = m.events_emitted.iter().map(|x| format!("{x:?}")).collect();
        emitted.sort();
        let mut inv: Vec<String> = m.inv.iter().map(|x| x.to_string()).collect();
        inv.sort();
        out.push_str(&format!(
            "{}|v{}|{:?}|{docs:?}|{loss:?}|ev{emitted:?}|inv{inv:?};",
            m.name,
            m.vanished,
            m.serving()
        ));
    }
    out
}

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

/// A body as the model writes it. Under the `field` policy a document's body is its visible fields (`v="a|b"`); the model's
/// body is the list of marks `["a", "b"]`, which the replay of a trace through `field` maps it to (one register `v`, the
/// marks joined by `|`).
fn body_vw(w: &World, b: &Body) -> Value {
    if w.policy == Policy::Field
        && let Some(lines) = b
        && lines.len() == 1
        && let Some(inner) = lines[0].strip_prefix("v=\"").and_then(|s| s.strip_suffix('"'))
    {
        return json!(inner.split('|').collect::<Vec<_>>());
    }
    body_v(b)
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

pub fn entry_v(w: &World, e: &Entry) -> Value {
    let mut d = json!({"idx": e.idx, "kind": e.kind.name(), "origin": w.name(e.origin), "hlc": e.hlc, "staging": e.staging});
    let o = d.as_object_mut().unwrap();
    if e.kind == Kind::Loss {
        o.insert("lkey".into(), lk_v(w, &e.lkey.unwrap()));
        o.insert(
            "body".into(),
            if w.policy == Policy::Field { Value::Null } else { body_v(&e.body) },
        );
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
        o.insert("body".into(), body_vw(w, &e.body));
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
            let docs: Vec<Value> = p.docs.iter().map(|(k, v)| json!([k, stamp_v(w, &v.stamp), body_vw(w, &v.body)])).collect();
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
        "loss": m.loss.iter().map(|(k, b)| json!([lk_v(w, k), if w.policy == Policy::Field { Value::Null } else { body_v(b) }])).collect::<Vec<_>>(),
        "loss_pending": m.loss_pending.iter().map(|(k, b)| json!([lk_v(w, k), if w.policy == Policy::Field { Value::Null } else { body_v(b) }])).collect::<Vec<_>>(),
        "loss_phys": sj(m.loss_phys.iter().map(|(k, p)| json!([lk_v(w, k), p])).collect()),
        "events_emitted": sj(m.events_emitted.iter().filter(|(_, n)| **n != 0).map(|(k, n)| {
            json!([[k.0, k.2.as_ref().map(|s| stamp_v(w, s))], n])
        }).collect()),
        "pend_tombs": sj(m.pend_tombs.iter().map(|(n, s)| json!([n, stamp_v(w, s)])).collect()),
        "held_n": m.cv.count(), "held_h": fnv(m.cv.iter()),
        "appended": m.appended.iter().copied().collect::<Vec<_>>(), "appended_n": m.appended.len(), "appended_h": fnv(m.appended.iter().copied()),
        "dochist_n": m.dochist.len(), "dochist_h": fnv(m.dochist.iter().copied()),
        "hist_kept_h": fnv(sorted_ids(m.hist_kept.iter().copied())),
        "recv_entry_h": fnv(sorted_ids(m.recv_entry.iter().copied())),
        "deferred": m.deferred.iter().map(|e| e.idx).collect::<Vec<_>>(),
        "info": sorted_ids(m.info.keys().copied()),
        "loss_info": sorted_ids(m.loss_info.keys().copied()),
        "held_records": sj(m.held_records.iter().map(|(p, k, s)| json!([p, k, stamp_v(w, s)])).collect()),
        "bare_state": sj(m.bare_state.iter().map(|(p, k, s)| json!([p, k, stamp_v(w, s)])).collect()),
        "state_held": sj(m.state_held.iter().map(|(o, h)| json!([w.name(*o), h])).collect()),
        "held_oop": sorted_ids(m.held_oop.iter().copied()),
        "eps": sj(m.refusal_eps.iter().map(|(i, e)| json!([i, e.start, e.last, e.n, e.end, e.end_q, e.info, e.cover, e.cover_q])).collect()),
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

/// Every leaf difference between two values (an array of unequal length is one difference).
pub fn all_diffs(path: &str, py: &Value, rs: &Value, out: &mut Vec<String>) {
    match (py, rs) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, va) in a {
                match b.get(k) {
                    Some(vb) => all_diffs(&format!("{path}.{k}"), va, vb, out),
                    None => out.push(format!("{path}.{k}: python {va} <> rust (absent)")),
                }
            }
            for k in b.keys().filter(|k| !a.contains_key(*k)) {
                out.push(format!("{path}.{k}: python (absent) <> rust {}", b[k]));
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                out.push(format!("{path}: length python {} <> rust {}", a.len(), b.len()));
                return;
            }
            for i in 0..a.len() {
                all_diffs(&format!("{path}[{i}]"), &a[i], &b[i], out);
            }
        }
        _ => {
            if py != rs {
                out.push(format!("{path}: python {py} <> rust {rs}"));
            }
        }
    }
}

/// The document layer of a member view: what depends on how the policy decides a version (last-writer-wins or the join): the
/// documents, the records, the relay's decisions (`appended`, `held`, `dochist`, ...). The rest is the register layer.
pub fn is_doc_layer(path: &str) -> bool {
    [
        ".docs",
        ".loss",
        ".events_emitted",
        ".appended",
        ".held_",
        ".dochist",
        ".hist_kept",
        ".recv_entry",
        ".deferred",
        ".held_records",
        ".bare_state",
        ".pend_tombs",
        "].body",
        ".lkey",
        ".extra",
    ]
    .iter()
    .any(|k| path.contains(k))
}

/// How a trace is replayed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplayMode {
    /// the `document` policy: every difference is a disagreement
    Document,
    /// the `field` policy on a trace with no delete and no concurrent same-key write (the header says `field_safe`): every
    /// difference is a disagreement
    FieldExact,
    /// the `field` policy on any trace: the replay stops at the first event where a document-layer field differs (the two
    /// policies decide differently there, expected) and reports where; a difference in the register layer before that is a
    /// disagreement
    FieldRegisterLayer,
}

/// Whether the trace's header marks it field-safe.
pub fn is_field_safe(path: &Path) -> bool {
    let text = std::fs::read_to_string(path).unwrap();
    let head: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    head["field_safe"] == true
}

pub struct Outcome {
    /// the event and the first document-layer difference at which a `FieldRegisterLayer` replay stopped
    pub stopped_at: Option<(usize, String)>,
    /// member views where a `field` replay's clock was above the model's while a snapshot join was open (the Q-F6 rule: a joined
    /// page's whole state is witnessed, the model witnesses only the entries applied); the clocks must agree again at the end
    pub hlc_raised_mid_join: usize,
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
    replay_file_mode(path, ReplayMode::Document)
}

pub fn replay_file_mode(path: &Path, rmode: ReplayMode) -> Result<Outcome, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut lines = text.lines();
    let head: Value =
        serde_json::from_str(lines.next().ok_or("empty trace")?).map_err(|e| e.to_string())?;
    let name = path.file_name().unwrap().to_string_lossy().to_string();
    assert!(
        matches!(head["sha256_16"].as_str(), Some("4343f5208763266f" | "409eb9d10f35682c")),
        "{name}: the trace is of a vendored model"
    );
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
    // a trace of a run with route probes (`Q6_F` > 0): register events travel out of position, the in-order liveness check is off
    w.in_order = head["route_f"].as_f64().unwrap_or(0.0) == 0.0;
    // 6d: the product routes (`doc_hold`, `doc_repair`) hold entries out of position and do not cover them
    let routes = head["routes"].as_str().unwrap_or("");
    w.held_uncovered = routes.contains("doc_hold") || routes.contains("doc_repair");
    // rule S: the trace says whether the model that made it ran with the staged-row rule (the harness replays what the model did)
    if let Some(on) = head["staged_row"].as_bool() {
        w.f.staged_row = on;
    }
    if let Some(on) = collect_mode() {
        w.product_marks = on;
        w.vector_cov = on;
    }
    // (the in-order check of register events stays off on every route run)
    if rmode != ReplayMode::Document {
        if rmode == ReplayMode::FieldExact {
            assert_eq!(head["field_safe"], true, "{name}: not a field-safe trace");
        }
        w.policy = Policy::Field;
        w.q7_entries = false;
        w.field_counters = false;
    }
    let mut ms: Vec<Member> =
        members.iter().enumerate().map(|(i, (n, s))| Member::new(i, n, *s)).collect();
    let mut jobs: Vec<Job> = Vec::new();
    let idx = |n: &str| names.iter().position(|x| x == n).unwrap_or_else(|| panic!("member {n}"));
    let mut out = Outcome { stopped_at: None, hlc_raised_mid_join: 0, events: 0, member_views: 0 };
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
        if ev["k"] == "steps_end" {
            // the run's steps are over: the D10 state counters of the steps' end, then the end-of-run delivery
            crate::run::count_open_refusals(&mut w, &ms, false);
            w.steps_end = ev["steps_end"].as_i64().unwrap();
            w.quiescing = true;
            out.events += 1;
            continue;
        }
        if ev["k"] == "check_inv" {
            let i = idx(ev["m"].as_str().unwrap());
            ms[i].check_inv(&w);
            if collect_mode().is_some() {
                continue;
            }
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
        if collect_mode().is_some() && (kind == "pre_finish" || kind == "final") {
            if kind == "pre_finish" && w.quiescing {
                crate::run::count_open_refusals(&mut w, &ms, true);
            }
            if kind == "final" {
                let r = finish(&mut w, &mut ms);
                let keep = [
                    "diverged",
                    "div_state",
                    "div_loss",
                    "div_replay",
                    "oracle_state",
                    "served_mismatch",
                    "alive_mismatch",
                    "loss_mismatch",
                    "loss_missing",
                    "loss_extra",
                    "heads_required",
                    "cnt_o8",
                    "cnt_o8_in_hb2",
                    "cnt_hb2_excluded",
                    "inv_partial_flip",
                ];
                let v: Vec<String> = keep
                    .iter()
                    .map(|k| format!("{k}={}", r.get(*k).copied().unwrap_or(0)))
                    .collect();
                collect_push(format!("FINAL {}", v.join(",")));
                // bookkeeping, NOT part of the comparison: held marks left, entries served, marks dropped by the product rule
                let live: Vec<&Member> = ms.iter().filter(|m| !m.vanished).collect();
                collect_push(format!(
                    "BOOK held_oop={} appended={} dropped={} released_in_window={}",
                    live.iter().map(|m| m.held_oop.len()).sum::<usize>(),
                    live.iter().map(|m| m.appended.len()).sum::<usize>(),
                    w.stats.get("marks_dropped_superseded"),
                    w.stats.get("held_released_in_window")
                ));
            }
            continue;
        }
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
                let before: BTreeSet<EntryId> = dm.appended.clone();
                dm.take_from(&mut w, sm, &e, ev["quiesce"].as_bool().unwrap());
                if std::env::var("Q6DIFF_DEBUG").is_ok() {
                    let added: Vec<String> =
                        dm.appended.difference(&before).map(|&i| w.ent(i).to_string()).collect();
                    eprintln!(
                        "DEBUG event {} take_from {} <- {} idx {}: appended {added:?}",
                        lineno + 1,
                        dm.name,
                        sm.name,
                        e.idx
                    );
                }
                Value::Null
            }
            "take" => {
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                ms[idx(&s("dst"))].take(&mut w, &e);
                Value::Null
            }
            "hold" => {
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                w.stats.bump(if w.held_uncovered && e.kind == Kind::Doc {
                    "doc_hold_taken"
                } else {
                    "hold_taken"
                });
                ms[idx(&s("dst"))].take_hold(&mut w, &e);
                Value::Null
            }
            "doc_repair" => {
                let (d, sr) = (idx(&s("dst")), idx(&s("src")));
                let (dm, sm) = pair(&mut ms, d, sr);
                let r = dm.scoped_repair(&mut w, sm);
                if r {
                    w.stats.bump("doc_repair");
                }
                json!(r)
            }
            "repair_ev" => {
                let e = w.ent_rc(ev["idx"].as_u64().unwrap() as usize);
                w.stats.bump("repair_events");
                ms[idx(&s("dst"))].add_event(&mut w, &e);
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
                if w.quiescing {
                    // ... and the state counters at the end of the run (a random run; a scripted one never ran the loop)
                    crate::run::count_open_refusals(&mut w, &ms, true);
                }
                let py: BTreeMap<String, i64> = ev["stats"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| {
                        let k = p[0].as_str().unwrap();
                        // the product routes (item P, provisional mapping): the model's `page_above_coverage` (a page names a version
                        // its sender does not cover) is the harness' `page_carries_held_state`
                        let k = if w.held_uncovered && k == "page_above_coverage" {
                            "page_carries_held_state"
                        } else if w.held_uncovered && k == "hb1_held_not_appended" {
                            // HB-1 removed: the model counts an entry held out of position that an apply had appended
                            "held_kept_unservable"
                        } else {
                            k
                        };
                        (k.to_string(), p[1].as_i64().unwrap())
                    })
                    .collect();
                // the page counter is compared as present or absent (the model counts a snapshot's pages, the harness the versions)
                let mut py = py;
                let rs: BTreeMap<String, i64> = w
                    .stats
                    .0
                    .iter()
                    .filter(|(k, v)| {
                        **v != 0
                            && !RUST_ONLY_STATS.contains(&k.as_str())
                            && !(rmode != ReplayMode::Document
                                && FIELD_ONLY_STATS.contains(&k.as_str()))
                    })
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                let mut rs = rs;
                if w.held_uncovered {
                    for m in [&mut py, &mut rs] {
                        if let Some(v) = m.get_mut("page_carries_held_state") {
                            *v = 1;
                        }
                    }
                }
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
                if let Ok(path) = std::env::var("Q6DIFF_O8OUT") {
                    // item P's O8 same-trace check: the Rust marker of this trace, one line per trace
                    use std::io::Write;
                    let mut f =
                        std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
                    writeln!(
                        f,
                        "{name}\t{}\t{}\t{}",
                        r.get("cnt_o8").copied().unwrap_or(0),
                        r.get("cnt_o8_in_hb2").copied().unwrap_or(0),
                        r.get("alive_mismatch").copied().unwrap_or(0)
                    )
                    .unwrap();
                }
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
                // the product routes' mapping: a model run whose only invariant is `page_above_coverage` has no invariant here
                let only_page_inv = w.held_uncovered
                    && py.contains_key("inv_page_above_coverage")
                    && !py
                        .keys()
                        .any(|k| k.starts_with("inv_") && k.as_str() != "inv_page_above_coverage");
                for (k, v) in &py {
                    if k.starts_with("inv_") {
                        continue;
                    }
                    if only_page_inv && k == "invariant" {
                        continue;
                    }
                    // the product routes: the model has no HB-2 exclusion, so its stamp-order replay verdict is the harness' RAW one
                    // (before the exclusion); the exclusion must then be counted (`cnt_hb2_excluded`) wherever the two differ
                    let key = if w.held_uncovered && k == "div_replay_stamp_order" {
                        "raw_div_replay".to_string()
                    } else {
                        rename(k)
                    };
                    let got = r.get(&key).copied().unwrap_or(0);
                    if key == "raw_div_replay"
                        && got != r.get("div_replay").copied().unwrap_or(0)
                        && r.get("cnt_hb2_excluded").copied().unwrap_or(0) == 0
                    {
                        bad.push("div_replay differs from its raw verdict without a counted HB-2 exclusion".to_string());
                    }
                    if got != *v {
                        bad.push(format!("{k}: python {v} <> rust {got}"));
                    }
                }
                let py_inv: BTreeSet<&String> = py
                    .keys()
                    .filter(|k| k.starts_with("inv_"))
                    // (the same mapping: on the product routes the model's invariant is the harness' counted held state)
                    .filter(|k| !(w.held_uncovered && k.as_str() == "inv_page_above_coverage"))
                    .collect();
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
        if collect_mode().is_some() {
            collect_push(format!("{kind} {res} {}", doc_layer_view(&ms)));
            // bookkeeping per event, NOT compared: marks held, entries served, marks dropped, held-behind-held decisions
            collect_push(format!(
                "BOOKEV {} {} {} {}",
                ms.iter().map(|m| m.held_oop.len()).sum::<usize>(),
                ms.iter().map(|m| m.appended.len()).sum::<usize>(),
                w.stats.get("marks_dropped_superseded"),
                w.stats.get("held_behind_held")
            ));
            continue;
        }
        if kind != "final" && kind != "pre_finish" && kind != "lost" {
            let mut diffs: Vec<String> = Vec::new();
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
                // the result of a client write is the policy's decision (refused, or a no-op); the others are the register's
                let tag = if matches!(kind, "local") { ".body" } else { "" };
                diffs.push(format!("{tag}result: python {} <> rust {res}", ev["res"]));
            }
            let new_py = ev["new"].as_array().unwrap();
            let new_rs: Vec<Value> = w.all[n0..].iter().map(|e| entry_v(&w, e)).collect();
            let is_loss = |v: &Value| v["kind"] == "loss";
            let policy_loss = rmode != ReplayMode::Document;
            if policy_loss {
                // the loss records are the policy's (a value per lost write for the model, a head per lost cell for the join);
                // the other entries are compared exactly, by content, because a loss entry minted in the same event shifts ids
                let strip = |v: &Value| -> Value {
                    let mut v = v.clone();
                    v["idx"] = Value::Null;
                    v
                };
                let py_other: Vec<Value> =
                    new_py.iter().filter(|v| !is_loss(v)).map(strip).collect();
                let rs_other: Vec<Value> =
                    new_rs.iter().filter(|v| !is_loss(v)).map(strip).collect();
                all_diffs("new", &Value::Array(py_other), &Value::Array(rs_other), &mut diffs);
                let (pl, rl) = (
                    new_py.iter().filter(|v| is_loss(v)).count(),
                    new_rs.iter().filter(|v| is_loss(v)).count(),
                );
                let same_keys = {
                    let key =
                        |v: &Value| (v["origin"].clone(), v["lkey"].clone(), v["extra"].clone());
                    let a: Vec<_> = new_py.iter().filter(|v| is_loss(v)).map(key).collect();
                    let b: Vec<_> = new_rs.iter().filter(|v| is_loss(v)).map(key).collect();
                    a == b
                };
                if pl != rl || !same_keys {
                    diffs.push(format!(
                        ".loss entries minted: python {pl}, rust {rl} (the policy's records)"
                    ));
                }
            } else if new_py.len() != new_rs.len() {
                diffs.push(format!(
                    ".body entries minted: python {} <> rust {}",
                    Value::Array(new_py.clone()),
                    Value::Array(new_rs.clone())
                ));
            } else {
                for (p, r) in new_py.iter().zip(&new_rs) {
                    all_diffs("new", p, r, &mut diffs);
                }
            }
            for mv in ev["ms"].as_array().unwrap() {
                let i = idx(mv["name"].as_str().unwrap());
                let rv = member_view(&w, &ms[i]);
                let mut mv = mv.clone();
                if rmode != ReplayMode::Document {
                    for k in ["loss", "loss_pending"] {
                        if let Some(a) = mv[k].as_array_mut() {
                            for rec in a.iter_mut() {
                                rec[1] = Value::Null;
                            }
                        }
                    }
                }
                let mut mine = Vec::new();
                all_diffs(&format!("member {}", ms[i].name), &mv, &rv, &mut mine);
                let hlc_path = format!("member {}.hlc", ms[i].name);
                let raised = rmode != ReplayMode::Document
                    && !rv["join"].is_null()
                    && rv["hlc"].as_i64() > mv["hlc"].as_i64();
                if raised && mine.iter().any(|d| d.starts_with(&hlc_path)) {
                    mine.retain(|d| !d.starts_with(&hlc_path));
                    out.hlc_raised_mid_join += 1;
                }
                diffs.extend(mine);
                out.member_views += 1;
            }
            if rmode != ReplayMode::Document
                && diffs.iter().any(|d| d.starts_with(".loss entries minted"))
            {
                // every loss entry a member mints ticks its clock: the clock follows the policy's records, not the register
                diffs.retain(|d| !(d.starts_with("member ") && d.contains(".hlc: ")));
            }
            if !diffs.is_empty() {
                let doc = diffs.iter().find(|d| is_doc_layer(d));
                // a register-layer difference in the same event is still a disagreement
                if rmode == ReplayMode::FieldRegisterLayer
                    && let Some(d) = doc
                    && diffs.iter().all(|d| is_doc_layer(d))
                {
                    out.stopped_at = Some((lineno + 1, d.clone()));
                    return Ok(out);
                }
                // the first difference that is not the policy's (the whole list when it has several)
                let first = diffs
                    .iter()
                    .find(|d| rmode == ReplayMode::Document || !is_doc_layer(d))
                    .unwrap_or(&diffs[0]);
                return Err(fail(first.clone()));
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
