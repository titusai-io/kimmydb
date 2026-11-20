//! The replay of the Ord differential's scripts (`tests/data/ord_diff_scripts.json`) through `Writer`s, shared by the
//! test `merge_state_ord_diff` and by the sim runner (`merge_state_sim`), which uses it as the scripted probe that kills the
//! mutants of the order state the random runs do not. See `merge_state_ord_diff.rs` for what is compared and why.

#![allow(dead_code)]

use std::collections::BTreeMap;

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, FieldOrd, Floor, JoinOptions, MStamp, MergeState, OrdAt, OrderedValue, Path, UpdateOp,
    Value, Writer, WriterId, Written,
};
use serde_json::Value as J;

const SCRIPTS: &str = include_str!("../data/ord_diff_scripts.json");
const MODEL6_SHA256: &str = "3b577688cd7e2e66161422c9797ab632d7b6864f76635614db2ab357777ccfa6";

fn wid(c: char) -> WriterId {
    wid_at(c, 0)
}

/// The writer of member `c` in its `epoch`th life (a restore takes the next).
fn wid_at(c: char, epoch: u64) -> WriterId {
    WriterId { node: NodeId::from_bytes([c as u8; 16]), epoch: Epoch::from_u64(epoch) }
}

fn path(j: &J) -> Path {
    Path::new(j.as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_string())).unwrap()
}

fn value(j: &J) -> Value {
    match j["t"].as_str().unwrap() {
        "null" => Value::Null,
        "i" => Value::Int64(j["v"].as_i64().unwrap()),
        "d" => Value::Double(j["v"].as_str().unwrap().parse().unwrap()),
        "s" => Value::String(j["v"].as_str().unwrap().to_string()),
        "a" => Value::Array(j["v"].as_array().unwrap().iter().map(value).collect()),
        "doc" => Value::Document(
            j["v"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| (f[0].as_str().unwrap().to_string(), value(&f[1])))
                .collect(),
        ),
        t => panic!("value tag {t}"),
    }
}

fn fields(j: &J) -> Vec<(Path, Value)> {
    j["v"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                Path::new(f[0].as_str().unwrap().split('.').map(str::to_string)).unwrap(),
                value(&f[1]),
            )
        })
        .collect()
}

fn ops(j: &J) -> Vec<UpdateOp> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|o| {
            let p = path(&o[1]);
            match o[0].as_str().unwrap() {
                "set" => UpdateOp::Set(p, value(&o[2])),
                "unset" => UpdateOp::Unset(p),
                "inc" => UpdateOp::Inc(p, value(&o[2])),
                k => panic!("operator {k}"),
            }
        })
        .collect()
}

// ---- the canonical text (the generator writes the same) --------------------------------------------------------------------

fn fnv(text: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}")
}

fn st_text(s: &MStamp) -> String {
    format!("{}.{}", s.hlc.wall_ms, s.writer.node.to_bytes()[0] as char)
}

fn val_text(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Int64(n) => format!("i:{n}"),
        Value::Double(d) => format!("d:{:016x}", d.to_bits()),
        Value::String(s) => format!("s:{s}"),
        Value::Array(a) => format!("a[{}]", a.iter().map(val_text).collect::<Vec<_>>().join(",")),
        v => panic!("a value the scripts do not make: {v:?}"),
    }
}

fn doc_text(doc: Option<Vec<(String, OrderedValue)>>) -> String {
    fn walk(d: &[(String, OrderedValue)], prefix: &str, out: &mut Vec<String>) {
        for (k, v) in d {
            let p = format!("{prefix}{k}");
            match v {
                OrderedValue::Doc(kids) => {
                    out.push(format!("{p}=map"));
                    walk(kids, &format!("{p}."), out);
                }
                OrderedValue::Leaf(x) => out.push(format!("{p}={}", val_text(x))),
            }
        }
    }
    let Some(doc) = doc else { return "-".into() };
    let mut out = Vec::new();
    walk(&doc, "", &mut out);
    out.join("\n")
}

fn ord_text(o: &FieldOrd) -> String {
    let floor = match o.floor {
        None => "-".to_string(),
        Some(Floor::U { stamp, prev, others }) => {
            format!("u:{}:{}:{}", st_text(&stamp), prev.wall_ms, u8::from(others))
        }
        Some(Floor::R { stamp }) => format!("r:{}", st_text(&stamp)),
    };
    let firsts: Vec<String> = o
        .firsts
        .iter()
        .map(|(w, c)| format!("{}={}@{}", w.node.to_bytes()[0] as char, st_text(&c.0), c.1))
        .collect();
    let last = o.last.map_or("-".to_string(), |c| format!("{}@{}", st_text(&c.0), c.1));
    let lvl = o.lvl.map_or("-".to_string(), |s| st_text(&s));
    format!("floor={floor} firsts=[{}] last={last} lvl={lvl}", firsts.join(";"))
}

fn ords_text(s: &MergeState) -> String {
    s.ords
        .iter()
        .map(|((life, at), o)| {
            let at = match at {
                OrdAt::Root => "R".to_string(),
                OrdAt::Field(p) => p.segments().join("."),
            };
            format!("{}@{at} {}", st_text(life), ord_text(o))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The digest and the two texts of a member.
fn member(w: &Writer) -> (String, String, String) {
    let (d, o) = (doc_text(w.state.ordered_document(&w.opts)), ords_text(&w.state));
    (fnv(&format!("{d}\n--\n{o}")), d, o)
}

// ---- the replay -------------------------------------------------------------------------------------------------------------

fn stamp_of(j: &J) -> Option<MStamp> {
    (!j.is_null()).then(|| MStamp {
        hlc: kimmy_core::hlc::Hlc::new(j[0].as_u64().unwrap(), 0),
        writer: wid_at(j[1].as_str().unwrap().chars().next().unwrap(), j[2].as_u64().unwrap_or(0)),
    })
}

/// The precondition under which a creation base folds nothing (the counter cell): a creation (an insert or
/// an upsert) is refused while any life is alive, so at an accepted creation the writer holds no counter
/// slot in an alive life. Asserted by the harness at every accepted creation; there is no assert in the
/// product code.
fn slot_in_alive_life(w: &Writer) -> bool {
    let alive = w.alive();
    w.state.slots.keys().any(|k| alive.contains(&k.0))
}

/// One write of a script on a writer.
fn write(w: &mut Writer, step: &J) -> Option<Written> {
    let creation = matches!(step["op"].as_str(), Some("insert" | "upsert_inc" | "upsert_soi"));
    let held = creation && slot_in_alive_life(w);
    let r = write_op(w, step);
    assert!(r.is_none() || !held, "a creation was accepted over a slot of an alive life");
    r
}

fn write_op(w: &mut Writer, step: &J) -> Option<Written> {
    let skew = step["skew"].as_u64().unwrap();
    let at = || path(&step["path"]);
    let arg = &step["arg"];
    match step["op"].as_str().unwrap() {
        "insert" => w.insert(&fields(arg), skew),
        "upsert_inc" => w.upsert_inc(&at(), arg["v"].as_i64().unwrap(), skew),
        "upsert_soi" => w
            .upsert_soi(
                &fields(&arg["fields"]),
                &at(),
                arg["n"]["v"].as_i64().unwrap(),
                &fields(&arg["sets"]),
                skew,
            )
            .map(|(_, second)| second),
        "set" => w.write_set(&at(), value(arg), skew),
        "unset" => w.write_unset(&at(), skew),
        "inc" => w.write_inc(&at(), arg["v"].as_i64().unwrap(), skew),
        "replace" => w.write_replace(&fields(arg), skew),
        "update" => w.write_update(&ops(arg), skew).ok().flatten(),
        "delete" => w.write_delete(skew),
        o => panic!("operator {o}"),
    }
}

/// What a replay found: the steps compared, the writes and receives among them, and the first step
/// of each script that disagrees (with the Rust text of the member there).
#[derive(Default)]
pub struct Found {
    pub scripts: usize,
    pub steps: usize,
    pub writes: usize,
    pub refused: usize,
    pub receives: usize,
    pub ordered_docs: usize,
    pub ord_keys: usize,
    pub bad: Vec<String>,
    /// What the DELIVERY scripts did (see [`replay_delivery`]); all 0 for the plain scripts.
    pub delivery: Delivery,
}

/// How many times the delivery scripts hit each way a state reaches a member besides its own writes, as the Rust replay ran them.
#[derive(Default, Debug, PartialEq)]
pub struct Delivery {
    /// A snapshot: the peer's current state joined, no oplog entry.
    pub snaps: usize,
    pub backups: usize,
    /// A restore: the member back at its backup under the next epoch.
    pub restores: usize,
    pub scripts_with_a_restore: usize,
    /// A pull: a window of the peer's oplog entries (a stamp each), in stamp order.
    pub pulls: usize,
    /// A pull whose window stopped with entries after it (a truncated window).
    pub truncated: usize,
    /// The entries of the windows the receiver's vector covered, and so skipped; and those it joined.
    pub skipped: usize,
    pub applied: usize,
    /// A write by a member after one of its restores (a stamp of epoch above 0).
    pub writes_after_a_restore: usize,
    /// A script in which a member that had restored then joined a pulled entry (or took a snapshot).
    pub restore_then_a_pull_that_joined: usize,
    pub restore_then_a_snapshot: usize,
}

/// The plain scripts: local writes and whole-state receives.
pub fn replay(opts: &JoinOptions) -> Found {
    replay_scripts(opts, false)
}

/// The DELIVERY scripts: the same writes, with the other ways a state reaches a member (a snapshot, a window of the peer's oplog entries
/// each skipped when the receiver's vector covers it and joined otherwise, a window cut short, a backup and a restore under the next epoch).
pub fn replay_delivery(opts: &JoinOptions) -> Found {
    replay_scripts(opts, true)
}

type Oplog = BTreeMap<MStamp, MergeState>;

fn replay_scripts(opts: &JoinOptions, delivery: bool) -> Found {
    let all: J = serde_json::from_str(SCRIPTS).unwrap();
    assert_eq!(all["model6_sha256"], MODEL6_SHA256);
    let mut f = Found::default();
    for sc in all["scripts"].as_array().unwrap() {
        if sc.get("delivery").is_some() != delivery {
            continue;
        }
        f.scripts += 1;
        let names: Vec<char> =
            "ABCD".chars().take(sc["members"].as_u64().unwrap() as usize).collect();
        let mut ws: BTreeMap<char, Writer> =
            names.iter().map(|c| (*c, Writer::new(wid(*c), opts.clone()))).collect();
        // the delivery scripts' members also keep an oplog (the state each entry carries, by stamp) and a backup of it
        let mut logs: BTreeMap<char, Oplog> = names.iter().map(|c| (*c, Oplog::new())).collect();
        let mut saved: BTreeMap<char, (MergeState, Oplog)> = BTreeMap::new();
        let mut restored: std::collections::BTreeSet<char> = std::collections::BTreeSet::new();
        let (mut restored_pull, mut restored_snap) = (false, false);
        let tag = format!("{} {}", sc["kind"].as_str().unwrap(), sc["seed"]);
        'steps: for (i, step) in sc["steps"].as_array().unwrap().iter().enumerate() {
            f.steps += 1;
            let who = step["m"].as_str().unwrap().chars().next().unwrap();
            let kind = step["k"].as_str().unwrap();
            let peer = || step["from"].as_str().unwrap().chars().next().unwrap();
            if kind == "recv" {
                f.receives += 1;
                let from = ws[&peer()].state.clone();
                ws.get_mut(&who).unwrap().receive_report(&from);
            } else if kind == "snap" {
                f.receives += 1;
                f.delivery.snaps += 1;
                let from = ws[&peer()].state.clone();
                ws.get_mut(&who).unwrap().receive_report(&from);
                restored_snap |= restored.contains(&who);
            } else if kind == "backup" {
                f.delivery.backups += 1;
                saved.insert(who, (ws[&who].state.clone(), logs[&who].clone()));
            } else if kind == "restore" {
                f.delivery.restores += 1;
                let (state, log) = saved.get(&who).expect("a restore follows a backup").clone();
                let w = ws.get_mut(&who).unwrap();
                // the state goes back and the writer takes the next epoch; its clock is kept (the model's restore)
                w.state = state;
                w.id = wid_at(who, step["epoch"].as_u64().unwrap());
                logs.insert(who, log);
                restored.insert(who);
            } else if kind == "pull" {
                f.delivery.pulls += 1;
                f.delivery.truncated += usize::from(step["truncated"].as_bool().unwrap());
                let from = peer();
                let stamps = step["stamps"].as_array().unwrap();
                for (n, sj) in stamps.iter().enumerate() {
                    f.receives += 1;
                    let st = stamp_of(sj).unwrap();
                    let Some(entry) = logs[&from].get(&st).cloned() else {
                        f.bad.push(format!("{tag} step {i}: {from} holds no entry at {sj}"));
                        break 'steps;
                    };
                    // the covered skip: the receiver's vector covers every writer of the entry's state
                    let w = ws.get_mut(&who).unwrap();
                    let covered =
                        entry.vv.iter().all(|(wr, h)| w.state.vv.get(wr).is_some_and(|x| x >= h));
                    if covered != step["skipped"][n].as_bool().unwrap() {
                        f.bad.push(format!(
                            "{tag} step {i} entry {sj}: rust skips {covered}, model {}",
                            step["skipped"][n]
                        ));
                        break 'steps;
                    }
                    if covered {
                        f.delivery.skipped += 1;
                        continue;
                    }
                    f.delivery.applied += 1;
                    let changed = w.receive_report(&entry).changed;
                    restored_pull |= restored.contains(&who);
                    // the entry is appended when it changed the state and was not held (the model's rule)
                    let appends = changed && !logs[&who].contains_key(&st);
                    if appends != step["appended"][n].as_bool().unwrap() {
                        f.bad.push(format!(
                            "{tag} step {i} entry {sj}: rust appends {appends}, model {}",
                            step["appended"][n]
                        ));
                        break 'steps;
                    }
                    if appends {
                        logs.get_mut(&who).unwrap().insert(st, entry);
                    }
                }
            } else {
                f.writes += 1;
                let w = ws.get_mut(&who).unwrap();
                let got = if delivery && step["op"] == "upsert_soi" {
                    // two writes, each an oplog entry the model serves: the creation, then the increment on its life (as the sim does)
                    let held = slot_in_alive_life(w);
                    let arg = &step["arg"];
                    let (skew, at) = (step["skew"].as_u64().unwrap(), path(&step["path"]));
                    let first = w.upsert_soi_create(
                        &fields(&arg["fields"]),
                        &at,
                        &fields(&arg["sets"]),
                        skew,
                    );
                    assert!(
                        first.is_none() || !held,
                        "a creation was accepted over a slot of an alive life"
                    );
                    let second = first.as_ref().and_then(|c| {
                        logs.get_mut(&who).unwrap().insert(c.stamp, w.state.clone());
                        w.upsert_inc_on(c.stamp, &at, arg["n"]["v"].as_i64().unwrap(), skew)
                    });
                    second.map(|x| x.stamp)
                } else {
                    write(w, step).map(|x| x.stamp)
                };
                f.refused += usize::from(got.is_none());
                if let Some(st) = got {
                    f.delivery.writes_after_a_restore +=
                        usize::from(st.writer.epoch != Epoch::from_u64(0));
                    if delivery {
                        logs.get_mut(&who).unwrap().insert(st, ws[&who].state.clone());
                    }
                }
                if got != stamp_of(&step["res"]) {
                    f.bad.push(format!(
                        "{tag} step {i} {} {who}: rust stamp {got:?}, model {}",
                        step["op"], step["res"]
                    ));
                    break 'steps;
                }
            }
            let (d, dt, ot) = member(&ws[&who]);
            if d != step["d"].as_str().unwrap() {
                f.bad.push(format!(
                    "{tag} step {i} {}: {who} reads and orders differently from the model\n{dt}\n--\n{ot}",
                    step["op"]
                ));
                break 'steps;
            }
            f.ordered_docs += usize::from(dt != "-");
            f.ord_keys += ws[&who].state.ords.len();
        }
        f.delivery.scripts_with_a_restore += usize::from(!restored.is_empty());
        f.delivery.restore_then_a_pull_that_joined += usize::from(restored_pull);
        f.delivery.restore_then_a_snapshot += usize::from(restored_snap);
        if f.bad.last().is_some_and(|b| b.starts_with(&tag)) {
            continue;
        }
        // a full-mesh sync, twice: every member holds the model's final state
        for _ in 0..2 {
            for a in &names {
                for b in &names {
                    if a != b {
                        let from = ws[b].state.clone();
                        ws.get_mut(a).unwrap().receive_report(&from);
                    }
                }
            }
        }
        for n in &names {
            if member(&ws[n]).0 != sc["final"].as_str().unwrap() {
                f.bad.push(format!("{tag}: member {n} after the full sync"));
            }
        }
    }
    f
}

/// The scripts that differ between the model with ABSORBED off and with it on (the flip's evidence), as
/// `(kind, seed, first differing step, whether the digest of that step differs)`. A step index past the last step
/// is the final digest after the full sync. Every script of the fixture is the ABSORBED-on model's; the `off`
/// record is what the ABSORBED-off model made of the same seed.
pub fn off_differences() -> Vec<(String, u64, usize, bool)> {
    let all: J = serde_json::from_str(SCRIPTS).unwrap();
    all["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|sc| sc.get("off").is_some() && sc.get("delivery").is_none())
        .map(|sc| {
            let off = &sc["off"];
            let i = off["step"].as_u64().unwrap() as usize;
            let steps = sc["steps"].as_array().unwrap();
            let differs = match (steps.get(i), off["off_step"].as_object()) {
                (Some(on), Some(off_step)) => {
                    on["d"] != off_step["d"]
                        || on["res"] != off_step["res"]
                        || on["op"] != off_step["op"]
                }
                (None, _) => off["off_final"] != sc["final"],
                (Some(_), None) => true,
            };
            (sc["kind"].as_str().unwrap().to_string(), sc["seed"].as_u64().unwrap(), i, differs)
        })
        .collect()
}

/// How many upserts of the scripts name a nested path (the incremented path, or a creation or `$set` field): the shape the Rust
/// fix of O2-2 and the model's A2.3 fix are compared on.
pub fn nested_upserts() -> usize {
    let all: J = serde_json::from_str(SCRIPTS).unwrap();
    let nested = |s: &J| {
        let mut names: Vec<String> = vec![
            s["path"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_str().unwrap())
                .collect::<Vec<_>>()
                .join("."),
        ];
        if s["op"] == "upsert_soi" {
            for k in ["fields", "sets"] {
                names.extend(
                    s["arg"][k]["v"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|f| f[0].as_str().unwrap().to_string()),
                );
            }
        }
        s["path"].as_array().unwrap().len() > 1 || names.iter().any(|n| n.contains('.'))
    };
    all["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|sc| sc.get("delivery").is_none())
        .flat_map(|sc| sc["steps"].as_array().unwrap())
        .filter(|s| s["k"] == "op" && matches!(s["op"].as_str(), Some("upsert_inc" | "upsert_soi")))
        .filter(|s| nested(s))
        .count()
}
