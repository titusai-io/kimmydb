//! Tests of the order state (`ord.rs`): the join and its laws, the read (the materialised document
//! in field order), the feed, and the mutation switches.
//!
//! The expected results are the reference model's own, from the A2.3 Python model
//! (`q7step0-model/`, `model6.py`): `tests/data/ord_goldens.json` holds, for each named
//! scenario, the state of every writer entry (what the Rust joins, in many delivery orders), the
//! ordered document and the order state the model's observer ends with, the writes themselves
//! (for the replay of the feed) and the script (for the replay through the `Writer` API: every
//! write and receive, in program order). `tests/data/ord_join_laws.json` holds the distinct order entries
//! of the model's `probe_ord_join_laws` (150 nested runs). Both are made by
//! `tests/data/gen_ord_goldens.py`; the hash of `model6.py` is in the fixtures.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as J;

use super::ord::BornMemo;
use super::prop_tests::{Rng, check_laws_on, opts};
use super::*;
use crate::hlc::Hlc;
use crate::ids::NodeId;

const GOLDENS: &str = include_str!("../../tests/data/ord_goldens.json");
const JOIN_LAWS: &str = include_str!("../../tests/data/ord_join_laws.json");
const RANDOM_STATES: &str = include_str!("../../tests/data/ord_random_states.json");

/// The `sha256` of `model6.py` (the reviewed A2.3 tree) that the fixtures were made from.
const MODEL6_SHA256: &str = "72da18ebbf3bb383d0741d1f10deb4265cb884dd960d49047913204016ebab3d";

// ---- the fixtures ---------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    let mut b = [0u8; 16];
    b[..name.len()].copy_from_slice(name.as_bytes());
    NodeId::from_bytes(b)
}

/// A writer is `[node, epoch]`; an epoch is a JSON number, or a decimal string when it does not fit
/// one (a restore's epoch in the model is the start HLC shifted by 64 bits). It is the epoch's 18
/// big-endian bytes, so the order of epochs is the model's.
fn writer(j: &J) -> WriterId {
    let n: u128 = match &j[1] {
        J::String(s) => s.parse().unwrap(),
        e => u128::from(e.as_u64().unwrap_or_else(|| panic!("a writer is [node, epoch], not {j}"))),
    };
    let mut b = [0u8; Epoch::LEN];
    b[Epoch::LEN - 16..].copy_from_slice(&n.to_be_bytes());
    WriterId { node: node(j[0].as_str().unwrap()), epoch: Epoch(b) }
}

fn stamp(j: &J) -> MStamp {
    MStamp {
        hlc: Hlc::new(j[0].as_u64().unwrap(), 0),
        writer: writer(&J::Array(vec![j[1].clone(), j[2].clone()])),
    }
}

fn stamp_opt(j: &J) -> Option<MStamp> {
    (!j.is_null()).then(|| stamp(j))
}

fn path(j: &J) -> Path {
    Path::new(j.as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_string())).unwrap()
}

fn ord_at(j: &J) -> OrdAt {
    if j.is_null() { OrdAt::Root } else { OrdAt::Field(path(j)) }
}

fn value(j: &J) -> Value {
    match j["t"].as_str().unwrap() {
        "unset" => Value::Unset,
        "null" => Value::Null,
        "map" => Value::Map,
        "i" => Value::Int64(j["v"].as_i64().unwrap()),
        "d" => Value::Double(j["v"].as_str().unwrap().parse().unwrap()),
        "s" => Value::String(j["v"].as_str().unwrap().to_string()),
        "a" => Value::Array(j["v"].as_array().unwrap().iter().map(value).collect()),
        t => panic!("value tag {t}"),
    }
}

fn field_ord(j: &J) -> FieldOrd {
    let floor = match &j["floor"] {
        f if f.is_null() => None,
        f if f[0] == "u" => Some(Floor::U {
            stamp: stamp(&f[1]),
            prev: Hlc::new(f[2].as_u64().unwrap(), 0),
            others: f[3].as_bool().unwrap(),
        }),
        f => Some(Floor::R { stamp: stamp(&f[1]) }),
    };
    let cand = |c: &J| (stamp(&c[0]), c[1].as_u64().unwrap());
    FieldOrd {
        floor,
        firsts: j["firsts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| (writer(&f[0]), (stamp(&f[1]), f[2].as_u64().unwrap())))
            .collect(),
        last: (!j["last"].is_null()).then(|| cand(&j["last"])),
        lvl: stamp_opt(&j["lvl"]),
    }
}

fn ords(j: &J) -> BTreeMap<(MStamp, OrdAt), FieldOrd> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|e| ((stamp(&e[0]), ord_at(&e[1])), field_ord(&e[2])))
        .collect()
}

fn obs_key(j: &J) -> ObsKey {
    (stamp(&j[0]), writer(&j[1]), stamp_opt(&j[2]))
}

fn num(j: &J) -> Num {
    Num::Int64(i128::from(j.as_i64().unwrap()))
}

pub(super) fn state(j: &J) -> MergeState {
    let mut s = MergeState::default();
    for l in j["lives"].as_array().unwrap() {
        let kind = if l[3] == "insert" { LifeKind::Insert } else { LifeKind::Upsert };
        s.lives.insert(
            stamp(&l[0]),
            Life {
                seen: l[1].as_array().unwrap().iter().map(stamp).collect(),
                order: l[2].as_array().unwrap().iter().map(path).collect(),
                kind,
            },
        );
    }
    s.dead = j["dead"].as_array().unwrap().iter().map(stamp).collect();
    for d in j["deletes"].as_array().unwrap() {
        s.deletes.insert(
            stamp(&d[0]),
            d[1].as_array()
                .unwrap()
                .iter()
                .map(|e| (writer(&e[0]), Hlc::new(e[1].as_u64().unwrap(), 0)))
                .collect(),
        );
    }
    // The model's registers carry the `obs` map of the stage-A format (r[4]); the Rust cell keeps what a
    // base write had counted as the ABSORBED pair of each slot instead: the highest `(stamp, total)` any
    // register observed of it (the model with ABSORBED on folds every `obs` into `absorbed`).
    type SlotKey = (MStamp, Path, WriterId, Option<MStamp>);
    let mut absorbed: BTreeMap<SlotKey, (MStamp, Num)> = BTreeMap::new();
    for r in j["regs"].as_array().unwrap() {
        let p = path(&r[1]);
        for e in r[4].as_array().unwrap() {
            let (l, w, g) = obs_key(&J::Array(e.as_array().unwrap()[..3].to_vec()));
            let seen = (stamp(&e[3]), num(&e[4]));
            let slot = absorbed.entry((l, p.clone(), w, g)).or_insert(seen);
            if seen.0 > slot.0 {
                *slot = seen;
            }
        }
        let generation = (!r[5].is_null()).then(|| {
            let g = &r[5];
            let vv = g["vv"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| (writer(&e[0]), Hlc::new(e[1].as_u64().unwrap(), 0)))
                .collect();
            let seen = g["seen"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| (obs_key(&J::Array(e.as_array().unwrap()[..3].to_vec())), num(&e[3])))
                .collect();
            Gen::start(stamp(&g["stamp"]), &vv, seen)
        });
        let reg =
            Reg::new(stamp(&r[2]), value(&r[3]), generation, r[6].as_bool().unwrap()).unwrap();
        s.regs.insert((stamp(&r[0]), path(&r[1])), reg);
    }
    for sl in j["slots"].as_array().unwrap() {
        let key = (stamp(&sl[0]), path(&sl[1]), writer(&sl[2]), stamp_opt(&sl[3]));
        let absorbed = absorbed.get(&key).copied();
        s.slots.insert(key, Slot { stamp: stamp(&sl[4]), total: num(&sl[5]), absorbed });
    }
    s.vv = j["vv"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (writer(&e[0]), Hlc::new(e[1].as_u64().unwrap(), 0)))
        .collect();
    s.ords = ords(&j["ords"]);
    s
}

/// The ordered document as a pre-order list of `(path, value)`, a map as `Value::Map`.
pub(super) fn flatten(doc: &[(String, OrderedValue)]) -> Vec<(Path, Value)> {
    fn walk(at: &[String], doc: &[(String, OrderedValue)], out: &mut Vec<(Path, Value)>) {
        for (name, v) in doc {
            let mut segs = at.to_vec();
            segs.push(name.clone());
            let p = Path::new(segs.clone()).unwrap();
            match v {
                OrderedValue::Leaf(x) => out.push((p, x.clone())),
                OrderedValue::Doc(kids) => {
                    out.push((p, Value::Map));
                    walk(&segs, kids, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(&[], doc, &mut out);
    out
}

pub(super) fn goldens() -> J {
    let g: J = serde_json::from_str(GOLDENS).unwrap();
    assert_eq!(g["model6_sha256"], MODEL6_SHA256);
    g
}

fn expected(sc: &J) -> Vec<(Path, Value)> {
    sc["expected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                path(&e[0]),
                value(&if e[1] == "map" { serde_json::json!({"t": "map"}) } else { e[1].clone() }),
            )
        })
        .collect()
}

/// Every delivery order of the entries of a scenario to a fresh observer, or a fixed sample.
fn orders(n: usize) -> Vec<Vec<usize>> {
    fn perms(k: usize, cur: &mut Vec<usize>, used: &mut Vec<bool>, out: &mut Vec<Vec<usize>>) {
        if cur.len() == k {
            out.push(cur.clone());
            return;
        }
        for i in 0..k {
            if !used[i] {
                used[i] = true;
                cur.push(i);
                perms(k, cur, used, out);
                cur.pop();
                used[i] = false;
            }
        }
    }
    if n <= 6 {
        let mut out = Vec::new();
        perms(n, &mut Vec::new(), &mut vec![false; n], &mut out);
        return out;
    }
    let mut rng = Rng(0x0DD0_0DD0 + n as u64);
    let mut out = vec![(0..n).collect::<Vec<_>>(), (0..n).rev().collect()];
    for _ in 0..200 {
        let mut k: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            k.swap(i, rng.below(i + 1));
        }
        out.push(k);
    }
    out
}

/// The scenarios the model's observer and the Rust join do not agree on, under `o`, by name: the
/// read side (the entries joined in `ordered`, the ordered document and the order state compared
/// with the model's) and the feed (the writes replayed, the order state of each entry compared).
fn disagreements(o: &JoinOptions, all_orders: bool) -> (Vec<String>, Vec<String>, Vec<String>) {
    let g = goldens();
    let (mut read, mut feed, mut writer) = (Vec::new(), Vec::new(), Vec::new());
    for (name, sc) in g["scenarios"].as_object().unwrap() {
        let entries: Vec<MergeState> =
            sc["entries"].as_array().unwrap().iter().map(state).collect();
        let want = expected(sc);
        let want_ords = ords(&sc["ords"]);
        let ords_to_try = if all_orders {
            orders(entries.len())
        } else {
            vec![(0..entries.len()).collect(), (0..entries.len()).rev().collect()]
        };
        let read_ok = ords_to_try.iter().all(|ord| {
            let mut s = MergeState::default();
            for &i in ord {
                s = join(&s, &entries[i], o).0;
            }
            s.ords == want_ords
                && s.ordered_document(o).map(|d| flatten(&d)).as_deref() == Some(&want[..])
        });
        if !read_ok {
            read.push(name.clone());
        }
        // (the replay of the bare feed starts every writer from the seed alone and replays one entry per
        // write: not for a scenario in which a writer receives, or in which a write is two entries (a
        // `$setOnInsert` upsert), which only the Writer API replay below can follow)
        let not_bare = sc["script"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["k"] == "recv" || s["op"] == "upsert_soi");
        if !not_bare && !replay(sc, o) {
            feed.push(name.clone());
        }
        // the Writer API: the script replayed through four writers, and the entries THEY built
        // delivered to a fresh observer in the same orders
        let writer_ok = replay_script(sc, o).is_ok_and(|r| {
            r.entries.len() == entries.len()
                && ords_to_try.iter().all(|ord| {
                    let mut s = MergeState::default();
                    for &i in ord {
                        s = join(&s, &r.entries[i], o).0;
                    }
                    s.ords == want_ords
                        && s.ordered_document(o).map(|d| flatten(&d)).as_deref() == Some(&want[..])
                })
        });
        if !writer_ok {
            writer.push(name.clone());
        }
    }
    (read, feed, writer)
}

/// Replay the writes of a scenario through the feed, each writer on a state of its own that
/// started from what the seed insert gave it: the order state of every write must be the
/// model's. (In these scenarios a writer receives nothing but the seed insert.)
fn replay(sc: &J, o: &JoinOptions) -> bool {
    let entries = sc["entries"].as_array().unwrap();
    let writes = sc["writes"].as_array().unwrap();
    let has_seed = sc["seed_entries"].as_u64().unwrap() == 1;
    let mut by_writer: BTreeMap<WriterId, MergeState> = BTreeMap::new();
    let mut seed: Option<MergeState> = None;
    for (k, w) in writes.iter().enumerate() {
        let at = stamp(&w["stamp"]);
        let ms = by_writer.entry(at.writer).or_insert_with(|| seed.clone().unwrap_or_default());
        if let Some(life) = stamp_opt(&w["life"]) {
            let items: Vec<(OrdAt, OrdMode)> = w["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|it| {
                    let mode = match it[1].as_str().unwrap() {
                        "w" => OrdMode::Write,
                        "r" => OrdMode::Reset,
                        "u" => OrdMode::Unset,
                        "L" => OrdMode::Level,
                        m => panic!("mode {m}"),
                    };
                    (ord_at(&it[0]), mode)
                })
                .collect();
            let events = ord_events(&items, o);
            ms.ord_apply(life, at, Hlc::new(w["known"].as_u64().unwrap(), 0), &events, o);
        }
        if ms.ords != ords(&entries[k]["ords"]) {
            return false;
        }
        if k == 0 && has_seed {
            seed = Some(ms.clone());
        }
    }
    true
}

// ---- the replay through the Writer API --------------------------------------------------

/// A document value of a script: `{"t": "doc", "v": [[name, value], ...]}` is a sub-document.
fn script_value(j: &J) -> Value {
    if j["t"] == "doc" {
        Value::Document(
            j["v"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| (f[0].as_str().unwrap().to_string(), script_value(&f[1])))
                .collect(),
        )
    } else {
        value(j)
    }
}

/// The fields of a whole document of a script (insert, replace): one-segment paths.
fn script_fields(j: &J) -> Vec<(Path, Value)> {
    j["v"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                Path::new(f[0].as_str().unwrap().split('.').map(str::to_string)).unwrap(),
                script_value(&f[1]),
            )
        })
        .collect()
}

/// The operators of an `update` of a script.
fn script_ops(j: &J) -> Vec<UpdateOp> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|o| {
            let p = path(&o[1]);
            match o[0].as_str().unwrap() {
                "set" => UpdateOp::Set(p, script_value(&o[2])),
                "unset" => UpdateOp::Unset(p),
                "inc" => UpdateOp::Inc(p, script_value(&o[2])),
                k => panic!("operator {k}"),
            }
        })
        .collect()
}

/// What a replay of a scenario's script through the `Writer`s gave.
struct Replayed {
    /// The state of the writer right after each write, in program order (the model's entries).
    entries: Vec<MergeState>,
}

/// Replay the script of a scenario through the Writer API: four writers (`I`, `A`, `B`, `C`), the
/// seed insert by `I` received by the other three, then every write and every receive in program
/// order. Each write must give or refuse a stamp as the model's did, at the model's stamp, leave the
/// model's order state in the entry, and show the model's ordered document to its own writer.
/// `Err` names the first step that disagrees.
fn replay_script(sc: &J, o: &JoinOptions) -> Result<Replayed, String> {
    let wid = |c: &str| writer(&serde_json::json!([c, 0]));
    let mut ws: BTreeMap<String, Writer> = ["I", "A", "B", "C"]
        .iter()
        .map(|c| ((*c).to_string(), Writer::new(wid(c), o.clone())))
        .collect();
    let (entries_j, own_j) = (sc["entries"].as_array().unwrap(), sc["own"].as_array().unwrap());
    let mut entries: Vec<MergeState> = Vec::new();
    let mut sent: BTreeMap<MStamp, MergeState> = BTreeMap::new();
    let mut k = 0usize;
    // check the entry just made against the model's k-th
    let mut check = |ws: &BTreeMap<String, Writer>,
                     who: &str,
                     at: MStamp,
                     entries: &mut Vec<MergeState>,
                     sent: &mut BTreeMap<MStamp, MergeState>,
                     what: &str|
     -> Result<(), String> {
        let w = &ws[who];
        // the model's entry is not normalised (a deleted life keeps its order entries until a join
        // drops them); the writer's state is, as every receiver's join leaves it
        let want_ords = join(&MergeState::default(), &state(&entries_j[k]), o).0.ords;
        if w.state.ords != want_ords {
            return Err(format!(
                "{what}: the order state after write {k} differs from the model's"
            ));
        }
        let own = &own_j[k];
        let got = w.state.ordered_document(o).map(|d| flatten(&d));
        let want = (!own.is_null()).then(|| expected(&serde_json::json!({ "expected": own })));
        if got != want {
            return Err(format!("{what}: {who} reads {got:?} after write {k}, the model {want:?}"));
        }
        sent.insert(at, w.state.clone());
        entries.push(w.state.clone());
        k += 1;
        Ok(())
    };
    if !sc["init"].is_null() {
        let fields = script_fields(&sc["init"]);
        let who = "I";
        let w = ws.get_mut(who).unwrap();
        let written = w.insert(&fields, 0).ok_or("the seed insert is refused")?;
        if written.stamp != stamp(&sc["entry_stamps"][0]) {
            return Err("the seed insert has another stamp".into());
        }
        check(&ws, who, written.stamp, &mut entries, &mut sent, "seed insert")?;
        let st = ws[who].state.clone();
        for c in ["A", "B", "C"] {
            ws.get_mut(c).unwrap().receive_report(&st);
        }
    }
    for (i, step) in sc["script"].as_array().unwrap().iter().enumerate() {
        let who = step["who"].as_str().unwrap();
        if step["k"] == "recv" {
            let from = &sent[&stamp(&step["stamp"])];
            ws.get_mut(who).unwrap().receive_report(&from.clone());
            continue;
        }
        let name = step["op"].as_str().unwrap();
        let what = format!("step {i}: {who} {name}");
        let w = ws.get_mut(who).unwrap();
        if let Some(c) = step["clock"].as_u64() {
            w.clock = Hlc::new(c, 0);
        }
        let skew = step["skew"].as_u64().unwrap();
        let at = || path(&step["path"]);
        let arg = &step["arg"];
        let got = match name {
            "insert" => w.insert(&script_fields(arg), skew),
            "set" => w.write_set(&at(), script_value(arg), skew),
            "unset" => w.write_unset(&at(), skew),
            "inc" => w.write_inc(&at(), arg["v"].as_i64().unwrap(), skew),
            "replace" => w.write_replace(&script_fields(arg), skew),
            "delete" => w.write_delete(skew),
            "upsert_inc" => w.upsert_inc(&at(), arg["v"].as_i64().unwrap(), skew),
            "update" | "setmany" => w.write_update(&script_ops(arg), skew),
            "upsert_soi" => w
                .upsert_soi(
                    &script_fields(&arg["fields"]),
                    &at(),
                    arg["n"]["v"].as_i64().unwrap(),
                    &script_fields(&arg["sets"]),
                    skew,
                )
                .map(|(_, second)| second),
            n => panic!("operator {n}"),
        };
        match (got, stamp_opt(&step["stamp"])) {
            (None, None) => {}
            (Some(g), Some(want)) if g.stamp == want => {
                check(&ws, who, g.stamp, &mut entries, &mut sent, &what)?;
            }
            (g, want) => {
                return Err(format!(
                    "{what}: the writer gave {:?}, the model {want:?}",
                    g.map(|x| x.stamp)
                ));
            }
        }
    }
    Ok(Replayed { entries })
}

// ---- the read: the goldens of the materialised order --------------------------------

/// The scenarios, by name. A new scenario of the generator is a new line here.
const SCENARIOS: [&str; 49] = [
    "a23_upsert_inc_of_a_nested_path_then_set",
    "a23_upsert_soi_field_inc_nested_then_set",
    "a23_upsert_soi_nested_field_then_set",
    "a23_upsert_soi_nested_set_field_then_set",
    "a23_upsert_soi_no_field_inc_nested_then_set",
    "a_scalar_over_a_map_voids_the_places_of_its_fields",
    "a_set_on_insert_upsert_feeds_both_of_its_writes",
    "a_standalone_noop_unset_leaves_nothing",
    "a_subdocument_set_that_only_reorders_is_a_write",
    "a_writer_with_two_lives_writes_into_the_newest",
    "approximate_case",
    "concurrent_creation_a_first",
    "concurrent_creation_b_first",
    "concurrent_replaces",
    "delete_reinsert_with_a_concurrent_set",
    "descendant_above_the_floor_is_not_a_survivor",
    "descendant_survivor_of_an_unset_map",
    "hybrid_floor_on_an_update_after_a_receive",
    "hybrid_noop_unset_rides_on_an_update",
    "inc_of_a_double_below_a_map_is_a_register_write",
    "inc_unset_replace_concurrent",
    "level_floor_nested_subdocument_set",
    "level_floor_replace",
    "level_floor_subdocument_set",
    "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
    "receive_a_concurrent_replace_then_set",
    "receive_a_concurrent_unset_then_set",
    "receive_a_survivor_increment_then_unset",
    "replace_drops_a_field_then_set_appends",
    "replace_reorders_even_unchanged",
    "replace_with_an_empty_document_has_no_update_bit",
    "same_new_path_from_two_writers",
    "sequential_rules",
    "set_then_concurrent_unset",
    "slot_above_the_floor_is_not_a_survivor",
    "survivor_increment_after_unset",
    "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
    "survivor_older_than_the_fields_own_write_lowers_its_position",
    "survivor_placement_a_inc_before_c",
    "survivor_placement_b_unset_knew_c",
    "two_lives_by_two_upserts",
    "two_writers_each_receive_the_others_write_then_write",
    "unset_then_concurrent_set",
    "unset_then_receive_a_survivor_increment",
    "update_bit_survivor_after_all_the_updates_fields",
    "update_with_inc_and_unset_of_a_map",
    "update_without_bit_survivor_by_prev",
    "updates_that_are_writes_by_what_they_name_alone",
    "write_below_a_map_is_a_write_to_the_map",
];

/// The field order of each named scenario, as pre-order lists of paths, pinned here AND equal to
/// the model's: each scenario's entries (the states of the writes) are joined in every delivery
/// order (all permutations up to six entries, 202 shuffles above) and the ordered document, the
/// order state and the values equal the model's. The arrays below are what the model prints
/// (`gen_ord_goldens.py`); a Rust result that differs from them is a bug, never a golden to tune.
#[test]
fn the_materialised_field_order_equals_the_models_on_every_scenario() {
    let g = goldens();
    let names: Vec<&str> = g["scenarios"].as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(names, SCENARIOS, "the scenarios of the fixture");
    let o = JoinOptions::default();
    let (mut joined, mut fields) = (0usize, 0usize);
    for (name, sc) in g["scenarios"].as_object().unwrap() {
        let entries: Vec<MergeState> =
            sc["entries"].as_array().unwrap().iter().map(state).collect();
        let want = expected(sc);
        let want_ords = ords(&sc["ords"]);
        assert_eq!(sc["absorbed_does_not_matter"], true, "{name}");
        for ord in orders(entries.len()) {
            let mut s = MergeState::default();
            for &i in &ord {
                s = join(&s, &entries[i], &o).0;
            }
            joined += 1;
            let doc = s.ordered_document(&o).expect("a life is alive");
            assert_eq!(flatten(&doc), want, "{name}: delivery order {ord:?}");
            assert_eq!(s.ords, want_ords, "{name}: the order state, delivery order {ord:?}");
            // the flat view agrees with the ordered one
            let vis = s.visible(&o).unwrap();
            assert_eq!(vis.len(), want.len(), "{name}");
            for (p, v) in &want {
                assert_eq!(vis.get(p), Some(v), "{name}: {p}");
            }
        }
        fields += want.len();
    }
    // pinned: what the test covered
    assert_eq!((names.len(), joined, fields), (49, 3512, 202));
}

fn paths(doc: &[(Path, Value)]) -> Vec<String> {
    doc.iter().map(|(p, _)| p.to_string()).collect()
}

fn order_of(name: &str) -> Vec<String> {
    let g = goldens();
    paths(&expected(&g["scenarios"][name]))
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_string()).collect()
}

/// The materialised-order goldens, spelled out. The arrays are what the A2.3 model gives
/// (the same arrays are in `tests/data/ord_goldens.json`, from which the test above checks the
/// join; these are pinned in the source so that a reader sees the rules).
#[test]
fn the_field_order_of_the_named_scenarios_is_pinned() {
    let pins: [(&str, &[&str]); 49] = [
        ("a_set_on_insert_upsert_feeds_both_of_its_writes", &["a", "b", "c", "e", "n", "d"]),
        ("a_standalone_noop_unset_leaves_nothing", &["a", "b", "m", "m.p", "nope", "e"]),
        ("a_subdocument_set_that_only_reorders_is_a_write", &["m", "m.q", "m.p", "m.r", "k", "z"]),
        ("a_writer_with_two_lives_writes_into_the_newest", &["x", "y", "z"]),
        ("hybrid_floor_on_an_update_after_a_receive", &["a", "c", "b", "e"]),
        ("inc_of_a_double_below_a_map_is_a_register_write", &["x", "m", "m.f", "w"]),
        ("receive_a_concurrent_replace_then_set", &["c", "b", "a", "d", "e"]),
        ("receive_a_concurrent_unset_then_set", &["q", "r", "p"]),
        ("receive_a_survivor_increment_then_unset", &["x", "w", "b"]),
        ("two_writers_each_receive_the_others_write_then_write", &["x", "q", "r", "s"]),
        ("unset_then_receive_a_survivor_increment", &["x", "a", "w", "e"]),
        ("approximate_case", &["z", "q", "p", "r"]),
        ("concurrent_creation_a_first", &["a", "c", "d"]),
        ("concurrent_creation_b_first", &["a", "d", "c"]),
        ("concurrent_replaces", &["b", "a", "c", "d", "e"]),
        ("delete_reinsert_with_a_concurrent_set", &["y", "x"]),
        ("descendant_above_the_floor_is_not_a_survivor", &["x", "z", "a", "a.b"]),
        ("descendant_survivor_of_an_unset_map", &["z", "a", "a.d", "y"]),
        ("hybrid_noop_unset_rides_on_an_update", &["b", "c", "a", "e"]),
        ("inc_unset_replace_concurrent", &["c", "a"]),
        (
            "level_floor_nested_subdocument_set",
            &["a", "a.d", "a.d.y", "a.d.x", "a.d.k", "a.e", "a.f"],
        ),
        ("level_floor_replace", &["b", "m", "m.q", "m.p", "m.r", "a", "c"]),
        ("level_floor_subdocument_set", &["m", "m.q", "m.p", "m.r"]),
        ("replace_drops_a_field_then_set_appends", &["c", "a", "b"]),
        ("replace_reorders_even_unchanged", &["c", "b", "a"]),
        ("replace_with_an_empty_document_has_no_update_bit", &["a", "d"]),
        ("same_new_path_from_two_writers", &["a", "c", "d"]),
        ("sequential_rules", &["yy", "zz", "z", "b", "m", "m.r", "m.p", "a"]),
        ("set_then_concurrent_unset", &["q"]),
        ("slot_above_the_floor_is_not_a_survivor", &["x", "z", "a"]),
        ("survivor_increment_after_unset", &["x", "a", "b"]),
        ("survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew", &["b", "d", "a"]),
        ("survivor_older_than_the_fields_own_write_lowers_its_position", &["x", "a", "d"]),
        ("survivor_placement_a_inc_before_c", &["b", "a", "c"]),
        ("survivor_placement_b_unset_knew_c", &["a", "c", "m", "m.r"]),
        ("two_lives_by_two_upserts", &["x", "y", "z"]),
        ("unset_then_concurrent_set", &["q", "r", "p"]),
        ("update_bit_survivor_after_all_the_updates_fields", &["x", "w", "z", "y", "a"]),
        (
            "update_with_inc_and_unset_of_a_map",
            &["a", "b", "s", "j", "j.v", "j.w", "z", "y", "k", "k.v"],
        ),
        ("update_without_bit_survivor_by_prev", &["x", "a", "w"]),
        ("write_below_a_map_is_a_write_to_the_map", &["x", "z", "a", "a.b", "w"]),
        ("a_scalar_over_a_map_voids_the_places_of_its_fields", &["x", "m", "m.q", "m.p", "w"]),
        (
            "updates_that_are_writes_by_what_they_name_alone",
            &["a", "m", "m.q", "m.p", "k", "k.j", "e"],
        ),
        (
            "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
            &["a", "n", "k", "k.z", "m", "m.x", "m.y"],
        ),
        ("a23_upsert_inc_of_a_nested_path_then_set", &["a", "a.b", "c"]),
        ("a23_upsert_soi_field_inc_nested_then_set", &["s", "a", "a.b", "c"]),
        ("a23_upsert_soi_nested_field_then_set", &["a", "a.b", "n", "c"]),
        ("a23_upsert_soi_nested_set_field_then_set", &["a", "a.b", "n", "c"]),
        ("a23_upsert_soi_no_field_inc_nested_then_set", &["a", "a.b", "c"]),
    ];
    for (name, want) in pins {
        assert_eq!(order_of(name), strs(want), "{name}");
    }
}

/// The feed reproduces the model's order state: each scenario's writes, replayed through
/// `ord_events` and `ord_apply` on the writers' own states, leave the order state the model's
/// entry holds after every write.
#[test]
fn the_feed_reproduces_the_models_order_state_after_every_write() {
    let (read, feed, writer) = disagreements(&JoinOptions::default(), true);
    assert_eq!((read, feed), (Vec::<String>::new(), Vec::<String>::new()));
    assert_eq!(writer, Vec::<String>::new(), "the Writer API reproduces the model");
}

/// The scenarios, replayed through four `Writer`s (the model's members) by their scripts, give the
/// model's stamps, refusals, order state and own ordered document after every write, and the
/// entries the writers built give the model's order state and document to an observer in every
/// delivery order (all permutations up to six entries, 202 shuffles above), as the entries the
/// model built do. A writer that receives before it writes feeds the join of what it received and
/// its own history.
#[test]
fn the_writer_reproduces_the_models_scenarios_after_every_write() {
    let g = goldens();
    let o = JoinOptions::default();
    let (mut writes, mut receives, mut refused, mut joined) = (0, 0, 0, 0);
    for (name, sc) in g["scenarios"].as_object().unwrap() {
        let r = replay_script(sc, &o).unwrap_or_else(|e| panic!("{name}: {e}"));
        let script = sc["script"].as_array().unwrap();
        receives += script.iter().filter(|s| s["k"] == "recv").count();
        refused += script.iter().filter(|s| s["k"] == "op" && s["stamp"].is_null()).count();
        writes += r.entries.len();
        let want = expected(sc);
        let want_ords = ords(&sc["ords"]);
        for ord in orders(r.entries.len()) {
            let mut s = MergeState::default();
            for &i in &ord {
                s = join(&s, &r.entries[i], &o).0;
            }
            joined += 1;
            assert_eq!(s.ords, want_ords, "{name}: the order state, delivery order {ord:?}");
            let doc = s.ordered_document(&o).expect("a life is alive");
            assert_eq!(flatten(&doc), want, "{name}: delivery order {ord:?}");
            assert_eq!(s.check_decoded(), Ok(()), "{name}: a state a writer built is accepted");
        }
    }
    // pinned: what the replay covered (the seed insert of a seeded scenario is one of its writes)
    assert_eq!((writes, receives, refused, joined), (201, 8, 9, 3512));
}

/// The O2 watch item: a top-level field named `""` is fed as `OrdAt::Field([""])`, never as the
/// root. A writer inserts it with a sibling, sets a sub-document at it, and replaces the document
/// (the root's level mark): the order state holds both keys, the root only a level mark, and the
/// state is accepted by both decoders and round trips.
#[test]
fn the_writer_feeds_a_top_level_field_named_empty_as_a_field() {
    let o = JoinOptions::default();
    let mut w = Writer::new(super::tests::wid('A', 0), o.clone());
    let empty = Path::flat("").unwrap();
    let x = Path::flat("x").unwrap();
    let sub = Value::Document(vec![("c".into(), Value::Int64(1))]);
    let life = w
        .insert(&[(empty.clone(), Value::Int64(0)), (x.clone(), Value::Int64(1))], 0)
        .unwrap()
        .stamp;
    assert!(w.state.ords.contains_key(&(life, OrdAt::Field(empty.clone()))));
    assert!(!w.state.ords.contains_key(&(life, OrdAt::Root)));
    w.write_set(&empty, sub.clone(), 0).unwrap();
    let r = w.write_replace(&[(x.clone(), Value::Int64(2)), (empty.clone(), sub)], 0).unwrap();
    let root = &w.state.ords[&(life, OrdAt::Root)];
    assert_eq!(
        (root.floor, root.firsts.len(), root.last, root.lvl),
        (None, 0, None, Some(r.stamp))
    );
    let field = &w.state.ords[&(life, OrdAt::Field(empty.clone()))];
    assert_eq!(field.lvl, Some(r.stamp));
    assert_eq!(w.state.check_decoded(), Ok(()));
    let enc = w.state.encode_compact();
    assert_eq!(MergeState::decode_compact(&enc).as_ref(), Ok(&w.state));
    let doc = w.state.ordered_document(&o).unwrap();
    assert_eq!(doc.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["x", ""]);
}

// ---- the mutation switches ----------------------------------------------------------

/// Each order switch, set wrong, changes the outcome of the scenarios named: the killers. A
/// switch of the read side changes the joined order state or the document; a switch of the feed
/// changes the order state a replayed write leaves (the bare feed, `ord_events` and `ord_apply`,
/// on scenarios in which no writer receives); a switch of the writer changes what the `Writer`
/// API gives when the script is replayed through it (stamps, refusals, order state, own document,
/// and the entries joined in every delivery order). The shortest killers: `ord_no_last` by
/// `approximate_case` (the field whose only valid candidate is its last write), `ord_latest` by
/// `same_new_path_from_two_writers`, `ord_no_survivor_clamp` by `survivor_increment_after_unset`,
/// `ord_no_level_floor` by `level_floor_subdocument_set`, `ord_ignores_bit` by
/// `update_bit_survivor_after_all_the_updates_fields`, `ord_no_reset` and `ord_no_pos` by
/// `level_floor_replace` and `sequential_rules`, and the switches of the writer's own rules by:
/// `ord_no_ancestor` `write_below_a_map_is_a_write_to_the_map`, `ord_no_hybrid`
/// `hybrid_noop_unset_rides_on_an_update` and `hybrid_floor_on_an_update_after_a_receive`,
/// `ord_noop_unset_stands` `a_standalone_noop_unset_leaves_nothing`, `ord_bit_unpacked`
/// `update_bit_survivor_after_all_the_updates_fields`, `ord_no_first`
/// `same_new_path_from_two_writers`, `ord_replace_without_reset` `replace_reorders_even_unchanged`,
/// `ord_reorder_is_noop` `a_subdocument_set_that_only_reorders_is_a_write`.
#[test]
fn the_order_switches_change_the_outcome_of_named_scenarios() {
    type Set = fn(&mut JoinOptions);
    // (switch, how to set it, scenarios killed through the read, scenarios killed through the feed)
    // (switch, how to set it, killed through the read, the bare feed, the Writer API)
    type Row<'a> = (&'a str, Set, &'a [&'a str], &'a [&'a str], &'a [&'a str]);
    let switches: [Row; 16] = [
        (
            "ord_keep_all",
            |o| o.set_ord_keep_all(true),
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_replaces",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_replaces",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "unset_then_concurrent_set",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_replaces",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_latest",
            |o| o.set_ord_latest(true),
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "level_floor_nested_subdocument_set",
                "receive_a_concurrent_replace_then_set",
                "same_new_path_from_two_writers",
                "sequential_rules",
                "two_writers_each_receive_the_others_write_then_write",
                "update_with_inc_and_unset_of_a_map",
            ],
            &[],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "same_new_path_from_two_writers",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_placement_a_inc_before_c",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "update_with_inc_and_unset_of_a_map",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_no_last",
            |o| o.set_ord_no_last(true),
            &["approximate_case"],
            &[],
            &["approximate_case"],
        ),
        (
            "ord_by_name",
            |o| o.set_ord_by_name(true),
            &[
                "a23_upsert_soi_field_inc_nested_then_set",
                "a23_upsert_soi_nested_field_then_set",
                "a23_upsert_soi_nested_set_field_then_set",
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_set_on_insert_upsert_feeds_both_of_its_writes",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_creation_b_first",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "sequential_rules",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
            &[],
            &[
                "a23_upsert_soi_field_inc_nested_then_set",
                "a23_upsert_soi_nested_field_then_set",
                "a23_upsert_soi_nested_set_field_then_set",
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_set_on_insert_upsert_feeds_both_of_its_writes",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_creation_b_first",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "sequential_rules",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_no_survivor_clamp",
            |o| o.set_ord_no_survivor_clamp(true),
            &[
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "replace_with_an_empty_document_has_no_update_bit",
                "survivor_increment_after_unset",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "unset_then_receive_a_survivor_increment",
                "update_without_bit_survivor_by_prev",
            ],
            &[],
            &[
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "replace_with_an_empty_document_has_no_update_bit",
                "survivor_increment_after_unset",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "unset_then_receive_a_survivor_increment",
                "update_without_bit_survivor_by_prev",
            ],
        ),
        (
            "ord_no_level_floor",
            |o| o.set_ord_no_level_floor(true),
            &[
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "concurrent_replaces",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "updates_that_are_writes_by_what_they_name_alone",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "concurrent_replaces",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "updates_that_are_writes_by_what_they_name_alone",
            ],
        ),
        (
            "ord_ignores_bit",
            |o| o.set_ord_ignores_bit(true),
            &[
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
            ],
            &[],
            &[
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
            ],
        ),
        (
            "ord_no_reset",
            |o| o.set_ord_no_reset(true),
            &[],
            &[
                "a_subdocument_set_that_only_reorders_is_a_write",
                "concurrent_replaces",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "sequential_rules",
                "updates_that_are_writes_by_what_they_name_alone",
            ],
            &[
                "a_subdocument_set_that_only_reorders_is_a_write",
                "concurrent_replaces",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "receive_a_concurrent_replace_then_set",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "sequential_rules",
                "updates_that_are_writes_by_what_they_name_alone",
            ],
        ),
        (
            "ord_no_pos",
            |o| o.set_ord_no_pos(true),
            &[],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "unset_then_concurrent_set",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_set_on_insert_upsert_feeds_both_of_its_writes",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_no_ancestor",
            |o| o.set_ord_no_ancestor(true),
            &[],
            &[],
            &[
                "a23_upsert_inc_of_a_nested_path_then_set",
                "a23_upsert_soi_field_inc_nested_then_set",
                "a23_upsert_soi_nested_field_then_set",
                "a23_upsert_soi_nested_set_field_then_set",
                "a23_upsert_soi_no_field_inc_nested_then_set",
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "sequential_rules",
                "survivor_placement_b_unset_knew_c",
                "update_with_inc_and_unset_of_a_map",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_no_hybrid",
            |o| o.set_ord_no_hybrid(true),
            &[],
            &[],
            &["hybrid_floor_on_an_update_after_a_receive", "hybrid_noop_unset_rides_on_an_update"],
        ),
        (
            "ord_noop_unset_stands",
            |o| o.set_ord_noop_unset_stands(true),
            &[],
            &[],
            &["a_standalone_noop_unset_leaves_nothing"],
        ),
        (
            "ord_bit_unpacked",
            |o| o.set_ord_bit_unpacked(true),
            &[],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_unset_replace_concurrent",
                "replace_drops_a_field_then_set_appends",
                "sequential_rules",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
            ],
            &[
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_unset_replace_concurrent",
                "replace_drops_a_field_then_set_appends",
                "sequential_rules",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
            ],
        ),
        (
            "ord_no_first",
            |o| o.set_ord_no_first(true),
            &[],
            &[
                "a23_upsert_inc_of_a_nested_path_then_set",
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "approximate_case",
                "concurrent_creation_a_first",
                "concurrent_creation_b_first",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "same_new_path_from_two_writers",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "two_lives_by_two_upserts",
                "unset_then_concurrent_set",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
            &[
                "a23_upsert_inc_of_a_nested_path_then_set",
                "a23_upsert_soi_field_inc_nested_then_set",
                "a23_upsert_soi_nested_field_then_set",
                "a23_upsert_soi_nested_set_field_then_set",
                "a23_upsert_soi_no_field_inc_nested_then_set",
                "a_scalar_over_a_map_voids_the_places_of_its_fields",
                "a_set_on_insert_upsert_feeds_both_of_its_writes",
                "a_standalone_noop_unset_leaves_nothing",
                "a_subdocument_set_that_only_reorders_is_a_write",
                "a_writer_with_two_lives_writes_into_the_newest",
                "approximate_case",
                "concurrent_creation_a_first",
                "concurrent_creation_b_first",
                "concurrent_replaces",
                "delete_reinsert_with_a_concurrent_set",
                "descendant_above_the_floor_is_not_a_survivor",
                "descendant_survivor_of_an_unset_map",
                "hybrid_floor_on_an_update_after_a_receive",
                "hybrid_noop_unset_rides_on_an_update",
                "inc_of_a_double_below_a_map_is_a_register_write",
                "inc_unset_replace_concurrent",
                "level_floor_nested_subdocument_set",
                "level_floor_replace",
                "level_floor_subdocument_set",
                "maps_that_survive_an_unset_through_empty_children_trail_in_path_order",
                "receive_a_concurrent_replace_then_set",
                "receive_a_concurrent_unset_then_set",
                "receive_a_survivor_increment_then_unset",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "replace_with_an_empty_document_has_no_update_bit",
                "same_new_path_from_two_writers",
                "sequential_rules",
                "set_then_concurrent_unset",
                "slot_above_the_floor_is_not_a_survivor",
                "survivor_increment_after_unset",
                "survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew",
                "survivor_older_than_the_fields_own_write_lowers_its_position",
                "survivor_placement_a_inc_before_c",
                "survivor_placement_b_unset_knew_c",
                "two_lives_by_two_upserts",
                "two_writers_each_receive_the_others_write_then_write",
                "unset_then_concurrent_set",
                "unset_then_receive_a_survivor_increment",
                "update_bit_survivor_after_all_the_updates_fields",
                "update_with_inc_and_unset_of_a_map",
                "update_without_bit_survivor_by_prev",
                "updates_that_are_writes_by_what_they_name_alone",
                "write_below_a_map_is_a_write_to_the_map",
            ],
        ),
        (
            "ord_replace_without_reset",
            |o| o.set_ord_replace_without_reset(true),
            &[],
            &[],
            &[
                "concurrent_replaces",
                "inc_unset_replace_concurrent",
                "level_floor_replace",
                "receive_a_concurrent_replace_then_set",
                "replace_drops_a_field_then_set_appends",
                "replace_reorders_even_unchanged",
                "sequential_rules",
            ],
        ),
        (
            "ord_reorder_is_noop",
            |o| o.set_ord_reorder_is_noop(true),
            &[],
            &[],
            &[
                "a_subdocument_set_that_only_reorders_is_a_write",
                "concurrent_replaces",
                "level_floor_replace",
                "receive_a_concurrent_replace_then_set",
                "replace_reorders_even_unchanged",
                "sequential_rules",
                "updates_that_are_writes_by_what_they_name_alone",
            ],
        ),
    ];
    for (name, set, read_killers, feed_killers, writer_killers) in switches {
        let mut o = JoinOptions::default();
        set(&mut o);
        let (read, feed, writer) = disagreements(&o, false);
        assert_eq!(read, strs(read_killers), "{name}: scenarios killed through the read");
        assert_eq!(feed, strs(feed_killers), "{name}: scenarios killed through the feed");
        assert_eq!(writer, strs(writer_killers), "{name}: scenarios killed through the Writer API");
        assert!(
            !read.is_empty() || !feed.is_empty() || !writer.is_empty(),
            "{name} changes nothing"
        );
    }
}

// ---- the entry and its join: small cases --------------------------------------------

pub(super) fn w(k: u8) -> WriterId {
    WriterId { node: NodeId::from_bytes([k; 16]), epoch: Epoch::from_u64(0) }
}

pub(super) fn at(hlc: u64, k: u8) -> MStamp {
    MStamp { hlc: Hlc::new(hlc, 0), writer: w(k) }
}

pub(super) fn u(hlc: u64, k: u8, prev: u64, others: bool) -> Floor {
    Floor::U { stamp: at(hlc, k), prev: Hlc::new(prev, 0), others }
}

#[test]
fn an_unset_floor_is_strict_and_a_replace_floor_is_inclusive() {
    let o = JoinOptions::default();
    let under_u = FieldOrd { floor: Some(u(5, 1, 4, false)), ..FieldOrd::default() };
    assert!(!under_u.valid(&(at(5, 1), 0), &o), "a write at the unset's own stamp is voided");
    assert!(!under_u.valid(&(at(4, 2), 0), &o));
    assert!(under_u.valid(&(at(6, 2), 0), &o));
    let under_r = FieldOrd { floor: Some(Floor::R { stamp: at(5, 1) }), ..FieldOrd::default() };
    assert!(under_r.valid(&(at(5, 1), 0), &o), "the replace's own write stands");
    assert!(!under_r.valid(&(at(4, 2), 0), &o));
    let mut keep = JoinOptions::default();
    keep.set_ord_keep_all(true);
    assert!(under_u.valid(&(at(1, 2), 0), &keep), "the mutant never voids");
}

/// The model orders a floor as the tuple `(stamp, "u", prev, bit)` against `(stamp, "r")`; since
/// `"r" < "u"`, at one stamp a replace is BELOW an unset and the model's `max` is the unset (O1-5 of the
/// review: the Rust had them the other way round, and said the opposite in its doc).
#[test]
fn floors_order_by_stamp_then_replace_below_unset_then_prev_then_the_bit() {
    let r = Floor::R { stamp: at(5, 1) };
    assert!(r < u(5, 1, 0, false), "a replace sorts below an unset at one stamp");
    assert!(r < u(5, 1, 9, true));
    assert_eq!(r.max(u(5, 1, 3, false)), u(5, 1, 3, false), "the model's max is the unset");
    assert!(u(5, 1, 3, true) < u(5, 1, 4, false), "prev before the bit");
    assert!(u(5, 1, 4, false) < u(5, 1, 4, true), "the bit sorts above its absence");
    assert!(u(6, 1, 0, false) > r && u(4, 1, 9, true) < r, "the stamp first");
    // and the join takes that floor when both are at one stamp (peer states only)
    let o = JoinOptions::default();
    let (x, y) = (
        FieldOrd { floor: Some(r), ..FieldOrd::default() },
        FieldOrd { floor: Some(u(5, 1, 3, false)), ..FieldOrd::default() },
    );
    assert_eq!(ord_join(&x, &y, &o).floor, Some(u(5, 1, 3, false)));
    assert_eq!(ord_join(&y, &x, &o).floor, Some(u(5, 1, 3, false)));
}

#[test]
fn the_join_voids_what_the_joined_floor_voids_and_keeps_the_earliest_per_writer() {
    let o = JoinOptions::default();
    let a = FieldOrd {
        firsts: [(w(1), (at(3, 1), 0)), (w(2), (at(9, 2), 1))].into(),
        last: Some((at(9, 2), 1)),
        ..FieldOrd::default()
    };
    let b = FieldOrd {
        floor: Some(u(5, 3, 4, true)),
        firsts: [(w(1), (at(7, 1), 2))].into(),
        last: Some((at(7, 1), 2)),
        lvl: Some(at(8, 3)),
    };
    let j = ord_join(&a, &b, &o);
    assert_eq!(j, ord_join(&b, &a, &o));
    assert_eq!(j.floor, Some(u(5, 3, 4, true)));
    // writer 1's first at 3 is voided, its candidate at 7 stands (b's), writer 2's at 9 stands
    assert_eq!(j.firsts, [(w(1), (at(7, 1), 2)), (w(2), (at(9, 2), 1))].into());
    assert_eq!(j.last, Some((at(9, 2), 1)));
    assert_eq!(j.lvl, Some(at(8, 3)));
    assert_eq!(ord_join(&j, &j, &o), j);
    assert!(j.is_normal(&o));
    assert!(!a.is_normal(&o) || a.floor.is_none());
    // the earliest valid candidate is the position
    assert_eq!(j.born(&o).map(|b| b.stamp()), Some(at(7, 1)));
}

#[test]
fn a_non_normal_entry_is_normalised_by_its_join_with_itself() {
    let o = JoinOptions::default();
    let bad = FieldOrd {
        floor: Some(u(5, 1, 4, false)),
        firsts: [(w(1), (at(2, 1), 0))].into(),
        last: Some((at(2, 1), 0)),
        lvl: None,
    };
    assert!(!bad.is_normal(&o));
    let j = ord_join(&bad, &bad, &o);
    assert!(j.is_normal(&o));
    assert!(j.firsts.is_empty() && j.last.is_none() && j.floor == bad.floor);
}

/// A state of one alive life `(1, A)` with a register stamped `(hlc, A)` at each given field and the
/// order entries given.
fn two_fields(fields: &[(&str, u64, Option<FieldOrd>)]) -> (MergeState, JoinOptions) {
    let (a, life) = (w(b'A'), at(1, b'A'));
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    s.vv.insert(a, Hlc::new(99, 0));
    for (name, hlc, x) in fields {
        let p = Path::flat(name).unwrap();
        let reg = Reg::new(at(*hlc, b'A'), Value::Int64(1), None, false).unwrap();
        s.regs.insert((life, p.clone()), reg);
        if let Some(x) = x {
            s.ords.insert((life, OrdAt::Field(p)), x.clone());
        }
    }
    (s, JoinOptions::default())
}

fn cand_ord(hlc: u64) -> FieldOrd {
    FieldOrd {
        firsts: [(w(b'A'), (at(hlc, b'A'), 0))].into(),
        last: Some((at(hlc, b'A'), 0)),
        ..FieldOrd::default()
    }
}

#[test]
fn each_writer_keeps_the_earliest_of_the_candidates_the_floor_lets_stand() {
    let o = JoinOptions::default();
    let a = FieldOrd { firsts: [(w(1), (at(3, 1), 0))].into(), ..FieldOrd::default() };
    let b = FieldOrd { firsts: [(w(1), (at(7, 1), 2))].into(), ..FieldOrd::default() };
    assert_eq!(ord_join(&a, &b, &o).firsts, [(w(1), (at(3, 1), 0))].into());
    assert_eq!(ord_join(&b, &a, &o).firsts, [(w(1), (at(3, 1), 0))].into());
    // a position breaks a tie of stamps (the same write can name a field twice at one stamp)
    let c = FieldOrd { firsts: [(w(1), (at(3, 1), 5))].into(), ..FieldOrd::default() };
    assert_eq!(ord_join(&c, &a, &o).firsts, [(w(1), (at(3, 1), 0))].into());
}

#[test]
fn an_entry_a_floor_voids_gives_no_position_even_in_a_state_nobody_normalised() {
    // `x` holds a candidate its own floor voids (a state no writer or decoder makes), `y` a valid one
    let bad = FieldOrd {
        floor: Some(u(5, b'A', 4, false)),
        firsts: [(w(b'A'), (at(2, b'A'), 0))].into(),
        last: Some((at(2, b'A'), 0)),
        ..FieldOrd::default()
    };
    let o = JoinOptions::default();
    assert_eq!(bad.born(&o), None, "no valid candidate");
    let (s, o) = two_fields(&[("x", 9, Some(bad.clone())), ("y", 9, Some(cand_ord(9)))]);
    let names = |s: &MergeState| {
        s.ordered_document(&o).unwrap().iter().map(|(n, _)| n.clone()).collect::<Vec<_>>()
    };
    // the voided candidate of x does not put it before y: it has no position and goes last
    assert_eq!(names(&s), ["y", "x"]);
    // a join normalises an entry only the second side has, and leaves one only the first has
    let life = at(1, b'A');
    let key = (life, OrdAt::Field(Path::flat("x").unwrap()));
    let base = MergeState { lives: s.lives.clone(), vv: s.vv.clone(), ..MergeState::default() };
    let mut with_bad = base.clone();
    with_bad.ords.insert(key.clone(), bad.clone());
    assert!(
        join(&base, &with_bad, &o).0.ords[&key].is_normal(&o),
        "an entry only `t` has is normalised"
    );
    assert_eq!(
        join(&with_bad, &base, &o).0.ords[&key],
        bad,
        "an entry only `s` has is kept as it is"
    );
}

#[test]
fn a_field_with_no_position_goes_after_the_fields_that_have_one() {
    // `a` has no order entry at all and sorts before `b` by name
    let (s, o) = two_fields(&[("a", 9, None), ("b", 9, Some(cand_ord(9)))]);
    let doc = s.ordered_document(&o).unwrap();
    assert_eq!(doc.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["b", "a"]);
    let al = s.alive_set(o.retention);
    assert!(s.born_of(&Path::flat("a").unwrap(), &al, &o).is_none());
    assert!(s.born_of(&Path::flat("b").unwrap(), &al, &o).is_some());
    // fields with no position among themselves go by path
    let (s, o) = two_fields(&[("b", 9, None), ("a", 9, None)]);
    let doc = s.ordered_document(&o).unwrap();
    assert_eq!(doc.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["a", "b"]);
}

#[test]
fn events_take_their_positions_among_the_siblings_a_write_names() {
    let o = JoinOptions::default();
    let f = |n: &str| OrdAt::Field(Path::new(n.split('.')).unwrap());
    let items = [
        (f("x"), OrdMode::Write),
        (f("m"), OrdMode::Write),
        (f("m.a"), OrdMode::Write),
        (f("x"), OrdMode::Write), // the same item again: dropped
        (f("y"), OrdMode::Write),
        (f("z"), OrdMode::Unset),
        (OrdAt::Root, OrdMode::Level),
        (f("m.b"), OrdMode::Reset),
        (f("x"), OrdMode::Reset), // the same field in another mode: kept, at its first position
    ];
    let got: Vec<(String, OrdMode, u64)> = ord_events(&items, &o)
        .into_iter()
        .map(|e| {
            let at = match &e.at {
                OrdAt::Root => "<root>".to_string(),
                OrdAt::Field(p) => p.to_string(),
            };
            (at, e.mode, e.pos)
        })
        .collect();
    let want = [
        ("x", OrdMode::Write, 0),
        ("m", OrdMode::Write, 1),
        ("m.a", OrdMode::Write, 0),
        ("y", OrdMode::Write, 2),
        ("z", OrdMode::Unset, 0),
        ("<root>", OrdMode::Level, 0),
        ("m.b", OrdMode::Reset, 1),
        ("x", OrdMode::Reset, 0),
    ];
    assert_eq!(got, want.map(|(a, m, p)| (a.to_string(), m, p)));
    // a mutant that ignores the position
    let mut no_pos = JoinOptions::default();
    no_pos.set_ord_no_pos(true);
    assert!(ord_events(&items, &no_pos).iter().all(|e| e.pos == 0));
}

#[test]
fn the_feed_only_moves_forward_the_floor_the_level_and_the_first() {
    let o = JoinOptions::default();
    let (life, a) = (at(1, b'A'), w(b'A'));
    let root = |s: &MergeState| s.ords.get(&(life, OrdAt::Root)).cloned();
    let x = OrdAt::Field(Path::flat("x").unwrap());
    let mut s = MergeState::default();
    // the level mark: the newer stamp stays whichever comes second
    s.ord_apply(
        life,
        at(9, b'A'),
        Hlc::new(8, 0),
        &ord_events(&[(OrdAt::Root, OrdMode::Level)], &o),
        &o,
    );
    s.ord_apply(
        life,
        at(5, b'A'),
        Hlc::new(4, 0),
        &ord_events(&[(OrdAt::Root, OrdMode::Level)], &o),
        &o,
    );
    assert_eq!(root(&s).and_then(|e| e.lvl), Some(at(9, b'A')));
    // the unset floor: the larger stays; a write at a stamp under it is voided at once
    s.ord_apply(
        life,
        at(9, b'A'),
        Hlc::new(8, 0),
        &ord_events(&[(x.clone(), OrdMode::Unset)], &o),
        &o,
    );
    s.ord_apply(
        life,
        at(5, b'A'),
        Hlc::new(4, 0),
        &ord_events(&[(x.clone(), OrdMode::Unset)], &o),
        &o,
    );
    assert_eq!(s.ords[&(life, x.clone())].floor, Some(u(9, b'A', 8, false)));
    // the first of a writer stays, the last follows
    let y = OrdAt::Field(Path::flat("y").unwrap());
    s.ord_apply(
        life,
        at(10, b'A'),
        Hlc::new(9, 0),
        &ord_events(&[(y.clone(), OrdMode::Write)], &o),
        &o,
    );
    s.ord_apply(
        life,
        at(11, b'A'),
        Hlc::new(10, 0),
        &ord_events(&[(y.clone(), OrdMode::Write)], &o),
        &o,
    );
    let e = &s.ords[&(life, y)];
    assert_eq!(e.firsts, [(a, (at(10, b'A'), 0))].into());
    assert_eq!(e.last, Some((at(11, b'A'), 0)));
}

/// Where the associativity of the order join ends. Commutative and idempotent on any NORMAL
/// entries, it is associative only on the entries of histories WITHIN RETENTION (the ADR-085 limit):
/// one writer's earlier and later first for a path never both stand next to a floor between them
/// (the later state has seen the floor, and carries it). Here are two firsts of one writer that no
/// such history keeps side by side and an unset between them: the groupings disagree. (Beyond
/// retention an honest history reaches the same shape: `ord_codec_tests`, the seed 1421 case.) The
/// model's probe and the simulation below pin the reachable side; the writer's feed keeps to it
/// (`a_writers_states_only_grow` in `ord_codec_tests`).
#[test]
fn the_order_join_is_associative_only_on_the_entries_a_history_reaches() {
    let o = JoinOptions::default();
    let early = FieldOrd {
        firsts: [(w(1), (at(5, 1), 0))].into(),
        last: Some((at(5, 1), 0)),
        ..FieldOrd::default()
    };
    let late = FieldOrd {
        firsts: [(w(1), (at(9, 1), 0))].into(),
        last: Some((at(9, 1), 0)),
        ..FieldOrd::default()
    };
    let unset = FieldOrd { floor: Some(u(7, 2, 6, false)), ..FieldOrd::default() };
    let left = ord_join(&ord_join(&early, &late, &o), &unset, &o);
    let right = ord_join(&early, &ord_join(&late, &unset, &o), &o);
    assert_ne!(left, right, "the first at 5 is voided before the first at 9 can be asked");
    assert!(left.firsts.is_empty() && right.firsts == [(w(1), (at(9, 1), 0))].into());
    for (a, b) in [(&early, &late), (&late, &unset), (&early, &unset)] {
        assert_eq!(ord_join(a, b, &o), ord_join(b, a, &o), "commutative all the same");
        assert_eq!(ord_join(a, a, &o), *a, "idempotent all the same");
    }
    // the reachable form of the same history: the later state has seen the unset and carries it
    let later_seen = ord_join(&late, &unset, &o);
    let l = ord_join(&ord_join(&early, &later_seen, &o), &unset, &o);
    let r = ord_join(&early, &ord_join(&later_seen, &unset, &o), &o);
    assert_eq!(l, r);
}

/// A chain of survivors `n1`, `n1.n2`, ... to the given depth, as a state a peer can send (the
/// reviewer's O1-1): one alive life, a register `Int64(5)` stamped `(2, A)` at every path and an
/// order entry with an unset floor `(10, A)` (prev 9) at every path. Every entry is normal, its
/// life listed, its stamps covered: `check_decoded` accepts it. Only `n1` is visible (its scalar
/// hides the children stamped no later), and every path below it is a survivor candidate of the
/// one above.
fn survivor_chain(depth: usize) -> MergeState {
    let (a, life) = (w(b'A'), at(1, b'A'));
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    s.vv.insert(a, Hlc::new(20, 0));
    for i in 1..=depth {
        let p = Path::new((1..=i).map(|k| format!("n{k}"))).unwrap();
        let reg = Reg::new(at(2, b'A'), Value::Int64(5), None, false).unwrap();
        s.regs.insert((life, p.clone()), reg);
        s.ords.insert(
            (life, OrdAt::Field(p)),
            FieldOrd { floor: Some(u(10, b'A', 9, false)), ..FieldOrd::default() },
        );
    }
    s
}

/// O1-1: the read of a chain of survivors is linear in its depth. The unmemoised recursion (the
/// model's) evaluates `born_of` 2^(depth - 1) times for the chain; the memoised read once per path.
/// Depth 40 is about 8 kilobytes of compact bytes that decode: unmemoised it would take about 5.5e11
/// evaluations. The bound on time is one second in a debug build: the work is under 10^4 cell visits,
/// milliseconds; only the exponential blow-up could miss the bound, by days.
#[test]
fn a_chain_of_survivors_is_read_in_linear_time() {
    let o = JoinOptions::default();
    // the same document, memoised or not, at a depth the unmemoised read can do
    let small = survivor_chain(10);
    let (memo, plain) = (BornMemo::new(true), BornMemo::new(false));
    let (d1, d2) =
        (small.ordered_document_with(&o, &memo), small.ordered_document_with(&o, &plain));
    assert_eq!(d1, d2);
    assert_eq!(memo.evaluations(), 10, "one evaluation per path");
    assert_eq!(plain.evaluations(), 1 << 9, "the unmemoised read is exponential: 2^(depth - 1)");
    // the deep chain: decodes, reads in linear evaluations and in time
    let deep = survivor_chain(40);
    let enc = deep.encode_compact();
    assert!(enc.len() < 10_000, "{} bytes", enc.len());
    let back = MergeState::decode_compact(&enc).expect("a decodable state");
    let memo = BornMemo::new(true);
    let t = std::time::Instant::now();
    let doc = back.ordered_document_with(&o, &memo).unwrap();
    let took = t.elapsed();
    assert_eq!(memo.evaluations(), 40, "linear in the depth");
    assert_eq!(doc.len(), 1);
    assert!(took < std::time::Duration::from_secs(1), "{took:?}");
    for depth in [8usize, 16, 32] {
        let m = BornMemo::new(true);
        survivor_chain(depth).ordered_document_with(&o, &m).unwrap();
        assert_eq!(m.evaluations(), depth);
    }
}

// ---- the read on the model's random states ------------------------------------------------

/// The ordered document of a state as the fixture spells it.
fn expected_flat(j: &J) -> Vec<(Path, Value)> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let v = if e[1] == "map" { serde_json::json!({"t": "map"}) } else { e[1].clone() };
            (path(&e[0]), value(&v))
        })
        .collect()
}

/// O1-4 of the O1 review. The goldens have no path with entries in two alive lives, no increment below
/// an unset map and no `lvl` in two lives, so five mutants of the read survived them. This fixture is
/// 79 states of the model's random nested runs (300 seeds, 30 steps; every writer entry of every
/// member's oplog and every final state, no implicit markers since O4 is not built, 2,094 distinct), with
/// the model's ordered document under ABSORBED off (the counter cell the Rust has): chosen as the
/// smallest states that each of the five mutants changes, and a spread of others. The Rust read must
/// give the model's order and values on every one. The five mutants (their lines in `ord.rs`):
///
///  1. `born_of` reads one life's entry (no fold over the alive lives);
///  2. no slot survivors of a descendant (`out.extend(slot_survivors(q))` dropped);
///  3. the level floor takes the `min` of the lives' `lvl`, not the `max`;
///  4. a descendant survivor is placed at its register stamp, not at its `born_of`;
///  5. `below` is built from the registers only (a descendant with only a slot is missed).
///
/// Each changes the order of at least one of these states, so this test fails under each (shown in the
/// commit message). The fixture lists, per state, the mutants it was chosen for.
#[test]
fn the_read_equals_the_models_on_random_states() {
    let j: J = serde_json::from_str(RANDOM_STATES).unwrap();
    assert_eq!(j["model6_sha256"], MODEL6_SHA256);
    let o = JoinOptions::default();
    let (mut two_lives, mut maps, mut chosen_for) = (0usize, 0usize, BTreeSet::new());
    let states = j["states"].as_array().unwrap();
    for (k, e) in states.iter().enumerate() {
        let s = state(&e["state"]);
        let want = expected_flat(&e["expected"]);
        let doc = s.ordered_document(&o).expect("a life is alive");
        assert_eq!(flatten(&doc), want, "state {k} (seed {})", e["seed"]);
        two_lives += usize::from(s.alive(&o).len() >= 2);
        maps += want.iter().filter(|(_, v)| *v == Value::Map).count();
        for m in e["kills"].as_array().unwrap() {
            chosen_for.insert(m.as_str().unwrap().to_string());
        }
    }
    assert_eq!(chosen_for.len(), 5, "every mutant has a state: {chosen_for:?}");
    assert_eq!((states.len(), j["pool"].as_u64()), (79, Some(2094)));
    assert!(two_lives >= 20 && maps >= 100, "{two_lives} {maps}");
}

/// O1-6 of the O1 review. The clamp reads the winner's `obs` only, which is the model with ABSORBED
/// off: the Rust counter cell has no `absorbed` yet. On the model's random states with ABSORBED on, the
/// order (and the values) differ from the Rust's on a few: 23 of the 2,094 distinct states of the
/// fixture's runs; the four smallest are kept (the first is the review's seed 155 family). The Rust
/// gives the ABSORBED-off document on all of them, which this test pins.
#[test]
fn the_read_gives_the_absorbed_off_document_where_the_q6_rule_would_differ() {
    let j: J = serde_json::from_str(RANDOM_STATES).unwrap();
    let o = JoinOptions::default();
    let kept = j["absorbed"].as_array().unwrap();
    assert_eq!((kept.len(), j["absorbed_differing_states"].as_u64()), (4, Some(23)));
    for (k, e) in kept.iter().enumerate() {
        let s = state(&e["state"]);
        let got = flatten(&s.ordered_document(&o).unwrap());
        assert_eq!(got, expected_flat(&e["expected_off"]), "kept state {k}, seed {}", e["seed"]);
        assert_ne!(expected_flat(&e["expected_on"]), got, "the Q6 rule makes a difference here");
    }
}

/// The same states against the model with ABSORBED ON (the A2 design's Q6 rule): the Rust differs, as
/// it must until the counter cell lands (per-slot `absorbed`, the slot layout the manager has not frozen).
/// Pinned so that the counter-cell step must flip this test, not rediscover the divergence.
#[test]
#[ignore = "known divergence: the Rust counter cell has no `absorbed` (Q6); the counter-cell step flips this"]
fn the_read_equals_the_model_with_absorbed_on() {
    let j: J = serde_json::from_str(RANDOM_STATES).unwrap();
    let o = JoinOptions::default();
    for (k, e) in j["absorbed"].as_array().unwrap().iter().enumerate() {
        let s = state(&e["state"]);
        let got = flatten(&s.ordered_document(&o).unwrap());
        assert_eq!(got, expected_flat(&e["expected_on"]), "kept state {k}, seed {}", e["seed"]);
    }
}

/// O1-11 of the O1 review. The model keys the root of a replace by `""` and conflates it with a
/// top-level field named `""` (`parent_of(p) or ""`): there `$set "" = {...}` raises the ROOT's level
/// floor. The Rust keeps `OrdAt::Root` and `OrdAt::Field("")` apart, which is right, and no differential
/// can reach it (the model cannot name the field). Here: they are two keys with two encodings (a
/// segment count of 0, and a count of 1 with an empty segment), in this order; and a replace (the root's
/// level mark) and a `$set "" = {c}` (the level mark of the field `""`) joined in both orders leave each
/// mark at its own key. The feed writes a top-level field named `""` as `OrdAt::Field([""])`, never as
/// the root (`the_writer_feeds_a_top_level_field_named_empty_as_a_field`): fed as the root,
/// `OrdRootNotALevel` would refuse an honest document. The state below is that honest document (a replace's root mark and a top-level `""`
/// with a level mark and a child) and `decode` accepts it.
#[test]
fn the_root_and_a_top_level_field_named_empty_are_distinct_keys() {
    let o = JoinOptions::default();
    let (a, b, life) = (w(b'A'), w(b'B'), at(1, b'A'));
    let empty = Path::flat("").unwrap();
    let key_field = (life, OrdAt::Field(empty.clone()));
    let key_root = (life, OrdAt::Root);
    assert!(key_root < key_field, "the root sorts first");
    let base = || {
        let mut s = MergeState::default();
        s.lives.insert(
            life,
            Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert },
        );
        s.vv.insert(a, Hlc::new(20, 0));
        s.vv.insert(b, Hlc::new(20, 0));
        s
    };
    // writer A replaces the document: the root's level mark; writer B sets "" to a document {c: 1}
    let mut replace = base();
    let items =
        [(OrdAt::Root, OrdMode::Level), (OrdAt::Field(Path::flat("x").unwrap()), OrdMode::Reset)];
    replace.ord_apply(life, at(5, b'A'), Hlc::new(4, 0), &ord_events(&items, &o), &o);
    let mut set = base();
    let c = Path::new(["", "c"]).unwrap();
    let items = [
        (OrdAt::Field(empty.clone()), OrdMode::Write),
        (OrdAt::Field(empty.clone()), OrdMode::Level),
        (OrdAt::Field(c), OrdMode::Reset),
    ];
    set.ord_apply(life, at(7, b'B'), Hlc::new(6, 0), &ord_events(&items, &o), &o);
    assert_eq!(replace.ords[&key_root].lvl, Some(at(5, b'A')));
    assert!(!replace.ords.contains_key(&key_field));
    assert_eq!(set.ords[&key_field].lvl, Some(at(7, b'B')));
    assert!(!set.ords.contains_key(&key_root));
    // joined in both orders: each mark at its own key, and the same state
    let (ab, ba) = (join(&replace, &set, &o).0, join(&set, &replace, &o).0);
    assert_eq!(ab, ba);
    assert_eq!(ab.ords[&key_root].lvl, Some(at(5, b'A')));
    assert_eq!(ab.ords[&key_field].lvl, Some(at(7, b'B')));
    // two keys, two encodings, and the state with both round trips with both
    assert_ne!(replace.encode_compact(), set.encode_compact());
    assert_eq!(ab.ords.keys().filter(|k| k.1 == OrdAt::Root).count(), 1);
    let enc = ab.encode_compact();
    assert_eq!(
        ab.check_decoded(),
        Ok(()),
        "an honest document with a root mark and a top-level \"\""
    );
    assert_eq!(MergeState::decode_compact(&enc).as_ref(), Ok(&ab));
    // and the root is refused as soon as it holds more than a level mark: what a feed that wrote the
    // field `""` as the root would have made
    let mut as_root = ab.clone();
    as_root.ords.get_mut(&key_root).unwrap().last = Some((at(7, b'B'), 0));
    assert_eq!(as_root.check_decoded(), Err(DecodeError::OrdRootNotALevel));
    // the key bytes: life 0, then a segment count of 0 (the root) or a count of 1 and an empty segment
    let root_only = {
        let mut s = base();
        s.ords.insert(key_root.clone(), FieldOrd { lvl: Some(at(5, b'A')), ..FieldOrd::default() });
        s
    };
    let field_only = {
        let mut s = base();
        s.ords.insert(
            key_field.clone(),
            FieldOrd { last: Some((at(5, b'A'), 0)), ..FieldOrd::default() },
        );
        s
    };
    let (er, ef) = (root_only.encode_compact(), field_only.encode_compact());
    let hlc5 = Hlc::new(5, 0).to_bytes();
    let root_tail: Vec<u8> = [&[1u8, 0, 0, 0, 0, 2][..], &hlc5, &[0]].concat();
    let field_tail: Vec<u8> = [&[1u8, 0, 1, 0, 0, 0, 1][..], &hlc5, &[0, 0]].concat();
    assert!(
        er.ends_with(&root_tail),
        "count 1, life 0, segment count 0, no floor, no firsts, lvl only"
    );
    assert!(ef.ends_with(&field_tail), "count 1, life 0, one segment of length 0, then the entry");
    assert!(MergeState::decode_compact(&er).is_ok() && MergeState::decode_compact(&ef).is_ok());
    // the read: a top-level field named "" is an ordinary field, ordered by its own entry
    let mut doc = ab.clone();
    for (p, hlc, who) in [(empty.clone(), 7u64, b'B'), (Path::flat("x").unwrap(), 5, b'A')] {
        let v = if p == empty { Value::Map } else { Value::Int64(1) };
        let reg = Reg::new(at(hlc, who), v, None, false).unwrap();
        doc.regs.insert((life, p), reg);
    }
    let reg = Reg::new(at(7, b'B'), Value::Int64(2), None, false).unwrap();
    doc.regs.insert((life, Path::new(["", "c"]).unwrap()), reg);
    let names: Vec<String> =
        doc.ordered_document(&o).unwrap().iter().map(|(n, _)| n.clone()).collect();
    assert_eq!(names, ["x", ""], "the replace at 5 is before the set of \"\" at 7");
}

/// n top-level fields, each a register older than its own unset floor (the reviewer's wide shape, D-1).
fn wide_under_floors(n: usize) -> MergeState {
    let (a, life) = (w(b'A'), at(1, b'A'));
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    s.vv.insert(a, Hlc::new(20, 0));
    for i in 0..n {
        let p = Path::flat(&format!("f{i:06}")).unwrap();
        let reg = Reg::new(at(2, b'A'), Value::Int64(1), None, false).unwrap();
        s.regs.insert((life, p.clone()), reg);
        s.ords.insert(
            (life, OrdAt::Field(p)),
            FieldOrd { floor: Some(u(10, b'A', 9, false)), ..FieldOrd::default() },
        );
    }
    s
}

/// A mixed shape: n/4 maps under unset floors, each with three children (two registers, one counter
/// slot) older than the floor, in two alive lives.
fn maps_under_floors(n: usize) -> MergeState {
    let a = w(b'A');
    let lives = [at(1, b'A'), at(2, b'A')];
    let mut s = MergeState::default();
    s.vv.insert(a, Hlc::new(20, 0));
    for life in lives {
        s.lives.insert(
            life,
            Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Upsert },
        );
    }
    for i in 0..n / 4 {
        let life = lives[i % 2];
        let m = Path::flat(&format!("m{i:06}")).unwrap();
        let reg = |v| Reg::new(at(3, b'A'), v, None, false).unwrap();
        s.regs.insert((life, m.clone()), reg(Value::Map));
        for (j, kid) in ["x", "y", "z"].iter().enumerate() {
            let q = m.child(kid).unwrap();
            if j < 2 {
                s.regs.insert((life, q), reg(Value::Int64(1)));
            } else {
                s.slots.insert(
                    (life, q, a, None),
                    Slot { stamp: at(3, b'A'), total: Num::Int64(5), absorbed: None },
                );
            }
        }
        s.ords.insert(
            (life, OrdAt::Field(m)),
            FieldOrd { floor: Some(u(10, b'A', 9, false)), ..FieldOrd::default() },
        );
    }
    s
}

/// D-1 of the delta review: the memoised read was linear in evaluations but quadratic in the cells it
/// visited, because each evaluation under an unset floor scanned every register and slot key for the
/// descendants of its path (n fields under floors visited n^2 keys; 100,000 of them, a few megabytes
/// that decode, about 10^10 per read). The descendants are now a contiguous run of the path-ordered
/// maps, so the read costs the descendants it uses. Counted in cell visits (`work`), not wall time:
///
///  * n fields under floors: visits at n = 1,000 / 2,000 / 4,000 grow with n (the ratio of the last to
///    the first is at most 4.4, a 10% tolerance over 4), and stay under 12 n;
///  * a mixed shape (maps with children, in two lives): the same;
///  * a chain of survivors of depth d: every path above has its own floor and looks at all below it, so
///    the visits are the sum of the descendants (d(d - 1)/2 pairs, each costing a winner probe, a slot
///    probe and a step of the key runs: about 4), bounded by 5 d^2, and d is at most 100 (the depth
///    limit of paths): under 50,000 at the limit. (Linear in d would need the survivors of a
///    path to be folded from its children's, which the clamp's `place` does not allow: it depends on
///    the floor of the path itself.)
#[test]
fn the_read_visits_cells_in_proportion_to_the_work_not_to_the_state() {
    let o = JoinOptions::default();
    let visits = |s: &MergeState| {
        let (doc, c) = super::work::count(|| s.ordered_document(&o));
        assert!(doc.is_some());
        c.visits
    };
    for (name, build) in [
        ("wide", wide_under_floors as fn(usize) -> MergeState),
        ("maps", maps_under_floors as fn(usize) -> MergeState),
    ] {
        let (v1, v2, v4) = (visits(&build(1_000)), visits(&build(2_000)), visits(&build(4_000)));
        eprintln!("{name}: visits {v1} / {v2} / {v4} at 1,000 / 2,000 / 4,000 fields");
        assert!(
            v4 * 10 <= v1 * 44,
            "{name}: {v1} {v4}: more than 4.4 times for 4 times the fields"
        );
        assert!(v2 * 10 <= v1 * 22, "{name}: {v1} {v2}");
        assert!(v4 <= 12 * 4_000, "{name}: {v4} visits for 4,000 fields");
    }
    for d in [10usize, 40, 100] {
        let v = visits(&survivor_chain(d));
        eprintln!("chain of depth {d}: {v} visits");
        assert!(v <= 5 * d * d, "chain {d}: {v}");
    }
}

/// A document nested `m` levels: the field "f" holding the field "f" ... holding an integer; the deepest
/// path has `m` segments.
fn nested_ordered(m: usize) -> Vec<(String, OrderedValue)> {
    let mut v = OrderedValue::Leaf(Value::Int64(1));
    for _ in 1..m {
        v = OrderedValue::Doc(vec![("f".to_string(), v)]);
    }
    vec![("f".to_string(), v)]
}

/// `OrderedValue::paths` is public and takes a document a caller built: it never panics. A document
/// whose deepest path has 100 segments is accepted (a path for each of its levels), one with 101 gives
/// the `InvalidPath` of `Path::new`, and so does a name with a NUL byte.
#[test]
fn the_paths_of_a_document_are_fallible_and_bounded() {
    let ok = OrderedValue::paths(&nested_ordered(100)).unwrap();
    assert_eq!(ok.len(), 100);
    assert_eq!(ok[99].segments().len(), 100);
    assert!(OrderedValue::paths(&nested_ordered(101)).is_err());
    assert!(
        OrderedValue::paths(&nested_ordered(5_000)).is_err(),
        "refused at level 101, not walked"
    );
    let nul = vec![("a\0b".to_string(), OrderedValue::Leaf(Value::Null))];
    assert!(OrderedValue::paths(&nul).is_err());
    assert_eq!(OrderedValue::paths(&[]).unwrap(), Vec::<Path>::new());
}

/// The derived `Clone`, `PartialEq`, `Debug` and `Drop` of a 100-deep `OrderedValue` (the deepest a state's
/// document can be) fit a thread of 256 KiB: 100 frames of a few hundred bytes each.
#[test]
fn the_derived_traits_of_a_hundred_deep_document_fit_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let doc = nested_ordered(100);
            let copy = doc.clone();
            assert_eq!(doc, copy);
            assert!(format!("{doc:?}").len() > 100);
            drop(copy);
            drop(doc);
        })
        .unwrap()
        .join()
        .unwrap();
}

// ---- the laws on the entries of the model's own runs ----------------------------------

/// `probe_ord_join_laws` of the model: the distinct order entries of the writer entries of 150 nested
/// runs, per path (states at different times, so a path has several). Commutative and idempotent on
/// all pairs, associative on all triples of distinct entries of a path; the counts are pinned and
/// the test fails if the fixture is vacuous.
#[test]
fn the_join_laws_hold_on_the_order_entries_of_the_models_runs() {
    let j: J = serde_json::from_str(JOIN_LAWS).unwrap();
    assert_eq!(j["model6_sha256"], MODEL6_SHA256);
    let pool: Vec<FieldOrd> = j["ords"].as_array().unwrap().iter().map(field_ord).collect();
    // every entry of the model's runs satisfies the reachability clauses decode checks, at the key
    // kind it was under (the root of a replace, or a path)
    for (x, raw) in pool.iter().zip(j["ords"].as_array().unwrap()) {
        let at =
            if raw["root"] == true { OrdAt::Root } else { OrdAt::Field(Path::flat("f").unwrap()) };
        assert_eq!(x.shape_error(&at), None, "{x:?}");
    }
    let o = JoinOptions::default();
    let (mut keys, mut several, mut pairs, mut triples, mut mixed) = (0, 0, 0, 0, 0);
    let (mut u_bit, mut u_clear, mut r_floor, mut no_floor, mut with_lvl, mut with_prev) =
        (0, 0, 0, 0, 0, 0);
    let mut writers_per_first = BTreeMap::<usize, usize>::new();
    for o_ in &pool {
        assert!(o_.is_normal(&o));
        match o_.floor {
            Some(Floor::U { others: true, .. }) => u_bit += 1,
            Some(Floor::U { .. }) => u_clear += 1,
            Some(Floor::R { .. }) => r_floor += 1,
            None => no_floor += 1,
        }
        with_lvl += usize::from(o_.lvl.is_some());
        with_prev +=
            usize::from(matches!(o_.floor, Some(Floor::U { prev, .. }) if prev > Hlc::ZERO));
        *writers_per_first.entry(o_.firsts.len()).or_default() += 1;
    }
    for idx in j["per_path"].as_array().unwrap() {
        let es: Vec<&FieldOrd> =
            idx.as_array().unwrap().iter().map(|i| &pool[i.as_u64().unwrap() as usize]).collect();
        keys += 1;
        several += usize::from(es.len() > 1);
        for a in &es {
            let mut norm = (*a).clone();
            norm.normalise(&o);
            assert_eq!(ord_join(a, a, &o), norm, "idempotent");
            for b in &es {
                pairs += 1;
                assert_eq!(ord_join(a, b, &o), ord_join(b, a, &o), "commutative");
            }
        }
        if es.len() >= 3 {
            for (i, a) in es.iter().enumerate() {
                for (k, b) in es.iter().enumerate() {
                    for (m, c) in es.iter().enumerate() {
                        if i == k || k == m || i == m {
                            continue;
                        }
                        triples += 1;
                        let bits: BTreeSet<Option<bool>> = [a, b, c]
                            .iter()
                            .map(|x| match x.floor {
                                Some(Floor::U { others, .. }) => Some(others),
                                _ => None,
                            })
                            .collect();
                        mixed +=
                            usize::from(bits.contains(&Some(true)) && bits.contains(&Some(false)));
                        assert_eq!(
                            ord_join(&ord_join(a, b, &o), c, &o),
                            ord_join(a, &ord_join(b, c, &o), &o),
                            "associative"
                        );
                    }
                }
            }
        }
    }
    let distinct = pool.len();
    eprintln!(
        "order entries: {keys} paths, {several} with several, {distinct} distinct, {pairs} pairs, {triples} triples ({mixed} mixing the bit); floors u+bit {u_bit}, u {u_clear}, r {r_floor}, none {no_floor}; lvl {with_lvl}; prev {with_prev}; firsts per entry {writers_per_first:?}"
    );
    assert_eq!(j["triples"], triples);
    assert_eq!(j["paths_with_several"], several);
    assert_eq!((several, triples), (779, 2178));
    assert_eq!(distinct, 457);
    assert_eq!(keys, 1864);
    assert_eq!(pairs, 5511);
    assert!(
        u_bit > 20 && u_clear > 20 && r_floor > 20 && no_floor > 20 && with_lvl > 20,
        "vacuous"
    );
    assert!(mixed > 20 && writers_per_first.keys().max() >= Some(&2), "vacuous");
}

// ---- a causal simulation of the order state alone ---------------------------------------

/// A simulation of the order state in states of one life: three writers write to a few paths
/// (leaf writes, unsets, whole-sub-document sets, replaces, updates that unset and write other
/// fields), receive each other's whole states at random and take a snapshot after every step, as
/// the model's oplog entries are. The states are reachable (every order entry is the work of the
/// feed on the writer's own state and of joins), which is what the laws are claimed for.
pub(super) fn ord_sim(seed: u64, steps: usize, o: &JoinOptions) -> Vec<MergeState> {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let ids = [w(b'A'), w(b'B'), w(b'C')];
    let life = MStamp { hlc: Hlc::new(1, 0), writer: ids[0] };
    let mut base = MergeState::default();
    base.lives
        .insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    base.vv.insert(ids[0], life.hlc);
    let mut ws: Vec<MergeState> = vec![base; 3];
    let fields: Vec<Path> =
        ["a", "b", "c", "d", "m"].iter().map(|n| Path::flat(n).unwrap()).collect();
    let kids: Vec<Path> =
        ["m.x", "m.y", "m.z"].iter().map(|n| Path::new(n.split('.')).unwrap()).collect();
    let m = Path::flat("m").unwrap();
    let mut pool = Vec::new();
    for _ in 0..steps {
        let k = rng.below(3);
        if rng.below(100) < 40 {
            let mut other = rng.below(3);
            if other == k {
                other = (k + 1) % 3;
            }
            let from = ws[other].clone();
            ws[k] = join(&ws[k], &from, o).0;
        } else {
            let known = ws[k].vv.values().copied().max().unwrap();
            let skew = *rng.pick(&[0u64, 0, 0, 3]);
            let at = MStamp { hlc: Hlc::new(known.wall_ms + 1 + skew, 0), writer: ids[k] };
            ws[k].vv.insert(ids[k], at.hlc);
            let f = |i: usize| OrdAt::Field(fields[i % fields.len()].clone());
            let kid = |i: usize| OrdAt::Field(kids[i % kids.len()].clone());
            let (a, b, c) = (rng.below(5), rng.below(5), rng.below(3));
            let items: Vec<(OrdAt, OrdMode)> = match rng.below(9) {
                0 | 1 => vec![(f(a), OrdMode::Write)],
                2 => vec![(f(a), OrdMode::Unset)],
                3 => vec![(kid(c), OrdMode::Write), (OrdAt::Field(m.clone()), OrdMode::Write)],
                4 => vec![
                    (OrdAt::Field(m.clone()), OrdMode::Write),
                    (OrdAt::Field(m.clone()), OrdMode::Level),
                    (kid(c), OrdMode::Reset),
                    (kid(c + 1), OrdMode::Reset),
                ],
                5 => vec![
                    (OrdAt::Root, OrdMode::Level),
                    (f(a), OrdMode::Reset),
                    (f(b), OrdMode::Reset),
                    (f(a + 2), OrdMode::Reset),
                ],
                6 => vec![
                    (f(a), OrdMode::Write),
                    (f(b + 1), OrdMode::Write),
                    (f(a + 3), OrdMode::Unset),
                ],
                7 => vec![(kid(c), OrdMode::Unset)],
                _ => vec![(f(a), OrdMode::Write), (f(b), OrdMode::Write)],
            };
            let events = ord_events(&items, o);
            ws[k].ord_apply(life, at, known, &events, o);
        }
        pool.push(ws[k].clone());
    }
    pool
}

/// The laws of the order state on states the simulation reaches: the property tests' law check
/// (commutative, associative where the model claims it, idempotent, absorbing, hashes independent
/// of the order) over states that carry order entries, and the entry-level laws over the distinct
/// entries of every path with the counts pinned.
#[test]
fn the_order_state_joins_by_the_laws_in_a_causal_simulation() {
    let o = opts(1_000_000_000);
    let pools: Vec<Vec<MergeState>> = (1..=60).map(|s| ord_sim(s, 40, &o)).collect();
    check_laws_on(&o, true, &pools, 3000);
    // the entries
    let (mut keys, mut several, mut pairs, mut triples) = (0usize, 0usize, 0usize, 0usize);
    let mut shapes = BTreeMap::<&str, usize>::new();
    for pool in &pools {
        let mut by_key: BTreeMap<(MStamp, OrdAt), Vec<FieldOrd>> = BTreeMap::new();
        for s in pool {
            for (k, x) in &s.ords {
                let e = by_key.entry(k.clone()).or_default();
                if !e.contains(x) {
                    e.push(x.clone());
                }
            }
        }
        for es in by_key.values() {
            keys += 1;
            several += usize::from(es.len() > 1);
            for x in es {
                assert!(x.is_normal(&o));
                *shapes
                    .entry(match x.floor {
                        Some(Floor::U { others: true, .. }) => "u+bit",
                        Some(Floor::U { .. }) => "u",
                        Some(Floor::R { .. }) => "r",
                        None => "none",
                    })
                    .or_default() += 1;
                *shapes
                    .entry(["firsts 0", "firsts 1", "firsts 2", "firsts 3"][x.firsts.len().min(3)])
                    .or_default() += 1;
                *shapes.entry(if x.lvl.is_some() { "lvl" } else { "no lvl" }).or_default() += 1;
                *shapes.entry(if x.last.is_some() { "last" } else { "no last" }).or_default() += 1;
            }
            for a in es {
                for b in es {
                    pairs += 1;
                    assert_eq!(ord_join(a, b, &o), ord_join(b, a, &o));
                }
                assert_eq!(ord_join(a, a, &o), *a);
            }
            for (i, a) in es.iter().enumerate() {
                for (k, b) in es.iter().enumerate() {
                    for (n, c) in es.iter().enumerate() {
                        if i != k && k != n && i != n {
                            triples += 1;
                            assert_eq!(
                                ord_join(&ord_join(a, b, &o), c, &o),
                                ord_join(a, &ord_join(b, c, &o), &o),
                                "associative"
                            );
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "simulation: {keys} entries-by-path, {several} with several, {pairs} pairs, {triples} triples; {shapes:?}"
    );
    assert_eq!((keys, several, pairs, triples), (527, 494, 23529, 175302));
    for shape in ["u+bit", "u", "r", "none", "firsts 1", "firsts 2", "firsts 3", "lvl", "last"] {
        assert!(shapes.get(shape).copied().unwrap_or(0) > 50, "vacuous: {shape} {shapes:?}");
    }
}

/// Item 9 of O2 (bounds): a document nested at the limit (100 levels), written through the Writer,
/// feeds a chain of 100 order entries (one per level, each a plain write of the Writer's), and the
/// read of the state a concurrent unset of the top and a survivor increment of the leaf make visits at
/// most 5·d² cells, the bound of D-1. The Writer refuses a path one level deeper.
#[test]
fn a_document_nested_at_the_limit_feeds_a_chain_the_read_visits_within_the_bound() {
    use super::tests::wid;
    let o = JoinOptions::default();
    let d = 100usize;
    fn nest(d: usize, leaf: Value) -> Value {
        if d == 0 { leaf } else { Value::Document(vec![("a".into(), nest(d - 1, leaf))]) }
    }
    let top = Path::flat("a").unwrap();
    let leaf_path = Path::new(std::iter::repeat_n("a".to_string(), d)).unwrap();
    // d levels: the leaf is at a path of d segments
    let doc = vec![(top.clone(), nest(d - 1, Value::Int64(1)))];
    let mut a = Writer::new(wid('A', 0), o.clone());
    let life = a.insert(&doc, 0).expect("a document nested at the limit is written").stamp;
    let keys: Vec<_> = a.state.ords.keys().filter(|k| k.0 == life).collect();
    assert_eq!(keys.len(), d, "one entry per level");
    assert_eq!(a.state.check_decoded(), Ok(()));
    // B sees it and increments the leaf; A, not having seen that, unsets the top
    let mut b = Writer::new(wid('B', 0), o.clone());
    b.receive(&a.state);
    b.write_inc(&leaf_path, 1, 0).expect("the increment");
    a.write_unset(&top, 0).expect("the unset");
    let joined = join(&a.state, &b.state, &o).0;
    assert_eq!(joined.check_decoded(), Ok(()));
    let (read, c) = super::work::count(|| joined.ordered_document(&o));
    let read = read.expect("a life is alive");
    assert!(c.visits <= 5 * d * d, "{} visits for depth {d}: the bound is {}", c.visits, 5 * d * d);
    assert!(!read.is_empty(), "the surviving increment shows a chain of maps");
    eprintln!(
        "depth {d}: {} order entries, {} visits (bound {})",
        joined.ords.len(),
        c.visits,
        5 * d * d
    );
    // and the encodings of the chain round trip
    let enc = joined.encode_compact();
    assert_eq!(MergeState::decode_compact(&enc).as_ref(), Ok(&joined));
    // one level deeper is refused by the Writer: a value nested past the limit, and a path
    assert!(Path::new(std::iter::repeat_n("a".to_string(), d + 1)).is_err());
    let mut fresh = Writer::new(wid('C', 0), o.clone());
    assert!(
        fresh.insert(&[(top, nest(d, Value::Int64(1)))], 0).is_none(),
        "101 levels are refused"
    );
    assert!(fresh.state.ords.is_empty(), "and nothing is fed");
}

// ---- the O2 review: nested upserts, the clock ahead of the vector --------------------------

/// O2-2 of the O2 review. A leaf write is also a write to each map above it; the upserts name the ancestors of their paths
/// as the other writes do. An upsert of `a.b` and then a `$set` of a new field `c`: the map `a` has a place before `c`, so
/// the document is `[a, a.b, c]` in every delivery order of the writer's entries (the model gives `[c, a, a.b]` here: it
/// names no ancestor in an upsert; `q7-logs/o2-fix/nested_upsert_model_gap.py` shows it).
#[test]
fn an_upsert_of_a_nested_path_names_its_ancestors_so_a_later_set_goes_after_them() {
    let o = JoinOptions::default();
    let ab = Path::new(["a", "b"]).unwrap();
    let c = Path::flat("c").unwrap();
    let s = Path::flat("s").unwrap();
    let n = Path::flat("n").unwrap();
    let names = [
        "upsert_inc",
        "upsert_soi, no field",
        "upsert_soi with a field",
        "upsert_soi, a nested $setOnInsert field",
        "upsert_soi, a nested $set field",
    ];
    let want: [&[&str]; 5] = [
        &["a", "a.b", "c"],
        &["a", "a.b", "c"],
        &["s", "a", "a.b", "c"],
        &["a", "a.b", "n", "c"],
        &["a", "a.b", "n", "c"],
    ];
    for (kind, name) in names.iter().enumerate() {
        let mut w = Writer::new(super::tests::wid('A', 0), o.clone());
        let mut entries = Vec::new();
        match kind {
            0 => {
                w.upsert_inc(&ab, 1, 0).unwrap();
                entries.push(w.state.clone());
            }
            1 => {
                w.upsert_soi(&[], &ab, 1, &[], 0).unwrap();
                entries.push(w.state.clone());
            }
            2 => {
                w.upsert_soi(&[(s.clone(), Value::Int64(7))], &ab, 1, &[], 0).unwrap();
                entries.push(w.state.clone());
            }
            3 => {
                w.upsert_soi(&[(ab.clone(), Value::Int64(7))], &n, 1, &[], 0).unwrap();
                entries.push(w.state.clone());
            }
            _ => {
                w.upsert_soi(&[], &n, 1, &[(ab.clone(), Value::Int64(7))], 0).unwrap();
                entries.push(w.state.clone());
            }
        }
        w.write_set(&c, Value::Int64(2), 0).unwrap();
        entries.push(w.state.clone());
        for ord in orders(entries.len()) {
            let mut st = MergeState::default();
            for &i in &ord {
                st = join(&st, &entries[i], &o).0;
            }
            let doc = st.ordered_document(&o).unwrap();
            assert_eq!(paths(&flatten(&doc)), strs(want[kind]), "{name}: delivery order {ord:?}");
        }
        let last = entries.last().unwrap();
        assert_eq!(
            paths(&flatten(&last.ordered_document(&o).unwrap())),
            strs(want[kind]),
            "{name}"
        );
    }
}

/// O2-4 of the O2 review: `known()` is the vector's maximum, not `top()` (which also counts the clock). A writer that ticks
/// without writing (the sim's loss-entry `tick(0)`) has a clock ahead of its vector; an unset it makes then remembers the
/// vector's maximum as `prev`. B incremented `a` without A having seen it; C wrote `d` at stamp 3. The increment survives A's
/// unset and is placed by `prev`: after the highest stamp A had seen (its insert, 1) it keeps its own stamp (2), before `d`
/// (3). With the clock in `prev` it would be placed after the clock and so after `d`.
#[test]
fn an_unset_after_a_tick_without_a_write_remembers_the_vectors_maximum_as_prev() {
    use super::tests::wid;
    let o = JoinOptions::default();
    let (x, a_, d) = (Path::flat("x").unwrap(), Path::flat("a").unwrap(), Path::flat("d").unwrap());
    let mut wa = Writer::new(wid('A', 0), o.clone());
    wa.insert(&[(x, Value::Int64(1)), (a_.clone(), Value::Int64(1))], 0).unwrap();
    let seed = wa.state.clone();
    let (mut wb, mut wc) =
        (Writer::new(wid('B', 0), o.clone()), Writer::new(wid('C', 0), o.clone()));
    wb.receive(&seed);
    wc.receive(&seed);
    wb.write_inc(&a_, 1, 0).unwrap(); // stamp 2
    wc.clock = Hlc::new(2, 0);
    wc.write_set(&d, Value::Int64(1), 0).unwrap(); // stamp 3
    // A mints a stamp and writes nothing: the clock runs ahead of the vector
    let _ = wa.tick(9);
    assert!(wa.clock > wa.state.vv.values().copied().max().unwrap());
    wa.write_unset(&a_, 0).unwrap();
    match wa.state.ords[&(wa.alive()[0], OrdAt::Field(a_.clone()))].floor {
        Some(Floor::U { prev, .. }) => {
            assert_eq!(
                prev,
                seed.vv.values().copied().max().unwrap(),
                "prev is the vector's maximum before the write: the insert"
            )
        }
        f => panic!("{f:?}"),
    }
    let mut st = join(&wa.state, &wb.state, &o).0;
    st = join(&st, &wc.state, &o).0;
    let doc = st.ordered_document(&o).unwrap();
    assert_eq!(paths(&flatten(&doc)), strs(&["x", "a", "d"]));
}
