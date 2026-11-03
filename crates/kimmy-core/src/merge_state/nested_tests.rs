//! Tests of the nested value semantics: cells keyed by (life, segment path), the recursive type
//! rule, the explicit map marker, the recursive leaf-effect diff, the refusal of a write below a
//! scalar or an array, atomic arrays, and `typeClash` / `arrayClash`.
//!
//! The scenarios are ports of the Python model's nested probes (`probes6.py` of the stage A
//! model: `probe_Q3_*`), with the same inputs and the same expected documents; each says which.
//! One difference is on purpose and named where it shows: the model's implicit map marker (a
//! leaf write that creates the maps above it also writes a marker there, decision D-1) is not
//! built here, so a probe whose expected records include the marker's `clash` expects one record
//! less. Each rule has a mutation switch of [`JoinOptions`]; `mutants_are_caught` turns each on
//! against the scenario that pins it.

use std::collections::{BTreeMap, BTreeSet};

use super::tests::{Ent, Mem, all_orders, fl, i, opts, pull_all, s};
use super::*;

/// A dotted spelling of a path, for the test names that have no dot.
fn np(dotted: &str) -> Path {
    Path::new(dotted.split('.')).unwrap()
}

/// A sub-document.
fn sub(f: &[(&str, Value)]) -> Value {
    Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
}

const M: Value = Value::Map;

/// The visible document, as the writer reports it, from dotted paths (`M` marks a map).
fn want(items: &[(&str, Value)]) -> Option<BTreeMap<Path, Value>> {
    Some(items.iter().map(|(k, v)| (np(k), v.clone())).collect())
}

type Op<'a> = &'a dyn Fn(&mut Mem, u64) -> Option<MStamp>;

fn set<'a>(path: &'a str, v: Value) -> impl Fn(&mut Mem, u64) -> Option<MStamp> + 'a {
    move |m, sk| m.set_at(&np(path), v.clone(), sk)
}
fn unset(path: &str) -> impl Fn(&mut Mem, u64) -> Option<MStamp> + '_ {
    move |m, sk| m.unset_at(&np(path), sk)
}
fn inc(path: &str, n: i64) -> impl Fn(&mut Mem, u64) -> Option<MStamp> + '_ {
    move |m, sk| m.inc_at(&np(path), n, sk)
}

/// The model's `_two`/`nested_case`: `I` creates the document, `A` and `B` (both holding it) do
/// one write each, with the given clock skews, concurrently. Every delivery order of the two
/// entries into a fresh observer must read `expect`; after the observers pull each other's
/// entries (loss entries included) they must all hold exactly `records` (a kind, a dotted path,
/// and whose stamp: `'a'` or `'b'`).
fn two(
    o: &JoinOptions,
    init: &[(&str, Value)],
    a: Op,
    b: Op,
    sk: (u64, u64),
    expect: &[(&str, Value)],
    records: &[(LossKind, &str, char)],
) -> Result<(), String> {
    let (mut mi, mut ma, mut mb) = (Mem::new('I', o), Mem::new('A', o), Mem::new('B', o));
    let i0 = mi.ins(init, 0).unwrap();
    ma.receive_entry(&mi.ent(i0));
    mb.receive_entry(&mi.ent(i0));
    let sa = a(&mut ma, sk.0).ok_or("A's write was refused")?;
    let sb = b(&mut mb, sk.1).ok_or("B's write was refused")?;
    let ents = [ma.ent(sa), mb.ent(sb)];
    let mut obs = Vec::new();
    for (ord, m) in all_orders(o, &ents, &[mi.ent(i0)]) {
        if m.visible() != want(expect) {
            return Err(format!("delivery {ord:?}: read {:?}, want {expect:?}", m.visible()));
        }
        obs.push(m);
    }
    pull_all(&mut obs);
    pull_all(&mut obs);
    let sets: BTreeSet<_> = obs.iter().map(Mem::loss_set).collect();
    let wanted: BTreeSet<_> = records
        .iter()
        .map(|(k, path, who)| (*k, LossAt::Field(np(path)), if *who == 'a' { sa } else { sb }))
        .collect();
    if sets.len() != 1 || sets.iter().next() != Some(&wanted) {
        return Err(format!("records {sets:?}, want {wanted:?}"));
    }
    Ok(())
}

fn ok(r: Result<(), String>) {
    if let Err(e) = r {
        panic!("{e}");
    }
}

// ---- ports of the model's nested probes --------------------------------------------------

use LossKind::{ArrayClash, Clash, TypeClash};

/// probes6.py `probe_Q3_marker_keeps_concurrent_child`.
fn q3_marker_keeps_concurrent_child(o: &JoinOptions) -> Result<(), String> {
    for sk in [(0, 9), (9, 0)] {
        two(
            o,
            &[("x", i(1)), ("a", sub(&[("b", i(1))]))],
            &set("a", sub(&[])),
            &set("a.c", i(3)),
            sk,
            &[("a", M), ("a.c", i(3)), ("x", i(1))],
            &[],
        )
        .map_err(|e| format!("{sk:?}: {e}"))?;
    }
    Ok(())
}

/// probes6.py `probe_Q3_scalar_over_map_type_rule`: the newer side wins, the side it hides is the
/// typeClash. In the model's "child newer" case the child write also made `a`'s implicit marker,
/// which beats the older scalar and is reported as a `clash`; without that marker only the
/// `typeClash` of `a` remains.
fn q3_scalar_over_map_type_rule(o: &JoinOptions) -> Result<(), String> {
    two(
        o,
        &[("x", i(1))],
        &set("a", i(5)),
        &set("a.b", i(1)),
        (9, 0),
        &[("a", i(5)), ("x", i(1))],
        &[(TypeClash, "a.b", 'b')],
    )
    .map_err(|e| format!("scalar newer: {e}"))?;
    two(
        o,
        &[("x", i(1))],
        &set("a", i(5)),
        &set("a.b", i(1)),
        (0, 9),
        &[("a", M), ("a.b", i(1)), ("x", i(1))],
        &[(TypeClash, "a", 'a')],
    )
    .map_err(|e| format!("child newer: {e}"))?;
    // a counter child hidden by a newer scalar, and a scalar over a map the writer saw
    let acct = [("x", i(1)), ("acct", sub(&[("balance", i(100))]))];
    two(
        o,
        &acct,
        &set("acct", i(5)),
        &inc("acct.balance", 20),
        (9, 0),
        &[("acct", i(5)), ("x", i(1))],
        &[(TypeClash, "acct.balance", 'b')],
    )
    .map_err(|e| format!("scalar over counter, newer: {e}"))?;
    two(
        o,
        &acct,
        &set("acct", i(5)),
        &inc("acct.balance", 20),
        (0, 9),
        &[("acct", M), ("acct.balance", i(20)), ("x", i(1))],
        &[(TypeClash, "acct", 'a')],
    )
    .map_err(|e| format!("scalar over counter, older: {e}"))
}

/// probes6.py `probe_Q3_partial_subdocument`: an unset of a sub-document keeps a newer concurrent
/// child (and loses to an older one).
fn q3_partial_subdocument(o: &JoinOptions) -> Result<(), String> {
    let init = [("x", i(1)), ("a", sub(&[("b", i(0)), ("c", i(2))]))];
    two(
        o,
        &init,
        &unset("a"),
        &set("a.b", i(1)),
        (0, 9),
        &[("a", M), ("a.b", i(1)), ("x", i(1))],
        &[(Clash, "a.b", 'a')],
    )
    .map_err(|e| format!("partial subdocument: {e}"))?;
    two(o, &init, &unset("a"), &set("a.b", i(1)), (9, 0), &[("x", i(1))], &[(Clash, "a.b", 'b')])
        .map_err(|e| format!("unset newer: {e}"))
}

/// probes6.py `probe_Q3_array_clash`: concurrent array writes do not merge element-wise; the
/// newer wins and the other is an arrayClash.
fn q3_array_clash(o: &JoinOptions) -> Result<(), String> {
    let arr = |xs: &[i64]| Value::Array(xs.iter().map(|x| i(*x)).collect());
    two(
        o,
        &[("x", i(1)), ("items", arr(&[1]))],
        &set("items", arr(&[1, 2])),
        &set("items", arr(&[1, 3])),
        (0, 9),
        &[("items", arr(&[1, 3])), ("x", i(1))],
        &[(ArrayClash, "items", 'a')],
    )
    .map_err(|e| format!("push/push, B newer: {e}"))?;
    two(
        o,
        &[("x", i(1)), ("items", arr(&[1]))],
        &set("items", arr(&[1, 2])),
        &set("items", arr(&[6])),
        (9, 0),
        &[("items", arr(&[1, 2])), ("x", i(1))],
        &[(ArrayClash, "items", 'b')],
    )
    .map_err(|e| format!("push/rewrite, A newer: {e}"))
}

/// probes6.py `probe_Q3_unset_subdoc_keeps_unseen_increment`.
fn q3_unset_subdoc_keeps_unseen_increment(o: &JoinOptions) -> Result<(), String> {
    for sk in [(0, 9), (9, 0)] {
        two(
            o,
            &[("x", i(1)), ("acct", sub(&[("balance", i(100))]))],
            &unset("acct"),
            &inc("acct.balance", 20),
            sk,
            &[("acct", M), ("acct.balance", i(20)), ("x", i(1))],
            &[],
        )
        .map_err(|e| format!("{sk:?}: {e}"))?;
    }
    Ok(())
}

/// probes6.py `probe_Q3_set_subdoc_unsets_seen_child_only`: `$set a:{b:1}` over `a:{b:0,c:2}`
/// sets `a.b` and unsets the `a.c` it saw; a concurrent newer `$set a.c` survives.
fn q3_set_subdoc_unsets_seen_child_only(o: &JoinOptions) -> Result<(), String> {
    two(
        o,
        &[("x", i(1)), ("a", sub(&[("b", i(0)), ("c", i(2))]))],
        &set("a", sub(&[("b", i(1))])),
        &set("a.c", i(9)),
        (0, 9),
        &[("a", M), ("a.b", i(1)), ("a.c", i(9)), ("x", i(1))],
        &[(Clash, "a.c", 'a')],
    )
    .map_err(|e| format!("set subdoc, child newer: {e}"))
}

/// probes6.py `probe_Q3_empty_map_reads_empty`.
fn q3_empty_map_reads_empty(o: &JoinOptions) -> Result<(), String> {
    two(
        o,
        &[("x", i(1)), ("a", sub(&[("b", i(1))]))],
        &set("a", sub(&[])),
        &set("x", i(2)),
        (0, 0),
        &[("a", M), ("x", i(2))],
        &[],
    )
}

/// probes6.py `probe_Q3_replace_keeps_nested_increment`: C adds 50 (A sees it); A replaces the
/// document changing only `x`, so no leaf is written for `acct.balance` (150 is unchanged); B,
/// who never saw C's 50, adds 20: 170 in both clock orders.
fn q3_replace_keeps_nested_increment(o: &JoinOptions) -> Result<(), String> {
    for sk in [(0, 9), (9, 0)] {
        let (mut mi, mut ma, mut mb, mut mc) =
            (Mem::new('I', o), Mem::new('A', o), Mem::new('B', o), Mem::new('C', o));
        let i0 = mi.ins(&[("x", i(1)), ("acct", sub(&[("balance", i(100))]))], 0).unwrap();
        for m in [&mut ma, &mut mb, &mut mc] {
            m.receive_entry(&mi.ent(i0));
        }
        let c = mc.inc_at(&np("acct.balance"), 50, 0).unwrap();
        ma.receive_entry(&mc.ent(c));
        let a = ma
            .replace(&[("x", i(2)), ("acct", sub(&[("balance", i(150))]))], sk.0)
            .ok_or("replace refused")?;
        let b = mb.inc_at(&np("acct.balance"), 20, sk.1).unwrap();
        let wrote = ma.w.state.regs.values().filter(|r| r.stamp == a).count();
        if wrote != 1 {
            return Err(format!("the replace wrote {wrote} cells, want only x"));
        }
        let ents: Vec<Ent> = vec![mc.ent(c), ma.ent(a), mb.ent(b)];
        for (ord, m) in all_orders(o, &ents, &[mi.ent(i0)]) {
            let w = want(&[("acct", M), ("acct.balance", i(170)), ("x", i(2))]);
            if m.visible() != w {
                return Err(format!("{sk:?} {ord:?}: read {:?}", m.visible()));
            }
        }
    }
    Ok(())
}

/// A scenario: it holds, or says how it did not.
type Scenario = fn(&JoinOptions) -> Result<(), String>;

const PROBES: [(&str, Scenario); 8] = [
    ("marker_keeps_concurrent_child", q3_marker_keeps_concurrent_child),
    ("empty_map_reads_empty", q3_empty_map_reads_empty),
    ("replace_keeps_nested_increment", q3_replace_keeps_nested_increment),
    ("scalar_over_map_type_rule", q3_scalar_over_map_type_rule),
    ("partial_subdocument", q3_partial_subdocument),
    ("array_clash", q3_array_clash),
    ("unset_subdoc_keeps_unseen_increment", q3_unset_subdoc_keeps_unseen_increment),
    ("set_subdoc_unsets_seen_child_only", q3_set_subdoc_unsets_seen_child_only),
];

#[test]
fn the_model_nested_probes_hold() {
    for (name, probe) in PROBES {
        if let Err(e) = probe(&opts()) {
            panic!("probe_Q3_{name}: {e}");
        }
    }
}

// ---- the type rule: recency per child, typeClash at any distance ------------------------

/// D-13: a stale scalar hides only the OLDER children. B sets `a.b` (stamp 2), A sets `a = 5`
/// (stamp 6), C sets `a.c` (stamp 11), all concurrent: `a` is a map showing only `c`, and the
/// two typeClash records name the side the type rule hid in each pair.
fn stale_scalar_hides_only_older_children(o: &JoinOptions) -> Result<(), String> {
    let (mut mi, mut ma, mut mb, mut mc) =
        (Mem::new('I', o), Mem::new('A', o), Mem::new('B', o), Mem::new('C', o));
    let i0 = mi.ins(&[("x", i(1))], 0).unwrap();
    for m in [&mut ma, &mut mb, &mut mc] {
        m.receive_entry(&mi.ent(i0));
    }
    let sb = mb.set_at(&np("a.b"), i(1), 0).unwrap();
    let sa = ma.set_at(&np("a"), i(5), 4).unwrap();
    let sc = mc.set_at(&np("a.c"), i(2), 9).unwrap();
    assert!(sb.hlc < sa.hlc && sa.hlc < sc.hlc);
    let ents = [mb.ent(sb), ma.ent(sa), mc.ent(sc)];
    let mut obs = Vec::new();
    for (ord, m) in all_orders(o, &ents, &[mi.ent(i0)]) {
        let w = want(&[("a", M), ("a.c", i(2)), ("x", i(1))]);
        if m.visible() != w {
            return Err(format!("{ord:?}: read {:?}", m.visible()));
        }
        obs.push(m);
    }
    pull_all(&mut obs);
    pull_all(&mut obs);
    let wanted: BTreeSet<_> = [
        (TypeClash, LossAt::Field(np("a.b")), sb), // the register at a is newer: the child hides
        (TypeClash, LossAt::Field(np("a")), sa),   // c is newer: a is a map, the scalar hides
    ]
    .into();
    if obs.iter().any(|m| m.loss_set() != wanted) {
        return Err(format!("records {:?}", obs[0].loss_set()));
    }
    Ok(())
}

#[test]
fn a_stale_scalar_hides_only_older_children() {
    ok(stale_scalar_hides_only_older_children(&opts()));
}

/// A typeClash is any ancestor/descendant pair, not only a parent and its child.
fn type_clash_at_distance(o: &JoinOptions) -> Result<(), String> {
    two(
        o,
        &[("x", i(1))],
        &set("a", i(5)),
        &set("a.b.c", i(1)),
        (0, 9),
        &[("a", M), ("a.b", M), ("a.b.c", i(1)), ("x", i(1))],
        &[(TypeClash, "a", 'a')],
    )?;
    two(
        o,
        &[("x", i(1))],
        &set("a", s("t")),
        &set("a.b.c", i(1)),
        (9, 0),
        &[("a", s("t")), ("x", i(1))],
        &[(TypeClash, "a.b.c", 'b')],
    )
}

#[test]
fn a_type_clash_is_any_ancestor_descendant_pair() {
    ok(type_clash_at_distance(&opts()));
}

#[test]
fn two_sub_documents_merge_field_by_field_with_no_type_clash() {
    // A marker is not a scalar: `$set a {x}` and `$set a.y` keep both and report nothing.
    ok(two(
        &opts(),
        &[("k", i(0))],
        &set("a", sub(&[("x", i(1))])),
        &set("a.y", i(2)),
        (0, 9),
        &[("a", M), ("a.x", i(1)), ("a.y", i(2)), ("k", i(0))],
        &[],
    ));
}

// ---- the recursive leaf-effect diff -----------------------------------------------------

fn written(m: &mut Mem, path: &str, v: Value) -> Option<BTreeSet<Path>> {
    let r = m.w.write_set(&np(path), v, 0)?;
    m.log(Some(r.clone()));
    Some(r.record.paths.into_iter().collect())
}

fn paths(xs: &[&str]) -> BTreeSet<Path> {
    xs.iter().map(|x| np(x)).collect()
}

fn diff_writes_only_changed_leaves(o: &JoinOptions) -> Result<(), String> {
    let mut m = Mem::new('A', o);
    let big = sub(&[("b", i(1)), ("c", sub(&[("d", s("x")), ("e", i(5))]))]);
    m.ins(&[("a", big.clone()), ("k", i(0))], 0).unwrap();
    let chk = |got: Option<BTreeSet<Path>>, want: Option<BTreeSet<Path>>, what: &str| {
        (got == want).then_some(()).ok_or(format!("{what}: wrote {got:?}, want {want:?}"))
    };
    chk(written(&mut m, "a", big.clone()), None, "the same sub-document is a no-op")?;
    chk(
        written(&mut m, "a", sub(&[("b", i(1)), ("c", sub(&[("d", s("x")), ("e", i(6))]))])),
        Some(paths(&["a.c.e", "a.c", "a"])),
        "one deep leaf changed: the leaf and the markers above it",
    )?;
    chk(
        written(&mut m, "a", sub(&[("b", i(1)), ("c", sub(&[("e", i(6))]))])),
        Some(paths(&["a.c.d", "a.c", "a"])),
        "a field the new value lacks is unset",
    )?;
    chk(
        written(&mut m, "a", sub(&[("b", Value::Int32(1)), ("c", sub(&[("e", i(6))]))])),
        Some(paths(&["a.b", "a"])),
        "type-strict: an Int32 over an Int64 of the same number is a change",
    )?;
    chk(
        written(&mut m, "a", i(7)),
        Some(paths(&["a.b", "a.c", "a.c.e", "a"])),
        "a scalar over a map unsets everything the writer saw below it",
    )?;
    if m.visible() != want(&[("a", i(7)), ("k", i(0))]) {
        return Err(format!("read {:?}", m.visible()));
    }
    chk(
        written(&mut m, "a", sub(&[("n", i(1))])),
        Some(paths(&["a.n", "a"])),
        "a map over a scalar writes the marker and the leaves",
    )?;
    chk(
        written(&mut m, "a", sub(&[])),
        Some(paths(&["a.n", "a"])),
        "an empty map unsets the child",
    )?;
    chk(written(&mut m, "a", sub(&[])), None, "an empty map over an empty map is a no-op")?;
    // replace: only changed fields, and an absent field of the replacing document is unset
    let before = m.w.state.clone();
    let r = m.w.write_replace(&fl(&[("k", i(0)), ("a", sub(&[]))]), 0);
    if r.is_some() || m.w.state != before {
        return Err("an identical replace must change nothing".into());
    }
    let r = m.w.write_replace(&fl(&[("a", sub(&[("z", i(1))]))]), 0).ok_or("replace refused")?;
    let got: BTreeSet<Path> = r.record.paths.into_iter().collect();
    chk(Some(got), Some(paths(&["a.z", "a", "k"])), "replace: the new leaf, its marker, the drop")
}

#[test]
fn a_sub_document_write_and_a_replace_write_only_changed_leaves() {
    ok(diff_writes_only_changed_leaves(&opts()));
}

// ---- refusals below a scalar or an array ------------------------------------------------

fn refuses_below_a_scalar(o: &JoinOptions) -> Result<(), String> {
    let mut w = Writer::new(super::tests::wid('A', 0), o.clone());
    w.insert(
        &fl(&[
            ("s", i(5)),
            ("arr", Value::Array(vec![i(1)])),
            ("n", Value::Null),
            ("m", sub(&[("k", s("v"))])),
        ]),
        0,
    )
    .unwrap();
    let (state, clock) = (w.state.clone(), w.clock);
    for below in ["s.b", "s.b.c", "arr.0", "n.x", "m.k.z"] {
        let target = np(below);
        let refused = w.write_set(&target, i(1), 0).is_none()
            && w.write_unset(&target, 0).is_none()
            && w.write_inc(&target, 1, 0).is_none();
        if !refused || w.state != state || w.clock != clock {
            return Err(format!("{below}: written (or moved the clock)"));
        }
    }
    // a counter and a map are not counted into: $inc of an array, a map, a null is refused too
    for target in ["arr", "m", "n"] {
        if w.write_inc(&np(target), 1, 0).is_some() || w.state != state || w.clock != clock {
            return Err(format!("$inc of {target} was accepted"));
        }
    }
    // below a map is fine, and so is a path under an absent parent (no marker is written for it)
    if w.write_set(&np("m.j"), i(1), 0).is_none() || w.write_set(&np("q.r"), i(1), 0).is_none() {
        return Err("a legal nested write was refused".into());
    }
    Ok(())
}

#[test]
fn a_local_write_below_a_scalar_or_an_array_is_refused_without_a_stamp() {
    ok(refuses_below_a_scalar(&opts()));
}

// ---- odd names are one segment ---------------------------------------------------------

#[test]
fn odd_names_are_one_segment_paths_through_write_join_visible_and_both_codecs() {
    for name in ["", ".", "a.b", "$x"] {
        let one = Path::flat(name).unwrap();
        assert_eq!(one.segments(), [name], "{name:?} is one segment");
        assert_ne!(one, np_or_two(name), "{name:?} is not the dotted spelling");
        let o = opts();
        let mut a = Writer::new(super::tests::wid('A', 0), o.clone());
        // as a top-level name, as a name inside a sub-document, and as a nested leaf
        a.insert(&[(one.clone(), sub(&[(name, i(1))]))], 0).unwrap();
        let inner = one.child(name).unwrap();
        assert_eq!(inner.segments(), [name, name]);
        assert!(a.write_set(&inner, i(2), 0).is_some());
        assert!(a.write_inc(&inner, 3, 0).is_some());
        let mut b = Writer::new(super::tests::wid('B', 0), o.clone());
        b.receive(&a.state);
        assert!(b.write_set(&one.child("other").unwrap(), s("t"), 3).is_some());
        a.receive(&b.state);
        let vis = a.visible().unwrap();
        let mut keys = vec![one.clone(), one.child("other").unwrap(), inner.clone()];
        keys.sort();
        assert_eq!(vis.keys().cloned().collect::<Vec<_>>(), keys);
        assert_eq!((&vis[&one], &vis[&inner]), (&Value::Map, &i(5)));
        assert_eq!(vis[&one.child("other").unwrap()], s("t"));
        assert_eq!(b.state.join(&a.state, &o).0, a.state, "{name:?} joins");
        for st in [&a.state, &b.state] {
            assert_eq!(&MergeState::decode(&st.encode()).unwrap(), st);
            assert_eq!(&MergeState::decode_compact(&st.encode_compact()).unwrap(), st);
        }
        assert_eq!(MergeState::decode(&a.state.encode()).unwrap().visible(&o).unwrap(), vis);
    }
}

/// The path a dotted-string reader would make of a name with a dot, and a one-segment path
/// otherwise (to compare against).
fn np_or_two(name: &str) -> Path {
    if name.contains('.') && name.len() > 1 {
        Path::new(name.split('.')).unwrap()
    } else {
        Path::new([name, name]).unwrap()
    }
}

// ---- every nested rule has a mutant that a scenario catches -------------------------

#[test]
fn mutants_are_caught() {
    type Switch = fn(&mut JoinOptions);
    let cases: [(&str, Switch, Scenario); 8] = [
        (
            "a scalar hides every child, even a newer one",
            |o| o.set_type_ignores_recency(true),
            stale_scalar_hides_only_older_children,
        ),
        (
            "a map marker hides its children",
            |o| o.set_marker_hides(true),
            q3_marker_keeps_concurrent_child,
        ),
        ("no typeClash is reported", |o| o.set_no_type_clash(true), q3_scalar_over_map_type_rule),
        (
            "a typeClash only between a parent and its child",
            |o| o.set_no_type_clash(true),
            type_clash_at_distance,
        ),
        ("two arrays are a plain clash", |o| o.set_array_clash_as_clash(true), q3_array_clash),
        (
            "a sub-document write rewrites every leaf",
            |o| o.set_rewrite_unchanged(true),
            diff_writes_only_changed_leaves,
        ),
        (
            "a write below a scalar is accepted",
            |o| o.set_write_below_scalar(true),
            refuses_below_a_scalar,
        ),
        (
            "a replace rewrites the leaves it kept",
            |o| o.set_rewrite_unchanged(true),
            q3_replace_keeps_nested_increment,
        ),
    ];
    for (what, mutate, scenario) in cases {
        assert_eq!(scenario(&opts()), Ok(()), "the scenario holds without the mutant: {what}");
        let mut o = opts();
        mutate(&mut o);
        assert!(scenario(&o).is_err(), "not caught: {what}");
    }
}
