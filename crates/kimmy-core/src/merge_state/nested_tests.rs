//! Tests of the nested value semantics: cells keyed by (life, segment path), the recursive type
//! rule, the explicit map marker, the recursive leaf-effect diff, the refusal of a write below a
//! scalar or an array, atomic arrays, and `typeClash` / `arrayClash`.
//!
//! The scenarios are ports of the Python model's nested probes (`probes6.py` of the stage A
//! model: `probe_Q3_*`), with the same inputs and the same expected documents; each says which.
//! The model's implicit map marker (a leaf write that creates the maps above it also writes a
//! marker there, decision D-1) is built: a probe whose expected records include the marker's
//! `clash` expects it. Each rule has a mutation switch of [`JoinOptions`]; `mutants_are_caught`
//! turns each on against the scenario that pins it.

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

/// "child newer": `$set a 5` (A, older) against `$set a.b 1` (B, newer), neither having seen the
/// other, over `{x: 1}`: `a` is a map showing only `b`, with the given loss records.
fn scalar_over_map_child_newer(
    o: &JoinOptions,
    records: &[(LossKind, &str, char)],
) -> Result<(), String> {
    two(
        o,
        &[("x", i(1))],
        &set("a", i(5)),
        &set("a.b", i(1)),
        (0, 9),
        &[("a", M), ("a.b", i(1)), ("x", i(1))],
        records,
    )
}

/// The model's behaviour for "child newer": B's leaf write made `a`'s implicit marker, which
/// beats A's older scalar and is reported as a `clash`, beside the `typeClash`.
#[test]
fn model_child_newer_has_the_implicit_markers_clash() {
    ok(scalar_over_map_child_newer(&opts(), &[(TypeClash, "a", 'a'), (Clash, "a", 'a')]));
}

/// A leaf write that creates the map above it: `{x: 1}`, then `$set a.b 1`, then `$unset a.b`.
fn leaf_write_then_unset() -> Mem {
    let mut m = Mem::new('A', &opts());
    m.ins(&[("x", i(1))], 0).unwrap();
    m.set_at(&np("a.b"), i(1), 0).unwrap();
    m
}

/// probes6.py / probes_order.py `probe_implicit_marker_and_emptied_map`: the leaf write left an
/// implicit marker cell at `a`, so once `a.b` is unset the emptied map STAYS, as `{}` (the
/// reference leaves `{a: {}}` too). `$unset a` then removes the lot. The marker is not a user
/// value: the cell at `a` reads as a map, never as a loser.
#[test]
fn model_leaf_write_leaves_an_implicit_marker() {
    let mut m = leaf_write_then_unset();
    let life = m.w.alive()[0];
    let cell = &m.w.state.regs[&(life, np("a"))];
    assert_eq!(cell.value, Value::ImplicitMap, "a cell at `a` after a leaf write");
    assert_eq!(
        m.visible(),
        want(&[("a", M), ("a.b", i(1)), ("x", i(1))]),
        "a map shows while it has a child"
    );
    m.unset_at(&np("a.b"), 0).unwrap();
    assert_eq!(
        m.visible(),
        want(&[("a", M), ("x", i(1))]),
        "the emptied implicit map stays as {{}}"
    );
    m.unset_at(&np("a"), 0).unwrap();
    assert_eq!(m.visible(), want(&[("x", i(1))]), "an unset of the map removes the lot");
}

/// probes6.py `probe_Q3_scalar_over_map_type_rule`: the newer side wins, the side it hides is the
/// typeClash. In the "child newer" case the child write also made `a`'s implicit marker, which
/// beats the older scalar and is reported as a `clash`, beside the `typeClash` of `a`.
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
    // the child write also made `a`'s implicit marker, which beats the older scalar (a `clash`)
    scalar_over_map_child_newer(o, &[(TypeClash, "a", 'a'), (Clash, "a", 'a')])
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
    // `a` is a map every writer sees, so no write below it makes an implicit marker (a marker
    // newer than the scalar would win the register at `a`: the next test)
    let i0 = mi.ins(&[("x", i(1)), ("a", sub(&[("z", i(0))]))], 0).unwrap();
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

/// The same three writes over a document with no `a`: B's `a.b` (stamp 2) and C's `a.c` (stamp 11)
/// each make an implicit marker at `a`, and C's marker is newer than A's `a = 5` (stamp 6), so
/// the register at `a` is a marker and both children show. The model reads the same
/// (`{a: {b: 1, c: 2}}`), with the scalar's `clash` and the two `typeClash` records, the pair
/// rule being independent of who wins the register.
#[test]
fn an_older_scalar_loses_to_the_implicit_marker_of_a_newer_child() {
    let o = opts();
    let (mut mi, mut ma, mut mb, mut mc) =
        (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
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
    for (ord, m) in all_orders(&o, &ents, &[mi.ent(i0)]) {
        let w = want(&[("a", M), ("a.b", i(1)), ("a.c", i(2)), ("x", i(1))]);
        assert_eq!(m.visible(), w, "{ord:?}");
        obs.push(m);
    }
    pull_all(&mut obs);
    pull_all(&mut obs);
    let wanted: BTreeSet<_> = [
        (Clash, LossAt::Field(np("a")), sa), // the scalar loses to the newer implicit marker
        (TypeClash, LossAt::Field(np("a")), sa), // c is newer than the scalar
        (TypeClash, LossAt::Field(np("a.b")), sb), // the scalar is newer than b
    ]
    .into();
    for m in &obs {
        assert_eq!(m.loss_set(), wanted);
    }
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
        // (the clash is the older scalar losing to the implicit marker of `a`; the marker of
        // `a.b` is not in a pair)
        &[(TypeClash, "a", 'a'), (Clash, "a", 'a')],
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
    // A marker is not a scalar: `$set a {x}` and `$set a.y` keep both and report no typeClash.
    // The one record is the older explicit marker of A losing to B's newer implicit one (the
    // model's `clash` on `a`: a write that creates the map above it makes a marker of its own).
    ok(two(
        &opts(),
        &[("k", i(0))],
        &set("a", sub(&[("x", i(1))])),
        &set("a.y", i(2)),
        (0, 9),
        &[("a", M), ("a.x", i(1)), ("a.y", i(2)), ("k", i(0))],
        &[(Clash, "a", 'a')],
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
    // (in the order the writer shows: a replace in another order of the same fields is a write)
    let r = m.w.write_replace(&fl(&[("a", sub(&[])), ("k", i(0))]), 0);
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

// ---- a sub-document is decomposed, never one register -------------------------------------

/// `I` writes a nested document `{a: {x: 1}}` with `first` (an insert, or a replace of an
/// inserted `{q: 0}`); `A` and `B` then hold it and `$set a.b` and `$set a.c` concurrently.
/// The document must be stored DECOMPOSED, a map marker at `a` and a cell for `a.x`, and no
/// register may hold a document; and both sets must survive in every delivery order (were `a`
/// one atomic register, the newer set would replace the whole of it).
fn nested_document_is_decomposed(
    first: &dyn Fn(&mut Mem) -> Option<MStamp>,
    rest: &[(&str, Value)],
) -> Result<(), String> {
    let o = opts();
    let (mut mi, mut ma, mut mb) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let st = first(&mut mi).ok_or("the write of the nested document was refused")?;
    let cells: Vec<(Path, Value)> =
        mi.w.state
            .regs
            .iter()
            .filter(|(_, r)| r.stamp == st && r.value != Value::Unset)
            .map(|((_, p), r)| (p.clone(), r.value.clone()))
            .collect();
    if cells != [(np("a"), M), (np("a.x"), i(1))] {
        return Err(format!("not a marker and a leaf: {cells:?}"));
    }
    if mi.w.state.regs.values().any(|r| matches!(r.value, Value::Document(_))) {
        return Err("a register holds a document".into());
    }
    let seed: Vec<Ent> = mi.oplog.values().cloned().collect();
    for m in [&mut ma, &mut mb] {
        seed.iter().for_each(|e| m.receive_entry(e));
    }
    let sa = ma.set_at(&np("a.b"), i(2), 0).ok_or("A's write was refused")?;
    let sb = mb.set_at(&np("a.c"), i(3), 0).ok_or("B's write was refused")?;
    let ents = [ma.ent(sa), mb.ent(sb)];
    let mut items = vec![("a", M), ("a.b", i(2)), ("a.c", i(3)), ("a.x", i(1))];
    items.extend(rest.iter().cloned());
    for (ord, m) in all_orders(&o, &ents, &seed) {
        if m.visible() != want(&items) {
            return Err(format!("delivery {ord:?}: read {:?}, want {items:?}", m.visible()));
        }
    }
    Ok(())
}

#[test]
fn an_insert_or_replace_of_a_nested_document_is_decomposed_so_concurrent_child_sets_survive() {
    ok(nested_document_is_decomposed(&|m| m.ins(&[("a", sub(&[("x", i(1))]))], 0), &[]));
    ok(nested_document_is_decomposed(
        &|m| {
            m.ins(&[("q", i(0))], 0)?;
            m.replace(&[("a", sub(&[("x", i(1))]))], 0)
        },
        &[],
    ));
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
            assert_eq!(MergeState::decode(&st.encode()).unwrap(), st.without_ords());
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
    let cases: [(&str, Switch, Scenario); 9] = [
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
            "a leaf write under an absent map writes no implicit marker",
            |o| o.set_no_implicit_marker(true),
            q3_scalar_over_map_type_rule,
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

// ---- the implicit map marker (item O4) ------------------------------------------------------

/// The paths holding an implicit marker in the writer's state (one life), in path order.
fn markers(m: &Mem) -> Vec<String> {
    m.w.state
        .regs
        .iter()
        .filter(|(_, r)| r.value == Value::ImplicitMap)
        .map(|((_, p), _)| p.to_string())
        .collect()
}

fn names(ps: &[&str]) -> Vec<Path> {
    ps.iter().map(|p| np(p)).collect()
}

/// probes_order.py `probe_implicit_marker_and_emptied_map`: `$set a.b.c` on an absent `a` writes
/// the markers of `a` and `a.b` under the write's own stamp (outermost first, before the leaf);
/// a second leaf under them writes none; `$unset a.b.c` leaves `{a: {b: {}}}`; `$unset a`
/// removes the lot.
#[test]
fn a_leaf_write_marks_each_absent_ancestor_once_and_the_emptied_maps_stay() {
    let mut m = Mem::new('A', &opts());
    m.ins(&[("x", i(1))], 0).unwrap();
    let life = m.w.alive()[0];
    let w = m.w.write_set(&np("a.b.c"), i(1), 0).unwrap();
    m.log(Some(w.clone()));
    assert_eq!(markers(&m), ["a", "a.b"]);
    for q in ["a", "a.b"] {
        assert_eq!(m.w.state.regs[&(life, np(q))].stamp, w.stamp, "{q}: the write's own stamp");
    }
    assert_eq!(w.record.paths, names(&["a", "a.b", "a.b.c"]), "the markers, then the leaf");
    assert_eq!(w.record.vals[&np("a")], Value::ImplicitMap);
    assert_eq!(m.visible(), want(&[("a", M), ("a.b", M), ("a.b.c", i(1)), ("x", i(1))]));
    let w2 = m.w.write_set(&np("a.b.d"), i(2), 0).unwrap();
    assert_eq!(w2.record.paths, names(&["a.b.d"]), "the maps are seen: no marker");
    m.log(Some(w2));
    assert_eq!(markers(&m), ["a", "a.b"]);
    m.unset_at(&np("a.b.c"), 0).unwrap();
    m.unset_at(&np("a.b.d"), 0).unwrap();
    assert_eq!(m.visible(), want(&[("a", M), ("a.b", M), ("x", i(1))]), "{{a: {{b: {{}}}}}}");
    m.unset_at(&np("a"), 0).unwrap();
    assert_eq!(m.visible(), want(&[("x", i(1))]));
}

/// The model places no marker for an insert, a replace, an unset or the creation of an upsert:
/// `model6.py` `_update` is the only writer of `MAPI` for a write of a live document
/// (lines 966-973: `ancestors_of`), the upserts (`upsert_inc`, `upsert_soi`, lines 845-889) name
/// their ancestors as order items and write cells and slots only, an insert flattens its document
/// with the explicit marker, and a replace and an unset go through `_diff` and `_unset_subtree`.
#[test]
fn an_upsert_an_insert_a_replace_and_an_unset_place_no_implicit_marker() {
    let o = opts();
    let mut up = Mem::new('A', &o);
    let w = up.w.upsert_inc(&np("a.k"), 1, 0);
    up.log(w).unwrap();
    assert!(markers(&up).is_empty(), "upsert_inc");
    assert_eq!(up.visible(), want(&[("a", M), ("a.k", i(1))]), "the slot alone makes the map");
    let mut soi = Mem::new('B', &o);
    let (c, n) = soi
        .w
        .upsert_soi(&[(np("c"), i(1))], &np("a.b.k"), 1, &[(np("d"), sub(&[("e", i(2))]))], 0)
        .unwrap();
    soi.log(Some(c));
    soi.log(Some(n));
    assert!(markers(&soi).is_empty(), "upsert_soi");
    let mut ins = Mem::new('C', &o);
    ins.ins(&[("x", sub(&[("y", sub(&[("z", i(1))]))]))], 0).unwrap();
    assert!(markers(&ins).is_empty(), "insert: the explicit marker");
    ins.replace(&[("x", sub(&[("y", sub(&[("z", i(2)), ("w", i(3))]))])), ("q", i(0))], 0).unwrap();
    assert!(markers(&ins).is_empty(), "replace");
    ins.unset_at(&np("x.y.z"), 0).unwrap();
    assert!(markers(&ins).is_empty(), "unset");
    assert!(
        ins.w.state.regs.values().any(|r| r.value == Value::Map),
        "the sub-documents carry the explicit marker"
    );
}

/// probes_order.py `probe_inc_creating_a_map_is_one_write` (D-3 revised): `$inc k.n` on an absent
/// `k` is ONE write: one stamp, one oplog entry, and an observer holding only that entry reads
/// `{x, k: {n: 5}}`, the marker and the slot in the same state, `k` after `x`.
#[test]
fn an_inc_that_creates_a_map_is_one_write() {
    let o = opts();
    let mut m = Mem::new('A', &o);
    let i0 = m.ins(&[("x", i(1))], 0).unwrap();
    let life = m.w.alive()[0];
    let n0 = m.oplog.len();
    let w = m.w.write_inc(&np("k.n"), 5, 0).unwrap();
    let st = m.log(Some(w.clone())).unwrap();
    assert_eq!(m.oplog.len(), n0 + 1, "one entry");
    assert_eq!(w.record.kind, WriteKind::Inc);
    assert_eq!(w.record.paths, names(&["k.n"]));
    assert_eq!(w.record.vals, BTreeMap::from([(np("k"), Value::ImplicitMap)]));
    assert_eq!(m.w.state.regs[&(life, np("k"))].stamp, st, "the marker and the slot: one stamp");
    assert!(m.w.state.slots.values().all(|sl| sl.stamp == st || sl.total == Num::Int64(0)));
    for (ord, obs) in all_orders(&o, &[m.ent(st)], &[m.ent(i0)]) {
        assert_eq!(obs.visible(), want(&[("k", M), ("k.n", i(5)), ("x", i(1))]), "{ord:?}");
        let doc = obs.w.state.ordered_document(&o).unwrap();
        assert_eq!(doc.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["x", "k"]);
    }
    // a second increment finds the map: no marker, and the record is the plain one
    let w2 = m.w.write_inc(&np("k.n"), 1, 0).unwrap();
    assert!(w2.record.vals.is_empty());
    assert_eq!(markers(&m), ["k"]);
    // the emptied map stays after the counter is unset
    m.unset_at(&np("k.n"), 0).unwrap();
    assert_eq!(m.visible(), want(&[("k", M), ("x", i(1))]));
}

/// An update with several operators places each marker once and under its one stamp (the
/// model's `seen_anc`): `$set a.b`, `$inc a.c` (the second finds `a` already named), and `$set
/// d.e.f`.
#[test]
fn an_update_marks_the_absent_ancestors_of_all_its_operators_once() {
    let mut m = Mem::new('A', &opts());
    m.ins(&[("x", i(1))], 0).unwrap();
    let w =
        m.w.write_update(
            &[
                UpdateOp::Set(np("a.b"), i(1)),
                UpdateOp::Inc(np("a.c"), Value::Int64(2)),
                UpdateOp::Set(np("d.e.f"), i(3)),
            ],
            0,
        )
        .unwrap();
    assert_eq!(markers(&m), ["a", "d", "d.e"]);
    assert_eq!(
        w.record.paths,
        names(&["a", "a.b", "d", "d.e", "d.e.f", "a.c"]),
        "registers, then slots"
    );
    assert!(
        m.w.state
            .regs
            .values()
            .filter(|r| r.value == Value::ImplicitMap)
            .all(|r| r.stamp == w.stamp)
    );
}

/// The join's rules for an implicit marker, checked against the model (`model6.py` join, line
/// 659): a marker is never a reported loser, but it beats an older register and reports it.
#[test]
fn an_implicit_marker_is_never_a_loser_but_makes_one() {
    let o = opts();
    for sk in [(0, 9), (9, 0)] {
        // two markers of one map: nothing to report
        ok(two(
            &o,
            &[("x", i(1))],
            &set("a.b", i(1)),
            &set("a.c", i(2)),
            sk,
            &[("a", M), ("a.b", i(1)), ("a.c", i(2)), ("x", i(1))],
            &[],
        ));
    }
    // an implicit marker (A's `a.y`) against an explicit one (B's `a: {x}`): the older of the two
    // is the loser, and only an explicit loser is reported
    let both = [("a", M), ("a.x", i(1)), ("a.y", i(2)), ("k", i(0))];
    ok(two(
        &o,
        &[("k", i(0))],
        &set("a.y", i(2)),
        &set("a", sub(&[("x", i(1))])),
        (0, 9),
        &both,
        &[],
    ));
    ok(two(
        &o,
        &[("k", i(0))],
        &set("a.y", i(2)),
        &set("a", sub(&[("x", i(1))])),
        (9, 0),
        &both,
        &[(Clash, "a", 'b')],
    ));
}

/// Both codecs spell the implicit marker (tag 6 at depth 0, never inside an array), and the
/// round trip is exact.
#[test]
fn the_implicit_marker_round_trips_in_both_codecs_and_is_refused_in_an_array() {
    let mut m = Mem::new('A', &opts());
    m.ins(&[("x", i(1))], 0).unwrap();
    m.set_at(&np("a.b.c"), i(1), 0).unwrap();
    let st = m.w.state.clone();
    let enc = st.encode_compact();
    let back = MergeState::decode_compact(&enc).unwrap();
    assert_eq!(back, st);
    assert_eq!(back.encode_compact(), enc);
    assert_eq!(back.compact_hash(), st.compact_hash());
    let v1 = MergeState::decode(&st.encode()).unwrap();
    assert_eq!(v1.regs, st.regs, "the version 1 form keeps the cells");
    assert_eq!(v1.encode(), st.encode());
    assert_eq!(back.check_decoded(), Ok(()));
    // never a value a caller hands in, never inside an array
    assert_eq!(Value::ImplicitMap.check_input(), Err(InvalidValue::MapMarker));
    assert_eq!(Value::ImplicitMap.check_register(), Ok(()));
    assert_eq!(
        Value::Array(vec![Value::ImplicitMap]).check_register(),
        Err(InvalidValue::MapMarker)
    );
    assert!(Value::ImplicitMap.is_marker() && Value::Map.is_marker() && !i(1).is_marker());
    assert!(!Value::ImplicitMap.is_plain() && !Value::Map.is_plain());
    // the tag byte: 6 in both forms, where the explicit marker is 5
    let mut explicit = st.clone();
    for r in explicit.regs.values_mut() {
        if r.value == Value::ImplicitMap {
            r.value = Value::Map;
        }
    }
    assert_eq!(st.encode().len(), explicit.encode().len());
    assert_eq!(st.encode_compact().len(), explicit.encode_compact().len());
    assert_ne!(st.encode(), explicit.encode());
    assert_ne!(st.encode_compact(), explicit.encode_compact());
}

/// What an implicit marker costs per absent ancestor (it feeds L34-B): the bytes of a state with
/// its markers against the same state with them removed (the state a writer made before the
/// markers). `$set a.b.c` on `{x: 1}` makes two. Measured once, here, in both forms.
#[test]
fn the_cost_of_an_implicit_marker_is_pinned() {
    let mut m = Mem::new('A', &opts());
    m.ins(&[("x", i(1))], 0).unwrap();
    m.set_at(&np("a.b.c"), i(1), 0).unwrap();
    let with = m.w.state.clone();
    let mut without = with.clone();
    without.regs.retain(|_, r| r.value != Value::ImplicitMap);
    let v1 = with.encode().len() - without.encode().len();
    let v2 = with.encode_compact().len() - without.encode_compact().len();
    // three writers in the version vector: a marker's generation carries the whole vector
    let o = opts();
    let (mut mi, mut mb, mut mc) = (Mem::new('I', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = mi.ins(&[("x", i(1))], 0).unwrap();
    mb.receive_entry(&mi.ent(i0));
    mc.receive_entry(&mi.ent(i0));
    let sb = mb.set_at(&np("x"), i(2), 0).unwrap();
    let sc = mc.set_at(&np("x"), i(3), 0).unwrap();
    for e in [mb.ent(sb), mc.ent(sc)] {
        mi.receive_entry(&e);
    }
    mi.set_at(&np("a.b.c"), i(1), 0).unwrap();
    let with = mi.w.state.clone();
    let mut without = with.clone();
    without.regs.retain(|_, r| r.value != Value::ImplicitMap);
    let (w1, w2) = (
        with.encode().len() - without.encode().len(),
        with.encode_compact().len() - without.encode_compact().len(),
    );
    eprintln!(
        "two implicit markers: one writer v1 {v1}, compact {v2}; three writers v1 {w1}, compact {w2}"
    );
    // two markers (`a`, `a.b`): 161 bytes each in the version 1 form, 19 in the compact form, on
    // average; the generation's version vector is not encoded, so the writers do not add to it
    assert_eq!(((v1, v2), (w1, w2)), ((322, 38), (322, 38)));
}
