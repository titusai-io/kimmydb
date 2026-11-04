//! The oracle: independent of the state machinery, computed from the global
//! causal log (model5.py after "oracle: independent of the state machinery").
//! Names and structure follow the Python so the two can be diffed.
//!
//! The log maps a write's stamp to `(ctx, meta)`: the stamps of the writes in
//! its causal past and what the write did. Every function that reads the
//! model's global `RET` takes it as `ret`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::merge_state::{
    LossAt, LossKey, LossKind, MStamp, Path, Value, WriteKind, WriteRecord, WriterId,
};

pub type Log = BTreeMap<MStamp, (BTreeSet<MStamp>, WriteRecord)>;
pub type Doc = BTreeMap<Path, Value>;
/// A head that must be, or is, reported: `(path, stamp)`.
/// The path the oracle gives a whole-document record. The model spells it `_`, but a field can
/// be named `_` too (the workload has one), and a record about that field is a different record,
/// so the oracle spells the document with a name no field of the workload has (one with a control
/// character, which the workload never uses) and keeps the field `_` as itself.
pub fn whole_document() -> Path {
    Path::flat("\u{1}whole document").unwrap()
}

/// The oracle's path of a loss record: the field, or [`whole_document`].
pub fn at_path(at: &LossAt) -> Path {
    at.field().cloned().unwrap_or_else(whole_document)
}

pub type Head = (Path, MStamp);

/// `st[0]`: the sim only makes stamps with a zero counter.
fn t(st: &MStamp) -> i128 {
    i128::from(st.hlc.wall_ms)
}

fn in_set(life: &Option<MStamp>, set: &BTreeSet<MStamp>) -> bool {
    life.as_ref().is_some_and(|l| set.contains(l))
}

fn is_reg_kind(k: WriteKind) -> bool {
    matches!(k, WriteKind::Insert | WriteKind::Reg)
}

/// The paths a write put a cell at, a counter slot or a register: its `paths`, and the implicit
/// markers an `$inc` made beside its slot (a map-creating `$inc` is ONE write; the markers are in
/// its `vals`, its `paths` hold the counter's path alone).
fn touched(m: &WriteRecord) -> BTreeSet<&Path> {
    let markers = (m.kind == WriteKind::Inc).then_some(&m.vals);
    m.paths.iter().chain(markers.into_iter().flat_map(|v| v.keys())).collect()
}

/// Did the write set a REGISTER at `p`: an insert or a set, or the implicit marker of a map an
/// `$inc` created?
fn sets_reg(m: &WriteRecord, p: &Path) -> bool {
    (is_reg_kind(m.kind) && m.paths.contains(p))
        || (m.kind == WriteKind::Inc && m.vals.contains_key(p))
}

/// An explicit insert (insertOne/insertMany), not the creation write of an upsert.
fn _explicit(m: &WriteRecord) -> bool {
    m.kind == WriteKind::Insert && !m.upsert
}

/// V6m2/V7m1/V7m2: a `$setOnInsert` cell of an upsert creation is dropped when
/// an explicit insert (a first insert or a re-insert after a delete) is alive.
fn _supp(log: &Log, m: &WriteRecord, p: &Path, alive: &BTreeSet<MStamp>) -> bool {
    m.soi.iter().any(|q| q == p) && alive.iter().any(|l| _explicit(&log[l].1))
}

fn deletes(log: &Log) -> Vec<MStamp> {
    log.iter().filter(|(_, (_, m))| m.kind == WriteKind::Delete).map(|(st, _)| *st).collect()
}

/// The model's `ins`: the writes that create a life.
fn creators(log: &Log) -> BTreeSet<MStamp> {
    log.iter()
        .filter(|(_, (_, m))| m.kind == WriteKind::Insert || m.insert)
        .map(|(st, _)| *st)
        .collect()
}

fn all_paths(log: &Log) -> BTreeSet<Path> {
    log.values().flat_map(|(_, m)| touched(m).into_iter().cloned()).collect()
}

pub fn killed_lives(log: &Log, ret: u64) -> BTreeSet<MStamp> {
    let dels = deletes(log);
    log.iter()
        .filter(|(st, (ctx, m))| {
            _explicit(m) && dels.iter().any(|d| !ctx.contains(d) && t(d) >= t(st) - i128::from(ret))
        })
        .map(|(st, _)| *st)
        .collect()
}

/// Q2: of the concurrent first inserts (explicit inserts by a creator that had
/// seen no delete), all but the newest.
pub fn superseded_lives(log: &Log, within: Option<&BTreeSet<MStamp>>) -> BTreeSet<MStamp> {
    let dels = deletes(log);
    let first: Vec<MStamp> = log
        .iter()
        .filter(|(st, (ctx, m))| {
            _explicit(m)
                && !dels.iter().any(|d| ctx.contains(d))
                && within.is_none_or(|w| w.contains(st))
        })
        .map(|(st, _)| *st)
        .collect();
    let newest = first.iter().max().copied();
    first.into_iter().filter(|l| Some(*l) != newest).collect()
}

fn _alive_lives(log: &Log, ins: &BTreeSet<MStamp>, dels: &[MStamp], ret: u64) -> BTreeSet<MStamp> {
    let sup = superseded_lives(log, None);
    ins.iter()
        .filter(|l| {
            !sup.contains(l)
                && dels
                    .iter()
                    .filter(|d| t(d) >= t(l) - i128::from(ret))
                    .all(|d| log[*l].0.contains(d))
        })
        .copied()
        .collect()
}

/// Lives a writer with causal past `ctx` could see as alive.
fn _alive_at(
    log: &Log,
    ctx: &BTreeSet<MStamp>,
    ins: &BTreeSet<MStamp>,
    dels: &[MStamp],
    ret: u64,
) -> BTreeSet<MStamp> {
    let sup = superseded_lives(log, Some(ctx));
    ins.iter()
        .filter(|l| {
            ctx.contains(l)
                && !sup.contains(l)
                && dels
                    .iter()
                    .filter(|d| ctx.contains(d) && t(d) >= t(l) - i128::from(ret))
                    .all(|d| log[*l].0.contains(d))
        })
        .copied()
        .collect()
}

/// Generation of a write, from the log alone (V3M3). In words: a non-numeric
/// base starts its own generation; any other write belongs to the generation of
/// the base that was winning at its writer (the highest-stamped register write
/// in the writer's causal past, in a life the writer saw as alive).
pub struct MakeGen<'a> {
    log: &'a Log,
    ins: BTreeSet<MStamp>,
    dels: Vec<MStamp>,
    ret: u64,
    memo: RefCell<BTreeMap<(MStamp, Path), Option<MStamp>>>,
}

pub fn make_gen(log: &Log, ret: u64) -> MakeGen<'_> {
    MakeGen {
        log,
        ins: creators(log),
        dels: deletes(log),
        ret,
        memo: RefCell::new(BTreeMap::new()),
    }
}

impl MakeGen<'_> {
    pub fn of(&self, st: MStamp, p: &Path) -> Option<MStamp> {
        if let Some(g) = self.memo.borrow().get(&(st, p.clone())) {
            return *g;
        }
        let (ctx, m) = &self.log[&st];
        let g = if sets_reg(m, p) && !m.vals[p].is_numeric() {
            Some(st)
        } else {
            let al = _alive_at(self.log, ctx, &self.ins, &self.dels, self.ret);
            let cands = ctx.iter().filter(|w| {
                let m2 = &self.log[*w].1;
                sets_reg(m2, p) && in_set(&m2.life, &al) && !_supp(self.log, m2, p, &al)
            });
            cands.max().and_then(|w| self.of(*w, p))
        };
        self.memo.borrow_mut().insert((st, p.clone()), g);
        g
    }
}

/// `gi == gW or (gi is not None and (gW is None or gi > gW))`: the generation
/// is the winner's or a NEWER one.
fn gen_counts(gi: Option<MStamp>, g_w: Option<MStamp>) -> bool {
    gi == g_w || (gi.is_some() && (g_w.is_none() || gi > g_w))
}

/// Returns (flat per-path values or None, alive lives, deletes). The value of a path, in
/// words: the winning base (the highest-stamped register write in an alive
/// life), plus every increment that NO base write on the path had seen (a base write is a
/// register write at the path in an alive life, its `$setOnInsert` creation cells suppressed
/// as the read side suppresses them; an increment is seen by it when it is in its causal
/// past), except the increments the winner's generation overwrote (an increment made in an
/// OLDER generation than the winner's, and not seen by the starter of the
/// winner's generation). Increments made in a NEWER generation are kept even
/// when its starter lost.
///
/// `bound` is the integer bound of the run (`JoinOptions::int_bound`, the real Int64 bound unless
/// a run lowers it so that the clamp is exercised): a total saturates to `[-bound - 1, bound]`.
///
/// This is the value of every PATH on its own (a register, a map marker, a counter); the type
/// rule that decides which paths show is [`oracle`].
fn flat_oracle(log: &Log, ret: u64, bound: i64) -> (Option<Doc>, BTreeSet<MStamp>, Vec<MStamp>) {
    let b = i128::from(bound);
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);

    let writes_on =
        |p: &Path, kinds: &[WriteKind]| -> Vec<(MStamp, &BTreeSet<MStamp>, &WriteRecord)> {
            log.iter()
                .filter(|(_, (_, m))| {
                    kinds.contains(&m.kind) && m.paths.iter().any(|q| q == p) && m.life.is_some()
                })
                .map(|(st, (ctx, m))| (*st, ctx, m))
                .collect()
        };

    let mut doc = Doc::new();
    for p in all_paths(log) {
        let regs: Vec<_> = log
            .iter()
            .filter(|(_, (_, m))| sets_reg(m, &p) && m.life.is_some())
            .map(|(st, (ctx, m))| (*st, ctx, m))
            .filter(|(_, _, m)| in_set(&m.life, &alive) && !_supp(log, m, &p, &alive))
            .collect();
        let incs: Vec<_> = writes_on(&p, &[WriteKind::Inc])
            .into_iter()
            .filter(|(_, _, m)| in_set(&m.life, &alive))
            .collect();
        let win = regs.iter().max_by_key(|r| r.0);
        if let Some(w) = win
            && !w.2.vals[&p].is_numeric()
        {
            doc.insert(p.clone(), w.2.vals[&p].clone());
            continue;
        }
        let g_w = win.and_then(|w| gener.of(w.0, &p));
        let counted = incs.iter().filter(|(st, _, _)| gen_counts(gener.of(*st, &p), g_w));
        let base: Option<i128> = match win.map(|w| &w.2.vals[&p]) {
            Some(Value::Int64(i)) => Some(i128::from(*i)),
            _ => None,
        };
        // what any base write on the path had seen: the causal pasts of all of them (the winner's
        // included), not the winner's alone. Structural: the log's contexts, never the model's.
        let seen_by_a_base: BTreeSet<MStamp> =
            regs.iter().flat_map(|(_, ctx, _)| ctx.iter().copied()).collect();
        let extra: Vec<i64> = counted
            .filter(|(st, _, _)| !seen_by_a_base.contains(st))
            .map(|(_, _, m)| m.n.unwrap())
            .collect();
        if base.is_none() && extra.is_empty() {
            continue;
        }
        let total = base.unwrap_or(0) + extra.iter().map(|n| i128::from(*n)).sum::<i128>();
        doc.insert(p, Value::Int64(total.clamp(-b - 1, b) as i64));
    }
    ((!alive.is_empty()).then_some(doc), alive, dels)
}

fn is_val(v: &Value) -> bool {
    !matches!(v, Value::Unset | Value::Map | Value::ImplicitMap)
}

type WriteAt<'a> = (MStamp, &'a BTreeSet<MStamp>, &'a WriteRecord);

/// Register writes (inserts and sets, unsets and markers included) at `p` in alive lives.
fn regs_at<'a>(log: &'a Log, alive: &BTreeSet<MStamp>, p: &Path) -> Vec<WriteAt<'a>> {
    log.iter()
        .filter(|(_, (_, m))| sets_reg(m, p) && in_set(&m.life, alive) && !_supp(log, m, p, alive))
        .map(|(st, (ctx, m))| (*st, ctx, m))
        .collect()
}

fn incs_at<'a>(log: &'a Log, alive: &BTreeSet<MStamp>, p: &Path) -> Vec<WriteAt<'a>> {
    log.iter()
        .filter(|(_, (_, m))| {
            m.kind == WriteKind::Inc && m.paths.contains(p) && in_set(&m.life, alive)
        })
        .map(|(st, (ctx, m))| (*st, ctx, m))
        .collect()
}

/// A visible subtree: a leaf, or a map and its children (in name order).
enum Tree {
    Leaf(Value),
    Map(Vec<(String, Tree)>),
}

fn flatten(t: &Tree, at: &Path, out: &mut Doc) {
    match t {
        Tree::Leaf(v) => {
            out.insert(at.clone(), v.clone());
        }
        Tree::Map(kids) => {
            out.insert(at.clone(), Value::Map);
            for (name, k) in kids {
                flatten(k, &at.child(name).expect("a valid name"), out);
            }
        }
    }
}

/// The type rule of D4 over the per-path values.
struct TypeRule<'a> {
    log: &'a Log,
    alive: &'a BTreeSet<MStamp>,
    flat: &'a Doc,
    kids: BTreeMap<Path, Vec<Path>>,
}

impl TypeRule<'_> {
    /// The subtree at `q` and the newest stamp among the writes to it and below it, or `None`
    /// when nothing shows there.
    fn node(&self, q: &Path) -> Option<(Tree, MStamp)> {
        let mut ks: Vec<(String, Tree, MStamp)> = Vec::new();
        for c in self.kids.get(q).into_iter().flatten() {
            if let Some((t, st)) = self.node(c) {
                ks.push((c.name().to_string(), t, st));
            }
        }
        let regs = regs_at(self.log, self.alive, q);
        let incs = incs_at(self.log, self.alive, q);
        let own_n = regs.iter().chain(incs.iter()).map(|r| r.0).max();
        let own = self.flat.get(q);
        if ks.is_empty() {
            let own = own?;
            let tree =
                if own.is_marker() { Tree::Map(Vec::new()) } else { Tree::Leaf(own.clone()) };
            return Some((tree, own_n?));
        }
        if let Some(own) = own.filter(|v| !v.is_marker()) {
            // a scalar or an array hides the children that are not newer than its register
            let rs = match regs.iter().max_by_key(|r| r.0) {
                Some(r) if is_val(&r.2.vals[q]) => r.0,
                _ => own_n?,
            };
            let shown: Vec<_> = ks.into_iter().filter(|k| k.2 > rs).collect();
            if shown.is_empty() {
                return Some((Tree::Leaf(own.clone()), own_n?));
            }
            let top = shown.iter().map(|k| k.2).chain(own_n).max()?;
            return Some((Tree::Map(shown.into_iter().map(|k| (k.0, k.1)).collect()), top));
        }
        // no cell of its own, or a marker: a map showing every child
        let top = ks.iter().map(|k| k.2).chain(own_n).max()?;
        ks.sort_by(|a, b| a.0.cmp(&b.0));
        Some((Tree::Map(ks.into_iter().map(|k| (k.0, k.1)).collect()), top))
    }
}

/// Returns (the visible document, alive lives, deletes), the document as the flat map
/// [`MergeState::visible`](kimmy_core::merge_state::MergeState::visible) returns: every present
/// path, with `Value::Map` at a map. The TYPE RULE of D4 at every depth, in words: a path p is a
/// map iff some descendant is visible and newer than p's own scalar or array register, or a map
/// marker is visible at p; otherwise it is its register. A marker never hides a child. A newer
/// scalar register at p beats an older marker (p's register is the newest register write at p).
/// An empty map shows when its marker does and no child does. "Newer" compares the newest stamp
/// among all writes to the descendant (a register write or an increment, in an alive life) with
/// the stamp of p's register: a stale scalar hides only the children older than it (D-13).
///
/// A leaf write under an absent map writes an implicit marker for each map above it that its
/// writer does not see (it arrives in the write's record: see [`touched`]); a marker reads as a
/// map like the explicit one, and a path with no cell of its own is a map if a child shows. The
/// ORDER of a level is not modelled.
pub fn oracle(log: &Log, ret: u64, bound: i64) -> (Option<Doc>, BTreeSet<MStamp>, Vec<MStamp>) {
    let (flat, alive, dels) = flat_oracle(log, ret, bound);
    let Some(flat) = flat else { return (None, alive, dels) };
    let mut allp = all_paths(log);
    for q in allp.clone() {
        allp.extend(q.ancestors());
    }
    let mut kids: BTreeMap<Path, Vec<Path>> = BTreeMap::new();
    let mut tops = Vec::new();
    for q in &allp {
        match q.parent() {
            None => tops.push(q.clone()),
            Some(par) => kids.entry(par).or_default().push(q.clone()),
        }
    }
    let rule = TypeRule { log, alive: &alive, flat: &flat, kids };
    let mut doc = Doc::new();
    for t in &tops {
        if let Some((tree, _)) = rule.node(t) {
            flatten(&tree, t, &mut doc);
        }
    }
    (Some(doc), alive, dels)
}

/// The visible document of a writer with the causal past `ctx`: the oracle over that part of
/// the log. `None`: no life is alive there.
pub fn oracle_at(log: &Log, ctx: &BTreeSet<MStamp>, ret: u64, bound: i64) -> Option<Doc> {
    let sub: Log = ctx.iter().filter_map(|st| log.get(st).map(|e| (*st, e.clone()))).collect();
    oracle(&sub, ret, bound).0
}

/// `typeClash` heads that must be reported, as (path, stamp). W is the newest register at an
/// ancestor `t`, a scalar or an array. A descendant `c` that is not visible in the final
/// document (so hidden by W's recency) loses each of its value-bearing heads that W's writer had
/// not seen. A descendant that is visible and has such a head NEWER than W (not having seen W)
/// makes `t` a map: W is the loser.
fn type_must(log: &Log, alive: &BTreeSet<MStamp>, doc: Option<&Doc>, ret: u64) -> BTreeSet<Head> {
    let mut must = BTreeSet::new();
    let gener = make_gen(log, ret);
    let allp = all_paths(log);
    for t in &allp {
        let rs = regs_at(log, alive, t);
        let Some(w) = rs.iter().max_by_key(|r| r.0) else { continue };
        if !is_val(&w.2.vals[t]) {
            continue;
        }
        for c in allp.iter().filter(|q| q.is_below(t)) {
            let cr = regs_at(log, alive, c);
            let ci = incs_at(log, alive, c);
            let Some(newest) = cr.iter().chain(ci.iter()).map(|r| r.0).max() else { continue };
            // the heads of c: value-bearing registers no other register saw, and the latest
            // increment of each (life, writer, generation) group
            let mut heads: Vec<(MStamp, &BTreeSet<MStamp>)> = cr
                .iter()
                .filter(|r| is_val(&r.2.vals[c]) && !cr.iter().any(|r2| r2.1.contains(&r.0)))
                .map(|r| (r.0, r.1))
                .collect();
            let mut groups: BTreeMap<(Option<MStamp>, WriterId, Option<MStamp>), Vec<MStamp>> =
                BTreeMap::new();
            for (st, _, m) in &ci {
                groups.entry((m.life, st.writer, gener.of(*st, c))).or_default().push(*st);
            }
            heads.extend(groups.values().map(|sts| {
                let top = *sts.iter().max().unwrap();
                (top, &log[&top].0)
            }));
            let visible = doc.is_some_and(|d| d.contains_key(c));
            if newest <= w.0 && !visible {
                must.extend(
                    heads.iter().filter(|(h, _)| !w.1.contains(h)).map(|(h, _)| (c.clone(), *h)),
                );
            } else if visible && heads.iter().any(|(h, hctx)| *h > w.0 && !hctx.contains(&w.0)) {
                must.insert((t.clone(), w.0));
            }
        }
    }
    must
}

/// Heads that must be reported, as (path, stamp): a head needs a record of ANY
/// kind (V3m2).
pub fn loss_oracle(
    log: &Log,
    alive: &BTreeSet<MStamp>,
    dels: &[MStamp],
    doc: Option<&Doc>,
    ret: u64,
) -> BTreeSet<Head> {
    let mut must: BTreeSet<Head> = type_must(log, alive, doc, ret);
    let gener = make_gen(log, ret);
    let sup = superseded_lives(log, None);
    let killed = killed_lives(log, ret);
    let sup_live: BTreeSet<MStamp> = sup.difference(&killed).copied().collect();
    for l in &sup_live {
        must.insert((whole_document(), *l));
    }
    // V6M1: every head written into a losing first insert is reported
    for (st, (_, m)) in log {
        if in_set(&m.life, &sup_live)
            && matches!(m.kind, WriteKind::Reg | WriteKind::Inc)
            && Some(*st) != m.life
        {
            for p in touched(m) {
                let later = log.values().any(|(c2, m2)| {
                    c2.contains(st) && touched(m2).contains(p) && m2.life == m.life
                });
                if !later {
                    must.insert((p.clone(), *st));
                }
            }
        }
    }
    // V6m2: the dropped $setOnInsert cells are reported
    for (st, (_, m)) in log {
        for p in &m.soi {
            if in_set(&m.life, alive) && _supp(log, m, p, alive) {
                let later = log.values().any(|(c2, m2)| {
                    c2.contains(st)
                        && m2.paths.contains(p)
                        && m2.life == m.life
                        && is_reg_kind(m2.kind)
                });
                // a cell overwritten in sequence by its own life's writer is no longer a head
                if !later {
                    must.insert((p.clone(), *st));
                }
            }
        }
    }
    for p in all_paths(log) {
        let regs: Vec<(MStamp, &BTreeSet<MStamp>, &WriteRecord)> = log
            .iter()
            .filter(|(_, (_, m))| {
                sets_reg(m, &p) && in_set(&m.life, alive) && !_supp(log, m, &p, alive)
            })
            .map(|(st, (ctx, m))| (*st, ctx, m))
            .collect();
        let heads: Vec<_> =
            regs.iter().filter(|r| !regs.iter().any(|r2| r2.1.contains(&r.0))).collect();
        if let Some(top) = heads.iter().max_by_key(|r| r.0) {
            for h in &heads {
                // an implicit marker carries no user value: never a reported loser (model6.py line 1472)
                if h.0 != top.0 && h.2.vals[&p] != Value::ImplicitMap {
                    must.insert((p.clone(), h.0));
                }
            }
        }
        let dead_regs: Vec<(MStamp, &BTreeSet<MStamp>, &WriteRecord)> = log
            .iter()
            .filter(|(st, (_, m))| {
                sets_reg(m, &p)
                    && !in_set(&m.life, alive)
                    && !in_set(&m.life, &sup_live)
                    && !(in_set(&m.life, &sup) && Some(**st) == m.life)
            })
            .map(|(st, (ctx, m))| (*st, ctx, m))
            .collect();
        // a superseded life's own fields: order-dependent (excluded above)
        for (st, _, _) in &dead_regs {
            if !dels.iter().any(|d| log[d].0.contains(st))
                && !dead_regs.iter().any(|(_, c2, _)| c2.contains(st))
            {
                must.insert((p.clone(), *st));
            }
        }
        let dead_incs: Vec<MStamp> = log
            .iter()
            .filter(|(_, (_, m))| {
                m.kind == WriteKind::Inc
                    && m.paths.contains(&p)
                    && !in_set(&m.life, alive)
                    && !in_set(&m.life, &sup_live)
            })
            .map(|(st, _)| *st)
            .collect();
        let mut by_slot: BTreeMap<(Option<MStamp>, WriterId, Option<MStamp>), Vec<MStamp>> =
            BTreeMap::new();
        for st in dead_incs {
            by_slot.entry((log[&st].1.life, st.writer, gener.of(st, &p))).or_default().push(st);
        }
        for sts in by_slot.values() {
            let head = *sts.iter().max().unwrap();
            if !dels.iter().any(|d| log[d].0.contains(&head)) {
                must.insert((p.clone(), head));
            }
        }
    }
    must
}

/// V4M1: from D5's words. For each path with a winning numeric or non-numeric
/// base: the whole total of every (life, writer, generation) group of
/// increments in an OLDER generation than the winner's, whose latest increment
/// is not in the causal past of the starter of the winner's generation.
pub fn hidden_oracle(log: &Log, ret: u64) -> BTreeMap<Path, i128> {
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);
    let mut out = BTreeMap::new();
    for p in all_paths(log) {
        let regs: Vec<MStamp> = log
            .iter()
            .filter(|(_, (_, m))| {
                sets_reg(m, &p) && in_set(&m.life, &alive) && !_supp(log, m, &p, &alive)
            })
            .map(|(st, _)| *st)
            .collect();
        let Some(top) = regs.iter().max() else { continue };
        let Some(g_w) = gener.of(*top, &p) else { continue };
        let mut groups: BTreeMap<(Option<MStamp>, WriterId, Option<MStamp>), Vec<MStamp>> =
            BTreeMap::new();
        for (st, (_, m)) in log {
            if m.kind == WriteKind::Inc && m.paths.contains(&p) && in_set(&m.life, &alive) {
                groups.entry((m.life, st.writer, gener.of(*st, &p))).or_default().push(*st);
            }
        }
        let mut total: i128 = 0;
        for ((_, _, gi), sts) in &groups {
            if gi.is_none_or(|g| g < g_w) {
                total += sts
                    .iter()
                    .filter(|s| !log[&g_w].0.contains(s))
                    .map(|s| i128::from(log[s].1.n.unwrap()))
                    .sum::<i128>();
            }
        }
        if total != 0 {
            out.insert(p, total);
        }
    }
    out
}

/// Section 17.3, second half: no record may name an increment that survives
/// (is counted in the value).
pub fn stale_records(
    records: &BTreeSet<LossKey>,
    log: &Log,
    ret: u64,
) -> Vec<(LossKind, Path, MStamp)> {
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);
    let mut bad = Vec::new();
    for k in records {
        let (kind, p, st) = (k.kind, &at_path(&k.at), k.stamp);
        let m = &log[&st].1;
        // hiding by the type rule is not monotone (a later descendant can revive it)
        if kind == LossKind::TypeClash || m.kind != WriteKind::Inc || !in_set(&m.life, &alive) {
            continue;
        }
        let regs: Vec<MStamp> = log
            .iter()
            .filter(|(_, (_, m2))| {
                sets_reg(m2, p) && in_set(&m2.life, &alive) && !_supp(log, m2, p, &alive)
            })
            .map(|(w, _)| *w)
            .collect();
        let Some(w) = regs.iter().max() else {
            bad.push((kind, p.clone(), st));
            continue;
        };
        if !log[w].1.vals[p].is_numeric() {
            continue;
        }
        if gen_counts(gener.of(st, p), gener.of(*w, p)) {
            bad.push((kind, p.clone(), st));
        }
    }
    bad
}

/// V7m4: for each first insert that is superseded AND killed by a delete that
/// neither saw it nor was seen by it, the sets of keys of which at least one
/// must be reported: insertLost for the whole insert, or deleteWon of one of
/// its fields.
pub fn killed_superseded_any(log: &Log, ret: u64) -> Vec<BTreeSet<Head>> {
    let dels = deletes(log);
    let mut out = Vec::new();
    for l in superseded_lives(log, None) {
        let (ctx, m) = &log[&l];
        // killed, and no delete observed it
        if dels.iter().any(|d| !ctx.contains(d) && t(d) >= t(&l) - i128::from(ret))
            && !dels.iter().any(|d| log[d].0.contains(&l))
        {
            let mut set: BTreeSet<Head> = BTreeSet::new();
            set.insert((whole_document(), l));
            set.extend(m.paths.iter().map(|p| (p.clone(), l)));
            out.push(set);
        }
    }
    out
}

/// The deletes that killed `life`: not in the causal past of its creator and inside its
/// retention (the model's `_killers`). A delete older than `life - ret` never kills it.
fn killers(log: &Log, life: &MStamp, dels: &[MStamp], ret: u64) -> Vec<MStamp> {
    dels.iter()
        .filter(|d| !log[life].0.contains(d) && t(d) >= t(life) - i128::from(ret))
        .copied()
        .collect()
}

/// Is a record sound: does the log contain what it claims? The write it names (`key.stamp`)
/// must have written its path ([`whole_document`] is the whole document, and only an `insertLost` of a
/// lost first insert may name it), and the kind's own evidence must exist:
///
/// * a clash: a newer register write to the path that neither saw nor was seen by the loser;
/// * an `insertLost`: the write is in a superseded first insert, or is a `$setOnInsert` cell;
/// * a `deleteWon`: the write's life is not alive, and a delete that killed that life (inside
///   its retention, unseen by its creator) had not seen the write.
pub fn sound_record(
    key: &LossKey,
    log: &Log,
    alive: &BTreeSet<MStamp>,
    dels: &[MStamp],
    ret: u64,
) -> bool {
    let (kind, p, st) = (key.kind, &at_path(&key.at), key.stamp);
    let Some((ctx, m)) = log.get(&st) else { return false };
    match kind {
        // A loss to a newer register write that neither saw nor was seen by the loser. Two arrays
        // are an `arrayClash`, anything else a `clash`: a `clash` needs a winner that is not an
        // array when the loser is one.
        LossKind::Clash | LossKind::ArrayClash => {
            let arr = |m: &WriteRecord| matches!(m.vals.get(p), Some(Value::Array(_)));
            sets_reg(m, p)
                && log.iter().any(|(w, (c2, m2))| {
                    *w > st
                        && !c2.contains(&st)
                        && !ctx.contains(w)
                        && sets_reg(m2, p)
                        && ((arr(m) && arr(m2)) == (kind == LossKind::ArrayClash))
                })
        }
        // A cell below an ancestor's scalar or array that is newer, or the scalar or array
        // that a newer cell below it makes a map (any depth); in both the other side is
        // concurrent.
        LossKind::TypeClash => {
            let bears = |m2: &WriteRecord, q: &Path| {
                (sets_reg(m2, q) && is_val(&m2.vals[q]))
                    || (m2.kind == WriteKind::Inc && m2.paths.contains(q))
            };
            let conc = |w: &MStamp, c2: &BTreeSet<MStamp>| {
                *w != st && !c2.contains(&st) && !ctx.contains(w)
            };
            if bears(m, p)
                && p.ancestors().any(|par| {
                    log.iter().any(|(w, (c2, m2))| {
                        *w > st && conc(w, c2) && sets_reg(m2, &par) && is_val(&m2.vals[&par])
                    })
                })
            {
                return true;
            }
            sets_reg(m, p)
                && is_val(&m.vals[p])
                && log.iter().any(|(w, (c2, m2))| {
                    *w > st
                        && conc(w, c2)
                        && touched(m2).into_iter().any(|q| q.is_below(p) && bears(m2, q))
                })
        }
        LossKind::InsertLost => {
            let sup = superseded_lives(log, None);
            if *p == whole_document() {
                return sup.contains(&st);
            }
            touched(m).contains(p)
                && (in_set(&m.life, &sup) || (m.kind == WriteKind::Insert && m.soi.contains(p)))
        }
        LossKind::DeleteWon => {
            touched(m).contains(p)
                && m.life.is_some_and(|l| {
                    log.contains_key(&l)
                        && !alive.contains(&l)
                        && killers(log, &l, dels, ret).iter().any(|d| !log[d].0.contains(&st))
                })
        }
        // Not produced by the workload: no record of this kind is sound here.
        LossKind::RetiredWriter => false,
    }
}

// ---- the leaf effects of a write ---------------------------------------------------------

/// What a client asked a node to do: the intent the leaf effects are checked against.
#[derive(Clone, Debug)]
pub enum Intent {
    /// `$set path value`; the value may be a `Document` (a sub-document).
    Set(Path, Value),
    Unset(Path),
    /// `$inc path n`.
    Inc(Path),
    /// A replacement document: its fields in order.
    Replace(Vec<(String, Value)>),
    Insert(Vec<(String, Value)>),
}

/// The effects a write must have: the cells it sets (`Value::Unset` for an unset), or a refusal.
/// No effects is a no-op, which the writer reports as no write at all.
#[derive(Debug, PartialEq)]
pub enum Expected {
    Refused,
    Effects(BTreeMap<Path, Value>),
}

/// Two values are the same stored representation (type-strict, doubles by bit pattern).
fn ident(a: &Value, b: &Value) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

/// Is some proper ancestor of `p` visible as a scalar or an array? The reference cannot create
/// a field below one, so the write is refused (D-2).
fn below_scalar(old: &Doc, p: &Path) -> bool {
    p.ancestors().any(|a| old.get(&a).is_some_and(|v| *v != Value::Map))
}

/// The implicit marker of each proper ancestor of `p` the writer does not see (the model's
/// `ancestors_of`, `model6.py` lines 966-973), written by a leaf write in the same write. An
/// insert, a replace, an unset and the creation of an upsert place none.
fn implicit_markers(old: &Doc, p: &Path, out: &mut BTreeMap<Path, Value>) {
    for a in p.ancestors().filter(|a| !old.contains_key(a)) {
        out.insert(a, Value::ImplicitMap);
    }
}

/// Everything visible strictly below `p`, and `p`, unset.
fn unset_all(old: &Doc, p: &Path, out: &mut BTreeMap<Path, Value>) {
    for q in old.keys().filter(|q| q.is_below(p)) {
        out.insert(q.clone(), Value::Unset);
    }
    out.insert(p.clone(), Value::Unset);
}

/// The leaf effects of changing `path` from what the writer sees there to `n` (the model's
/// `eff_of`): type-strict, only changed leaves; a sub-document sets its changed leaves, unsets
/// the old fields it lacks, and writes its marker when the path was not a map before or
/// anything below it was written; a scalar or an array over a map unsets what was seen below.
fn eff_of(old: &Doc, path: &Path, n: &Value, out: &mut BTreeMap<Path, Value>) {
    let o = old.get(path);
    let was_map = o == Some(&Value::Map);
    if let Value::Document(fields) = n {
        let before = out.len();
        for (name, v) in fields {
            eff_of(old, &path.child(name).expect("a valid name"), v, out);
        }
        if was_map {
            let kept: BTreeSet<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
            let gone: Vec<Path> = old
                .keys()
                .filter(|q| q.is_below(path) && q.segments().len() == path.segments().len() + 1)
                .filter(|q| !kept.contains(q.name()))
                .cloned()
                .collect();
            for q in gone {
                unset_all(old, &q, out);
            }
        }
        if !was_map || out.len() > before {
            out.insert(path.clone(), Value::Map);
        }
    } else if was_map {
        unset_all(old, path, out);
        out.insert(path.clone(), n.clone());
    } else if !o.is_some_and(|o| ident(o, n)) {
        out.insert(path.clone(), n.clone());
    }
}

/// The cells an inserted document stands for: a marker at each map and a cell per leaf.
fn flatten_doc(fields: &[(String, Value)], at: Option<&Path>, out: &mut BTreeMap<Path, Value>) {
    for (name, v) in fields {
        let q = at.map_or_else(|| Path::flat(name), |a| a.child(name)).expect("a valid name");
        if let Value::Document(inner) = v {
            out.insert(q.clone(), Value::Map);
            flatten_doc(inner, Some(&q), out);
        } else {
            out.insert(q, v.clone());
        }
    }
}

/// The effects `intent` must have against the document `old` the writer sees (`None`: no life
/// is alive). `Inc` is not covered beyond its refusals (its effect is a counter slot).
pub fn expected_effects(old: Option<&Doc>, intent: &Intent) -> Expected {
    let mut out = BTreeMap::new();
    match (old, intent) {
        (None, Intent::Insert(doc)) => flatten_doc(doc, None, &mut out),
        (Some(_), Intent::Insert(_)) | (None, _) => return Expected::Refused,
        (Some(old), Intent::Set(p, v)) => {
            if below_scalar(old, p) {
                return Expected::Refused;
            }
            implicit_markers(old, p, &mut out);
            eff_of(old, p, v, &mut out);
        }
        (Some(old), Intent::Unset(p)) => {
            if below_scalar(old, p) {
                return Expected::Refused;
            }
            if old.contains_key(p) {
                unset_all(old, p, &mut out);
            }
        }
        (Some(old), Intent::Inc(p)) => {
            if below_scalar(old, p) {
                return Expected::Refused;
            }
        }
        (Some(old), Intent::Replace(doc)) => {
            for (name, v) in doc {
                eff_of(old, &Path::flat(name).expect("a valid name"), v, &mut out);
            }
            let tops: Vec<&Path> = old.keys().filter(|q| q.segments().len() == 1).collect();
            for q in tops {
                if !doc.iter().any(|(name, _)| name == q.name()) {
                    unset_all(old, q, &mut out);
                }
            }
        }
    }
    Expected::Effects(out)
}

/// Check one write against its intent: `old` is the document the writer saw, `got` what the
/// writer did (`None`: refused, or a no-op). `Err` names the disagreement.
pub fn check_write(
    old: Option<&Doc>,
    intent: &Intent,
    got: Option<&WriteRecord>,
) -> Result<(), String> {
    let want = expected_effects(old, intent);
    match (&want, got, intent) {
        // an $inc: refused where the rule says so, otherwise the writer decides (type, bound)
        (Expected::Refused, Some(_), _) => Err(format!("{intent:?} was accepted, want a refusal")),
        // an increment that made a slot also made the implicit markers of the maps it created (one
        // write); a double base is a register write, whose cells are the writer's business
        (_, Some(r), Intent::Inc(p)) if r.kind == WriteKind::Inc => {
            let mut markers = BTreeMap::new();
            if let Some(old) = old {
                implicit_markers(old, p, &mut markers);
            }
            (r.vals == markers && r.paths == [p.clone()]).then_some(()).ok_or(format!(
                "{intent:?} wrote {:?} at {:?}, want the markers {markers:?} at its path alone",
                r.vals, r.paths
            ))
        }
        (_, _, Intent::Inc(_)) => Ok(()),
        // a no-op is no write; only a write of a sub-document or a replace can change the ORDER of the
        // fields and nothing else, and then it is a write with no cell
        (Expected::Effects(e), Some(_), Intent::Unset(_)) if e.is_empty() => {
            Err(format!("{intent:?} changes nothing and was written"))
        }
        (Expected::Effects(e), Some(_), Intent::Set(_, v))
            if e.is_empty() && !matches!(v, Value::Document(_)) =>
        {
            Err(format!("{intent:?} changes nothing and was written"))
        }
        (Expected::Effects(e), None, _) if e.is_empty() => Ok(()),
        (Expected::Effects(e), None, _) => Err(format!("{intent:?} was refused, want {e:?}")),
        (Expected::Effects(e), Some(r), _) => {
            let ok = r.kind
                == if matches!(intent, Intent::Insert(_)) {
                    WriteKind::Insert
                } else {
                    WriteKind::Reg
                }
                && r.vals.len() == e.len()
                && r.paths.len() == e.len()
                && e.iter().all(|(q, v)| r.vals.get(q).is_some_and(|x| ident(x, v)));
            ok.then_some(()).ok_or(format!("{intent:?} wrote {:?}, want {e:?}", r.vals))
        }
        (Expected::Refused, None, _) => Ok(()),
    }
}
