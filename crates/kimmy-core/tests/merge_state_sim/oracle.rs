//! The oracle: independent of the state machinery, computed from the global
//! causal log (model5.py after "oracle: independent of the state machinery").
//! Names and structure follow the Python so the two can be diffed.
//!
//! The log maps a write's stamp to `(ctx, meta)`: the stamps of the writes in
//! its causal past and what the write did. Every function that reads the
//! model's global `RET` takes it as `ret`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::merge_state::{LossKey, LossKind, MStamp, Value, WriteKind, WriteRecord, WriterId};

pub type Log = BTreeMap<MStamp, (BTreeSet<MStamp>, WriteRecord)>;
pub type Doc = BTreeMap<String, Value>;
/// A head that must be, or is, reported: `(path, stamp)`.
pub type Head = (String, MStamp);

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

/// An explicit insert (insertOne/insertMany), not the creation write of an upsert.
fn _explicit(m: &WriteRecord) -> bool {
    m.kind == WriteKind::Insert && !m.upsert
}

/// V6m2/V7m1/V7m2: a `$setOnInsert` cell of an upsert creation is dropped when
/// an explicit insert (a first insert or a re-insert after a delete) is alive.
fn _supp(log: &Log, m: &WriteRecord, p: &str, alive: &BTreeSet<MStamp>) -> bool {
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

fn all_paths(log: &Log) -> BTreeSet<String> {
    log.values().flat_map(|(_, m)| m.paths.iter().cloned()).collect()
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
    memo: RefCell<BTreeMap<(MStamp, String), Option<MStamp>>>,
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
    pub fn of(&self, st: MStamp, p: &str) -> Option<MStamp> {
        if let Some(g) = self.memo.borrow().get(&(st, p.to_string())) {
            return *g;
        }
        let (ctx, m) = &self.log[&st];
        let g = if is_reg_kind(m.kind) && !m.vals[p].is_numeric() {
            Some(st)
        } else {
            let al = _alive_at(self.log, ctx, &self.ins, &self.dels, self.ret);
            let cands = ctx.iter().filter(|w| {
                let m2 = &self.log[*w].1;
                is_reg_kind(m2.kind)
                    && m2.paths.iter().any(|q| q == p)
                    && in_set(&m2.life, &al)
                    && !_supp(self.log, m2, p, &al)
            });
            cands.max().and_then(|w| self.of(*w, p))
        };
        self.memo.borrow_mut().insert((st, p.to_string()), g);
        g
    }
}

/// `gi == gW or (gi is not None and (gW is None or gi > gW))`: the generation
/// is the winner's or a NEWER one.
fn gen_counts(gi: Option<MStamp>, g_w: Option<MStamp>) -> bool {
    gi == g_w || (gi.is_some() && (g_w.is_none() || gi > g_w))
}

/// Returns (visible-doc or None, alive lives, deletes). The value of a path, in
/// words: the winning base (the highest-stamped register write in an alive
/// life), plus every increment that the winner's writer had not seen, except
/// the increments the winner's generation overwrote (an increment made in an
/// OLDER generation than the winner's, and not seen by the starter of the
/// winner's generation). Increments made in a NEWER generation are kept even
/// when its starter lost.
///
/// `bound` is the integer bound of the run (`JoinOptions::int_bound`, the real Int64 bound unless
/// a run lowers it so that the clamp is exercised): a total saturates to `[-bound - 1, bound]`.
pub fn oracle(log: &Log, ret: u64, bound: i64) -> (Option<Doc>, BTreeSet<MStamp>, Vec<MStamp>) {
    let b = i128::from(bound);
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);

    let writes_on =
        |p: &str, kinds: &[WriteKind]| -> Vec<(MStamp, &BTreeSet<MStamp>, &WriteRecord)> {
            log.iter()
                .filter(|(_, (_, m))| {
                    kinds.contains(&m.kind) && m.paths.iter().any(|q| q == p) && m.life.is_some()
                })
                .map(|(st, (ctx, m))| (*st, ctx, m))
                .collect()
        };

    let mut doc = Doc::new();
    for p in all_paths(log) {
        let regs: Vec<_> = writes_on(&p, &[WriteKind::Insert, WriteKind::Reg])
            .into_iter()
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
            Some(Value::Int(i)) => Some(i128::from(*i)),
            _ => None,
        };
        let empty = BTreeSet::new();
        let wctx = win.map_or(&empty, |w| w.1);
        let extra: Vec<i64> =
            counted.filter(|(st, _, _)| !wctx.contains(st)).map(|(_, _, m)| m.n.unwrap()).collect();
        if base.is_none() && extra.is_empty() {
            continue;
        }
        let total = base.unwrap_or(0) + extra.iter().map(|n| i128::from(*n)).sum::<i128>();
        doc.insert(p, Value::Int(total.clamp(-b - 1, b) as i64));
    }
    ((!alive.is_empty()).then_some(doc), alive, dels)
}

/// Heads that must be reported, as (path, stamp): a head needs a record of ANY
/// kind (V3m2).
pub fn loss_oracle(
    log: &Log,
    alive: &BTreeSet<MStamp>,
    dels: &[MStamp],
    ret: u64,
) -> BTreeSet<Head> {
    let mut must: BTreeSet<Head> = BTreeSet::new();
    let gener = make_gen(log, ret);
    let sup = superseded_lives(log, None);
    let killed = killed_lives(log, ret);
    let sup_live: BTreeSet<MStamp> = sup.difference(&killed).copied().collect();
    for l in &sup_live {
        must.insert(("_".to_string(), *l));
    }
    // V6M1: every head written into a losing first insert is reported
    for (st, (_, m)) in log {
        if in_set(&m.life, &sup_live)
            && matches!(m.kind, WriteKind::Reg | WriteKind::Inc)
            && Some(*st) != m.life
        {
            for p in &m.paths {
                let later = log
                    .values()
                    .any(|(c2, m2)| c2.contains(st) && m2.paths.contains(p) && m2.life == m.life);
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
                is_reg_kind(m.kind)
                    && m.paths.contains(&p)
                    && in_set(&m.life, alive)
                    && !_supp(log, m, &p, alive)
            })
            .map(|(st, (ctx, m))| (*st, ctx, m))
            .collect();
        let heads: Vec<_> =
            regs.iter().filter(|r| !regs.iter().any(|r2| r2.1.contains(&r.0))).collect();
        if let Some(top) = heads.iter().max_by_key(|r| r.0) {
            for h in &heads {
                if h.0 != top.0 {
                    must.insert((p.clone(), h.0));
                }
            }
        }
        let dead_regs: Vec<(MStamp, &BTreeSet<MStamp>, &WriteRecord)> = log
            .iter()
            .filter(|(st, (_, m))| {
                is_reg_kind(m.kind)
                    && m.paths.contains(&p)
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
pub fn hidden_oracle(log: &Log, ret: u64) -> BTreeMap<String, i128> {
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);
    let mut out = BTreeMap::new();
    for p in all_paths(log) {
        let regs: Vec<MStamp> = log
            .iter()
            .filter(|(_, (_, m))| {
                is_reg_kind(m.kind)
                    && m.paths.contains(&p)
                    && in_set(&m.life, &alive)
                    && !_supp(log, m, &p, &alive)
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
) -> Vec<(LossKind, String, MStamp)> {
    let ins = creators(log);
    let dels = deletes(log);
    let alive = _alive_lives(log, &ins, &dels, ret);
    let gener = make_gen(log, ret);
    let mut bad = Vec::new();
    for k in records {
        let (kind, p, st) = (k.kind, &k.path, k.stamp);
        let m = &log[&st].1;
        if m.kind != WriteKind::Inc || !in_set(&m.life, &alive) {
            continue;
        }
        let regs: Vec<MStamp> = log
            .iter()
            .filter(|(_, (_, m2))| {
                is_reg_kind(m2.kind)
                    && m2.paths.contains(p)
                    && in_set(&m2.life, &alive)
                    && !_supp(log, m2, p, &alive)
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
            set.insert(("_".to_string(), l));
            set.extend(m.paths.iter().map(|p| (p.clone(), l)));
            out.push(set);
        }
    }
    out
}

pub fn sound_record(key: &LossKey, log: &Log, alive: &BTreeSet<MStamp>, dels: &[MStamp]) -> bool {
    let (kind, p, st) = (key.kind, &key.path, key.stamp);
    let Some((ctx, m)) = log.get(&st) else { return false };
    match kind {
        LossKind::Clash => log.iter().any(|(w, (c2, m2))| {
            *w != st
                && *w > st
                && !c2.contains(&st)
                && !ctx.contains(w)
                && m2.paths.contains(p)
                && is_reg_kind(m2.kind)
                && is_reg_kind(m.kind)
        }),
        LossKind::InsertLost => {
            let sup = superseded_lives(log, None);
            if p == "_" {
                return sup.contains(&st);
            }
            in_set(&m.life, &sup) || (m.kind == WriteKind::Insert && m.soi.contains(p))
        }
        LossKind::DeleteWon => {
            !in_set(&m.life, alive) && dels.iter().any(|d| !log[d].0.contains(&st))
        }
        LossKind::RetiredWriter => false,
    }
}
