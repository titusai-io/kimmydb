//! The state a window slides: one [`Slider`] per accumulator (ADR-264).
//!
//! Both ends of a window only move forward as the current document does, so a
//! slider is told each document once when it enters (`add`) and once when it
//! leaves (`remove`), in the same order, and then asked for the answer over the
//! documents it holds (`value`). Nothing is rebuilt from the documents of the
//! window to answer, whatever the accumulator, except the periodic rebuild that
//! bounds the drift of a subtracted floating-point deviation and the one corner of
//! a double total that overflows:
//!
//! | Accumulator | State | A step costs |
//! |---|---|---|
//! | `$sum`, `$avg` | an exact 128-bit integer total, an exact sum of the doubles, counts of NaN and the infinities, and, once a `Decimal128` has been seen, an exact decimal total | O(1) (doubles: a few steps) |
//! | `$min`, `$max` | a monotone deque | O(1) amortized |
//! | `$push`, `$firstN`, `$lastN` | a queue of the window's values | O(1) |
//! | `$addToSet` | a queue of the positions of each member and the members in order of first position | O(log w) |
//! | `$top`, `$bottom`, `$topN`, `$bottomN`, `$maxN`, `$minN` | the window's rows in an ordered set | O(log w) |
//! | `$stdDevPop`, `$stdDevSamp`, `$covariancePop`, `$covarianceSamp` | running co-moments, finite values only, and a count of the non-finite ones | O(1) |
//!
//! Answering costs what the answer holds: a `$push` over a window of `w` documents
//! is an array of `w`, which is charged to the stage's byte budget as it is made.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use bson::{Bson, Document};
use kimmy_core::Result;
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::decimal::arith::{self, RunningSum};
use kimmy_core::decimal::{Raw, decode_raw, pack_raw};

use crate::accumulators::{Best, Kind, Row, Spec, Welford, numeric, sort_key};
use crate::aggregate::group_key;
use crate::expr::{Binding, Expr, Mean, Scope, Total};

/// Evaluate `expr` against `doc`; a missing field is `null`.
pub(crate) fn eval(expr: &Expr, doc: &Document, vars: &[Binding<'_>]) -> Result<Bson> {
    expr.eval_in(&Scope::with_bindings(doc, vars))
}

/// Evaluate `expr` against `doc`; `None` when there is no value (a missing field).
pub(crate) fn eval_present(
    expr: &Expr,
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<Option<Bson>> {
    expr.eval_present(&Scope::with_bindings(doc, vars))
}

/// A running state over a window that moves forward.
pub(crate) trait Slider {
    /// The document at `i` enters the window.
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()>;
    /// The document at `i`, the oldest held, leaves.
    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()>;
    /// Hold nothing, the window being empty and starting at `at`.
    fn reset(&mut self, at: usize);
    /// The answer over the window `[s, e)`, which the state holds.
    fn value(&mut self, docs: &[Document], s: usize, e: usize) -> Result<Bson>;
}

// ---------------------------------------------------------------------------
// An exact sum of doubles
// ---------------------------------------------------------------------------

/// The exact sum of finite doubles, in non-overlapping partials (Shewchuk), so that a
/// double can leave by adding its negation, and the total is rounded once.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExactDoubles {
    partials: Vec<f64>,
    /// A partial overflowed: the total is unreliable until the sum is emptied.
    pub(crate) overflowed: bool,
}

impl ExactDoubles {
    pub(crate) fn add(&mut self, x: f64) {
        let mut x = x;
        let mut kept = 0;
        for j in 0..self.partials.len() {
            let mut y = self.partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            if !hi.is_finite() {
                self.overflowed = true;
                return;
            }
            let lo = y - (hi - x);
            if lo != 0.0 {
                self.partials[kept] = lo;
                kept += 1;
            }
            x = hi;
        }
        self.partials.truncate(kept);
        self.partials.push(x);
    }

    pub(crate) fn clear(&mut self) {
        self.partials.clear();
        self.overflowed = false;
    }

    /// The sum rounded once to the nearest double.
    pub(crate) fn value(&self) -> f64 {
        let p = &self.partials;
        let mut n = p.len();
        if n == 0 {
            return 0.0;
        }
        n -= 1;
        let mut hi = p[n];
        let mut lo = 0.0;
        while n > 0 {
            let x = hi;
            n -= 1;
            let y = p[n];
            hi = x + y;
            let yr = hi - x;
            lo = y - yr;
            if lo != 0.0 {
                break;
            }
        }
        // Round half to even when the dropped part is exactly half.
        if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            let yr = x - hi;
            if y == yr {
                hi = x;
            }
        }
        hi
    }

    /// The sum with the integer `ints` added, rounded once.
    pub(crate) fn value_with(&self, ints: i128) -> f64 {
        let mut all = self.clone();
        // An integer of up to 2^127 as two doubles that add up to it exactly.
        let hi = ints as f64;
        let lo = (ints - hi as i128) as f64;
        all.add(hi);
        all.add(lo);
        all.value()
    }
}

// ---------------------------------------------------------------------------
// $sum and $avg
// ---------------------------------------------------------------------------

/// The numbers of a window, kept in the three ways their sum can be asked for.
#[derive(Clone, Debug, Default)]
struct Plain {
    ints: i128,
    n_int: usize,
    doubles: ExactDoubles,
    n_dbl: usize,
    nan: usize,
    pos_inf: usize,
    neg_inf: usize,
}

impl Plain {
    fn apply(&mut self, v: &Bson, adding: bool) {
        let step = |n: &mut usize| {
            if adding {
                *n += 1;
            } else {
                *n = n.saturating_sub(1);
            }
        };
        match v {
            Bson::Int32(_) | Bson::Int64(_) => {
                let x = i128::from(match v {
                    Bson::Int32(n) => i64::from(*n),
                    Bson::Int64(n) => *n,
                    _ => 0,
                });
                self.ints += if adding { x } else { -x };
                step(&mut self.n_int);
            }
            Bson::Double(d) if d.is_nan() => step(&mut self.nan),
            Bson::Double(d) if *d == f64::INFINITY => step(&mut self.pos_inf),
            Bson::Double(d) if *d == f64::NEG_INFINITY => step(&mut self.neg_inf),
            Bson::Double(d) => {
                self.doubles.add(if adding { *d } else { -*d });
                step(&mut self.n_dbl);
                if self.n_dbl == 0 {
                    // Nothing is left: forget the rounding dust and any overflow.
                    self.doubles.clear();
                }
            }
            _ => {}
        }
    }

    fn count(&self) -> usize {
        self.n_int + self.n_dbl + self.nan + self.pos_inf + self.neg_inf
    }

    /// NaN or an infinity if the window holds one, as a sum of its numbers would be.
    fn special(&self) -> Option<f64> {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            Some(f64::NAN)
        } else if self.pos_inf > 0 {
            Some(f64::INFINITY)
        } else if self.neg_inf > 0 {
            Some(f64::NEG_INFINITY)
        } else {
            None
        }
    }
}

/// The exact decimal term of a number, as `$sum` converts it once a `Decimal128` is
/// in play: an integer exactly, a double by its exact binary value.
fn decimal_term(v: &Bson) -> Option<Raw> {
    match v {
        Bson::Int32(n) => Some(arith::from_i64(i64::from(*n))),
        Bson::Int64(n) => Some(arith::from_i64(*n)),
        Bson::Double(d) => Some(arith::from_binary64(d.to_bits()).0),
        Bson::Decimal128(d) => Some(decode_raw(d)),
        _ => None,
    }
}

/// `$sum` and `$avg` over a window: no document is revisited to answer.
pub(crate) struct SumSlider<'a, 'v> {
    pub(crate) expr: &'a Expr,
    pub(crate) avg: bool,
    pub(crate) vars: &'a [Binding<'v>],
    plain: Plain,
    /// Every number of the window as a decimal; built when a `Decimal128` first enters.
    dec: Option<RunningSum>,
    /// `Decimal128`s in the window now.
    dec_n: usize,
    lo: usize,
    hi: usize,
}

impl<'a, 'v> SumSlider<'a, 'v> {
    pub(crate) fn new(expr: &'a Expr, avg: bool, vars: &'a [Binding<'v>]) -> Self {
        SumSlider { expr, avg, vars, plain: Plain::default(), dec: None, dec_n: 0, lo: 0, hi: 0 }
    }
}

impl Slider for SumSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.vars)?;
        if matches!(v, Bson::Decimal128(_)) {
            self.dec_n += 1;
            if self.dec.is_none() {
                // The first decimal: bring in the numbers already in the window.
                let mut sum = RunningSum::new();
                for doc in &docs[self.lo..self.hi] {
                    if let Some(t) = decimal_term(&eval(self.expr, doc, self.vars)?) {
                        sum.add(&t);
                    }
                }
                self.dec = Some(sum);
            }
        } else {
            self.plain.apply(&v, true);
        }
        if let (Some(sum), Some(t)) = (&mut self.dec, decimal_term(&v)) {
            sum.add(&t);
        }
        self.hi = i + 1;
        Ok(())
    }

    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.vars)?;
        if matches!(v, Bson::Decimal128(_)) {
            self.dec_n = self.dec_n.saturating_sub(1);
        } else {
            self.plain.apply(&v, false);
        }
        if let (Some(sum), Some(t)) = (&mut self.dec, decimal_term(&v)) {
            sum.remove(&t);
        }
        self.lo = i + 1;
        Ok(())
    }

    fn reset(&mut self, at: usize) {
        self.plain = Plain::default();
        self.dec = None;
        self.dec_n = 0;
        self.lo = at;
        self.hi = at;
    }

    fn value(&mut self, docs: &[Document], s: usize, e: usize) -> Result<Bson> {
        if self.dec_n > 0
            && let Some(sum) = &self.dec
        {
            let raw = if self.avg { sum.finish_avg() } else { sum.finish_sum() };
            return Ok(Bson::Decimal128(pack_raw(raw)));
        }
        let p = &self.plain;
        if p.doubles.overflowed {
            // A total past the range of a double: the group accumulator answers it.
            return if self.avg {
                let mut m = Mean::default();
                for doc in &docs[s..e] {
                    m.add(&eval(self.expr, doc, self.vars)?);
                }
                Ok(m.to_bson())
            } else {
                let mut t = Total::default();
                for doc in &docs[s..e] {
                    t.add(&eval(self.expr, doc, self.vars)?);
                }
                Ok(t.to_bson())
            };
        }
        let count = p.count();
        if self.avg && count == 0 {
            return Ok(Bson::Null);
        }
        let total = if let Some(special) = p.special() {
            special
        } else if p.n_dbl == 0 {
            // Integers only.
            if self.avg {
                p.ints as f64
            } else {
                return Ok(match i64::try_from(p.ints) {
                    Ok(n) => Bson::Int64(n),
                    Err(_) => Bson::Double(p.ints as f64),
                });
            }
        } else {
            p.doubles.value_with(p.ints)
        };
        if self.avg {
            return Ok(Bson::Double(total / count as f64));
        }
        Ok(Bson::Double(total))
    }
}

// ---------------------------------------------------------------------------
// $min and $max
// ---------------------------------------------------------------------------

/// `$min` and `$max`: the candidates that could still be the answer, best first. `$min`
/// keeps the first of equal values and `$max` the last, as the reference does.
pub(crate) struct ExtremeSlider<'a, 'v> {
    pub(crate) expr: &'a Expr,
    pub(crate) max: bool,
    pub(crate) vars: &'a [Binding<'v>],
    deque: VecDeque<(usize, Bson)>,
}

impl<'a, 'v> ExtremeSlider<'a, 'v> {
    pub(crate) fn new(expr: &'a Expr, max: bool, vars: &'a [Binding<'v>]) -> Self {
        ExtremeSlider { expr, max, vars, deque: VecDeque::new() }
    }
}

impl Slider for ExtremeSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.vars)?;
        if matches!(v, Bson::Null) {
            return Ok(());
        }
        while let Some((_, back)) = self.deque.back() {
            let o = canonical_cmp(back, &v);
            // A candidate the newcomer beats can never be the answer again; an equal
            // one goes too for `$max` (the last wins) and stays for `$min`.
            if (self.max && o.is_le()) || (!self.max && o.is_gt()) {
                self.deque.pop_back();
            } else {
                break;
            }
        }
        self.deque.push_back((i, v));
        Ok(())
    }

    fn remove(&mut self, i: usize, _: &[Document]) -> Result<()> {
        if self.deque.front().is_some_and(|(at, _)| *at == i) {
            self.deque.pop_front();
        }
        Ok(())
    }

    fn reset(&mut self, _: usize) {
        self.deque.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        Ok(self.deque.front().map_or(Bson::Null, |(_, v)| v.clone()))
    }
}

// ---------------------------------------------------------------------------
// $push, $firstN, $lastN
// ---------------------------------------------------------------------------

/// Which queue of values.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Queue {
    /// `$push`: every present value of the window.
    Push,
    /// `$firstN`: the first `n` values, `null` for a missing one.
    First,
    /// `$lastN`: the last `n` values, `null` for a missing one.
    Last,
}

/// The values of the window in order.
pub(crate) struct QueueSlider<'a, 'v> {
    pub(crate) expr: &'a Expr,
    pub(crate) kind: Queue,
    pub(crate) n: usize,
    pub(crate) vars: &'a [Binding<'v>],
    live: VecDeque<(usize, Bson)>,
}

impl<'a, 'v> QueueSlider<'a, 'v> {
    pub(crate) fn new(expr: &'a Expr, kind: Queue, n: usize, vars: &'a [Binding<'v>]) -> Self {
        QueueSlider { expr, kind, n, vars, live: VecDeque::new() }
    }
}

impl Slider for QueueSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let value = match self.kind {
            Queue::Push => eval_present(self.expr, &docs[i], self.vars)?,
            _ => Some(eval(self.expr, &docs[i], self.vars)?),
        };
        if let Some(v) = value {
            self.live.push_back((i, v));
        }
        Ok(())
    }

    fn remove(&mut self, i: usize, _: &[Document]) -> Result<()> {
        if self.live.front().is_some_and(|(at, _)| *at == i) {
            self.live.pop_front();
        }
        Ok(())
    }

    fn reset(&mut self, _: usize) {
        self.live.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        let values = self.live.iter().map(|(_, v)| v.clone());
        Ok(Bson::Array(match self.kind {
            Queue::Push => values.collect(),
            Queue::First => values.take(self.n).collect(),
            Queue::Last => {
                let skip = self.live.len().saturating_sub(self.n);
                values.skip(skip).collect()
            }
        }))
    }
}

// ---------------------------------------------------------------------------
// $addToSet
// ---------------------------------------------------------------------------

/// The distinct values of the window, in the order of their first position in it.
pub(crate) struct SetSlider<'a, 'v> {
    pub(crate) expr: &'a Expr,
    pub(crate) vars: &'a [Binding<'v>],
    /// The window's present values by position.
    live: BTreeMap<usize, Bson>,
    /// The positions each distinct value holds, oldest first.
    positions: HashMap<Vec<u8>, VecDeque<usize>>,
    /// The first position of each distinct value.
    firsts: BTreeSet<usize>,
}

impl<'a, 'v> SetSlider<'a, 'v> {
    pub(crate) fn new(expr: &'a Expr, vars: &'a [Binding<'v>]) -> Self {
        SetSlider {
            expr,
            vars,
            live: BTreeMap::new(),
            positions: HashMap::new(),
            firsts: BTreeSet::new(),
        }
    }
}

impl Slider for SetSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        if let Some(v) = eval_present(self.expr, &docs[i], self.vars)? {
            let queue = self.positions.entry(group_key(&v)).or_default();
            if queue.is_empty() {
                self.firsts.insert(i);
            }
            queue.push_back(i);
            self.live.insert(i, v);
        }
        Ok(())
    }

    fn remove(&mut self, i: usize, _: &[Document]) -> Result<()> {
        if let Some(v) = self.live.remove(&i) {
            let key = group_key(&v);
            if let Some(queue) = self.positions.get_mut(&key) {
                queue.pop_front();
                self.firsts.remove(&i);
                match queue.front() {
                    Some(next) => {
                        self.firsts.insert(*next);
                    }
                    None => {
                        self.positions.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    fn reset(&mut self, _: usize) {
        self.live.clear();
        self.positions.clear();
        self.firsts.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        Ok(Bson::Array(self.firsts.iter().map(|p| self.live[p].clone()).collect()))
    }
}

// ---------------------------------------------------------------------------
// $top, $bottom, $topN, $bottomN, $maxN, $minN
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RankRow(Arc<Row>);

impl PartialEq for RankRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.cmp(&other.0).is_eq()
    }
}
impl Eq for RankRow {}
impl PartialOrd for RankRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// `$top`, `$bottom`, `$topN` and `$bottomN`: the window's rows in rank order.
pub(crate) struct RankedSlider<'a, 'v> {
    spec: &'a Spec,
    vars: &'a [Binding<'v>],
    n: usize,
    single: bool,
    bottom: bool,
    dirs: Arc<[bool]>,
    set: BTreeSet<RankRow>,
    fifo: VecDeque<Arc<Row>>,
    seq: u64,
}

impl<'a, 'v> RankedSlider<'a, 'v> {
    /// `docs[0]` is read for `n`, which is one number for the whole partition.
    pub(crate) fn new(spec: &'a Spec, docs: &[Document], vars: &'a [Binding<'v>]) -> Result<Self> {
        let single = matches!(spec.kind, Kind::Top | Kind::Bottom);
        let n = if single { 1 } else { spec.resolve(&Scope::with_bindings(&docs[0], vars))? };
        Ok(RankedSlider {
            spec,
            vars,
            n,
            single,
            bottom: matches!(spec.kind, Kind::Bottom | Kind::BottomN),
            dirs: spec.dirs(),
            set: BTreeSet::new(),
            fifo: VecDeque::new(),
            seq: 0,
        })
    }
}

impl Slider for RankedSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let keys = self.spec.sort.iter().map(|k| sort_key(&docs[i], &k.path)).collect();
        let output = eval(&self.spec.value, &docs[i], self.vars)?;
        let row = Arc::new(Row { keys, output, seq: self.seq, dirs: Arc::clone(&self.dirs) });
        self.seq += 1;
        self.set.insert(RankRow(Arc::clone(&row)));
        self.fifo.push_back(row);
        Ok(())
    }

    fn remove(&mut self, _: usize, _: &[Document]) -> Result<()> {
        if let Some(row) = self.fifo.pop_front() {
            self.set.remove(&RankRow(row));
        }
        Ok(())
    }

    fn reset(&mut self, _: usize) {
        self.set.clear();
        self.fifo.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        let mut rows: Vec<Arc<Row>> = self.set.iter().take(self.n).map(|r| r.0.clone()).collect();
        // `$bottom*` ranked by the reverse order to choose its rows and answers them in
        // the order asked for; equal rows stay in order of arrival either way.
        if self.bottom {
            rows.sort_by(|a, b| a.cmp_rank(b, true));
        }
        if self.single {
            return Ok(rows.first().map_or(Bson::Null, |r| r.output.clone()));
        }
        Ok(Bson::Array(rows.into_iter().map(|r| r.output.clone()).collect()))
    }
}

#[derive(Clone)]
struct BestRow(Arc<Best>);

impl PartialEq for BestRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.cmp(&other.0).is_eq()
    }
}
impl Eq for BestRow {}
impl PartialOrd for BestRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for BestRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// `$maxN` and `$minN`: the window's values, best first.
pub(crate) struct BestSlider<'a, 'v> {
    expr: &'a Expr,
    vars: &'a [Binding<'v>],
    n: usize,
    max: bool,
    set: BTreeSet<BestRow>,
    fifo: VecDeque<(usize, Arc<Best>)>,
    seq: u64,
}

impl<'a, 'v> BestSlider<'a, 'v> {
    pub(crate) fn new(spec: &'a Spec, docs: &[Document], vars: &'a [Binding<'v>]) -> Result<Self> {
        Ok(BestSlider {
            expr: &spec.value,
            vars,
            n: spec.resolve(&Scope::with_bindings(&docs[0], vars))?,
            max: spec.kind == Kind::MaxN,
            set: BTreeSet::new(),
            fifo: VecDeque::new(),
            seq: 0,
        })
    }
}

impl Slider for BestSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let value = eval(self.expr, &docs[i], self.vars)?;
        // `$maxN` and `$minN` skip null and missing.
        if !matches!(value, Bson::Null | Bson::Undefined) {
            let best = Arc::new(Best { value, seq: self.seq, max: self.max });
            self.seq += 1;
            self.set.insert(BestRow(Arc::clone(&best)));
            self.fifo.push_back((i, best));
        }
        Ok(())
    }

    fn remove(&mut self, i: usize, _: &[Document]) -> Result<()> {
        if self.fifo.front().is_some_and(|(at, _)| *at == i)
            && let Some((_, best)) = self.fifo.pop_front()
        {
            self.set.remove(&BestRow(best));
        }
        Ok(())
    }

    fn reset(&mut self, _: usize) {
        self.set.clear();
        self.fifo.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        Ok(Bson::Array(self.set.iter().take(self.n).map(|b| b.0.value.clone()).collect()))
    }
}

// ---------------------------------------------------------------------------
// $stdDevPop, $stdDevSamp, $covariancePop, $covarianceSamp
// ---------------------------------------------------------------------------

/// Removals after which a subtracted deviation is rebuilt from the window, at least this
/// many and at least as many as the window holds, so a rebuild costs a document per step
/// on average and the drift of subtracting is bounded.
const REBUILD_AFTER_REMOVALS: usize = 1024;

/// `$stdDevPop` and `$stdDevSamp`. A non-finite value is counted, not folded in, so none
/// can leave a mean that is NaN for good behind it.
pub(crate) struct StdDevSlider<'a, 'v> {
    pub(crate) expr: &'a Expr,
    pub(crate) sample: bool,
    pub(crate) vars: &'a [Binding<'v>],
    w: Welford,
    nonfinite: usize,
    removals: usize,
    lo: usize,
    hi: usize,
}

impl<'a, 'v> StdDevSlider<'a, 'v> {
    pub(crate) fn new(expr: &'a Expr, sample: bool, vars: &'a [Binding<'v>]) -> Self {
        StdDevSlider {
            expr,
            sample,
            vars,
            w: Welford::default(),
            nonfinite: 0,
            removals: 0,
            lo: 0,
            hi: 0,
        }
    }

    fn number(&self, i: usize, docs: &[Document]) -> Result<Option<f64>> {
        Ok(numeric(&eval(self.expr, &docs[i], self.vars)?))
    }
}

impl Slider for StdDevSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        match self.number(i, docs)? {
            Some(x) if x.is_finite() => self.w.add(x),
            Some(_) => self.nonfinite += 1,
            None => {}
        }
        self.hi = i + 1;
        Ok(())
    }

    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        match self.number(i, docs)? {
            Some(x) if x.is_finite() => {
                self.w.remove(x);
                self.removals += 1;
            }
            Some(_) => self.nonfinite = self.nonfinite.saturating_sub(1),
            None => {}
        }
        self.lo = i + 1;
        Ok(())
    }

    fn reset(&mut self, at: usize) {
        self.w = Welford::default();
        self.nonfinite = 0;
        self.removals = 0;
        self.lo = at;
        self.hi = at;
    }

    fn value(&mut self, docs: &[Document], _: usize, _: usize) -> Result<Bson> {
        if self.removals >= REBUILD_AFTER_REMOVALS.max(self.hi - self.lo) {
            // Subtracting drifts a little each time; start again from the window.
            self.w = Welford::default();
            for i in self.lo..self.hi {
                if let Some(x) = self.number(i, docs)?
                    && x.is_finite()
                {
                    self.w.add(x);
                }
            }
            self.removals = 0;
        }
        let n = self.w.n + self.nonfinite as u64;
        if self.nonfinite > 0 {
            // What `$group` answers over these documents: NaN, and null for one value
            // of a sample.
            return Ok(if self.sample && n == 1 { Bson::Null } else { Bson::Double(f64::NAN) });
        }
        Ok(match (n, self.sample) {
            (0, _) => Bson::Null,
            // The reference answers an integer zero for one value.
            (1, false) => Bson::Int32(0),
            _ => self.w.finish(self.sample),
        })
    }
}

/// Running co-moments of pairs of numbers.
#[derive(Clone, Debug, Default)]
struct CoMoment {
    n: u64,
    mean_x: f64,
    mean_y: f64,
    c: f64,
}

impl CoMoment {
    fn add(&mut self, x: f64, y: f64) {
        self.n += 1;
        let dx = x - self.mean_x;
        self.mean_x += dx / self.n as f64;
        self.mean_y += (y - self.mean_y) / self.n as f64;
        self.c += dx * (y - self.mean_y);
    }

    fn remove(&mut self, x: f64, y: f64) {
        if self.n <= 1 {
            *self = CoMoment::default();
            return;
        }
        let n = self.n as f64;
        let mean_x = (n * self.mean_x - x) / (n - 1.0);
        self.c -= (x - mean_x) * (y - self.mean_y);
        self.mean_y = (n * self.mean_y - y) / (n - 1.0);
        self.mean_x = mean_x;
        self.n -= 1;
    }
}

/// `$covariancePop` and `$covarianceSamp` over pairs `[x, y]`. A pair with a member that
/// is not a number is skipped; a pair with a non-finite member is counted, not folded in.
pub(crate) struct CovSlider<'a, 'v> {
    pub(crate) exprs: &'a [Expr; 2],
    pub(crate) sample: bool,
    pub(crate) vars: &'a [Binding<'v>],
    m: CoMoment,
    nonfinite: usize,
    removals: usize,
    lo: usize,
    hi: usize,
}

impl<'a, 'v> CovSlider<'a, 'v> {
    pub(crate) fn new(exprs: &'a [Expr; 2], sample: bool, vars: &'a [Binding<'v>]) -> Self {
        CovSlider {
            exprs,
            sample,
            vars,
            m: CoMoment::default(),
            nonfinite: 0,
            removals: 0,
            lo: 0,
            hi: 0,
        }
    }

    fn pair(&self, i: usize, docs: &[Document]) -> Result<Option<(f64, f64)>> {
        let x = numeric(&eval(&self.exprs[0], &docs[i], self.vars)?);
        let y = numeric(&eval(&self.exprs[1], &docs[i], self.vars)?);
        Ok(x.zip(y))
    }
}

impl Slider for CovSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        match self.pair(i, docs)? {
            Some((x, y)) if x.is_finite() && y.is_finite() => self.m.add(x, y),
            Some(_) => self.nonfinite += 1,
            None => {}
        }
        self.hi = i + 1;
        Ok(())
    }

    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        match self.pair(i, docs)? {
            Some((x, y)) if x.is_finite() && y.is_finite() => {
                self.m.remove(x, y);
                self.removals += 1;
            }
            Some(_) => self.nonfinite = self.nonfinite.saturating_sub(1),
            None => {}
        }
        self.lo = i + 1;
        Ok(())
    }

    fn reset(&mut self, at: usize) {
        self.m = CoMoment::default();
        self.nonfinite = 0;
        self.removals = 0;
        self.lo = at;
        self.hi = at;
    }

    fn value(&mut self, docs: &[Document], _: usize, _: usize) -> Result<Bson> {
        if self.removals >= REBUILD_AFTER_REMOVALS.max(self.hi - self.lo) {
            self.m = CoMoment::default();
            for i in self.lo..self.hi {
                if let Some((x, y)) = self.pair(i, docs)?
                    && x.is_finite()
                    && y.is_finite()
                {
                    self.m.add(x, y);
                }
            }
            self.removals = 0;
        }
        let n = self.m.n + self.nonfinite as u64;
        if self.nonfinite > 0 {
            return Ok(if self.sample && n == 1 { Bson::Null } else { Bson::Double(f64::NAN) });
        }
        Ok(match (n, self.sample) {
            (0, _) | (1, true) => Bson::Null,
            (n, true) => Bson::Double(self.m.c / (n - 1) as f64),
            (n, false) => Bson::Double(self.m.c / n as f64),
        })
    }
}
