//! The join law over the PRE-SYNC member states of a differential script, and what the loss records
//! of every route may be (the at-least-once contract: design section 16 Q7, `docs/deviations.md`, "The
//! loss records are at-least-once").
//!
//! The differentials compare ONE route: the final full-mesh state. This check takes the member states
//! as they stand before that sync (each still has its `contained` set of writes) and, for every
//! ordered triple of members, joins both ways, `L = (a + b) + c` and `R = a + (b + c)`:
//!
//! * `LAW-STATE`: the state WITHOUT its loss records is the same in `L` and `R` (associative), and so is its `loss_free_hash`;
//! * `LAW-COMM`, `LAW-IDEM`: `a + b = b + a` and `(a + b) + b = a + b`, on the whole state, records
//!   included (pairs);
//! * `LAW-MUST`: in every result (each member, each pair, each `L`, each `R`) every head the oracle
//!   requires over the writes that result holds, and that the result's horizon has not passed, is
//!   covered by one of its records (`oracle::covered`): the records are never fewer than required, but for what the
//!   cap under-reports. THE CAP RULE: a required head with no record is the cap's under-report (`docs/storage.md`,
//!   "a loss beyond `K` may go unreported"; `docs/deviations.md`, the entry on `K`) only when the result holds `K`
//!   records and the head's stamp ranks at or below the lowest of them: the cap keeps the greatest, so nothing above the lowest
//!   held record could have been refused or displaced. A missing head that ranks ABOVE the lowest held record, or a missing head
//!   in a result below `K` records, is a `LAW-MUST` failure at any `K`;
//! * `LAW-ALLOWED`: every record of every result is sound against the whole history
//!   (`oracle::sound_record`), none names a surviving increment (`oracle::stale_records`), and every
//!   killed-and-superseded group has one of its keys: the records are never more than allowed (`LAW-ALLOWED unsound`,
//!   `LAW-ALLOWED stale`, `LAW-ALLOWED group`);
//! * `LAW-CONV`: for every triple `L + R = R + L`, and its records are the `K` greatest by rank of the
//!   union of the records of `L` and `R` that its horizon keeps: members that have exchanged everything
//!   hold the same records, under the cap, at the script's drawn `K`.
//!
//! What is NOT claimed, and so not asserted: `records(L) = records(R)`. Which records a member holds
//! depends on the order it met the writes in (`docs/deviations.md`; the ways it happens: a cell
//! that a later write overwrote after seeing it keeps its record in one route; a `clash` is decided from the
//! two operand states, not from the two cells; a `clash` in one route is an `insertLost` in the other).
//!
//! WHERE IT RUNS. The pairs (`LAW-COMM`, `LAW-IDEM`) in every script. `LAW-STATE` in the scripts whose retention nothing
//! outruns (`ret = 10^9`), and, in the others, in the triples inside the strict per-origin bound of the claim (ADR-085,
//! ADR-097, `docs/time-and-conflicts.md`: for every origin the three states' version vectors name, the greatest entry minus the
//! least, a missing entry counting as 0, is at most `ret`); outside the bound the state is not associative across a partition
//! longer than `R` (the documented non-claim, `hand_built.rs`), and the triples outside it are counted, not checked. The
//! records (`LAW-MUST`, `LAW-ALLOWED`, `LAW-CONV`) only in the scripts of retention 10^9: the oracle does not model the
//! horizon's pruning of a required head.
//!
//! PLANTS. [`Plant`] makes the check's own joins wrong in one defined way each, and the test of the test
//! (`the_law_check_catches_its_plants`, in each differential) requires the check to fail with the tag it
//! was designed to fail with.

use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::merge_state::{
    JoinOptions, LossAt, LossKey, LossKind, LossValue, MStamp, MergeState,
};

use crate::oracle;

/// One defined wrongness of the check's own joins; `None` is the real join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plant {
    None,
    /// The right route's result `a + (b + c)` loses a register cell: associativity leaks by route (`LAW-STATE`).
    RouteLeak,
    /// `b + a` loses a register cell: commutativity (`LAW-COMM`).
    CommLeak,
    /// `(a + b) + b` loses a register cell: idempotence (`LAW-IDEM`).
    IdemLeak,
    /// The join reports no cell of a dead life (rule (2), `deleteWon` and `insertLost`: the `cells_reported` switch off): a required record is missing (`LAW-MUST`).
    NoCellReports,
    /// Every result holds a `clash` record for a write that wrote no such path (`LAW-ALLOWED`).
    BogusRecord,
    /// The join of `L` and `R`, both ways, loses its greatest record: the records are not the `K` greatest of the union (`LAW-CONV`).
    TopRecordDropped,
    /// The join of `L` and `R`, both ways, keeps the `K` SMALLEST records of the union: a cap that keeps the wrong end (`LAW-CONV`).
    CapKeepsSmallest,
    /// The join reports no `typeClash` (the `no_type_clash` switch): a required record is missing, in a document with maps (`LAW-MUST`).
    NoTypeClash,
    /// `R + L` loses a register cell: `L + R != R + L` (`LAW-CONV`).
    ConvCommLeak,
}

impl Plant {
    /// The tag of the failure the plant is designed to cause.
    pub fn tag(self) -> &'static str {
        match self {
            Plant::None => "",
            Plant::RouteLeak => "LAW-STATE",
            Plant::CommLeak => "LAW-COMM",
            Plant::IdemLeak => "LAW-IDEM",
            Plant::NoCellReports => "LAW-MUST",
            Plant::BogusRecord => "LAW-ALLOWED unsound",
            Plant::TopRecordDropped | Plant::CapKeepsSmallest | Plant::ConvCommLeak => "LAW-CONV",
            Plant::NoTypeClash => "LAW-MUST",
        }
    }

    /// The plant `Q7DIFF_LAW_PLANT` names, `Plant::None` when it is not set: a hold runs the differential once per plant and requires it
    /// red on the plant's tag. A name that is no plant is a panic, not a silent pass.
    pub fn from_env() -> Plant {
        let Ok(name) = std::env::var("Q7DIFF_LAW_PLANT") else { return Plant::None };
        Plant::NESTED
            .into_iter()
            .find(|p| format!("{p:?}") == name)
            .unwrap_or_else(|| panic!("Q7DIFF_LAW_PLANT={name} names no plant"))
    }

    /// The plants of the flat differential (a flat document has no `typeClash`).
    pub const FLAT: [Plant; 8] = [
        Plant::RouteLeak,
        Plant::CommLeak,
        Plant::IdemLeak,
        Plant::NoCellReports,
        Plant::BogusRecord,
        Plant::TopRecordDropped,
        Plant::CapKeepsSmallest,
        Plant::ConvCommLeak,
    ];

    /// The plants of the nested differential.
    pub const NESTED: [Plant; 9] = [
        Plant::RouteLeak,
        Plant::CommLeak,
        Plant::IdemLeak,
        Plant::NoCellReports,
        Plant::NoTypeClash,
        Plant::BogusRecord,
        Plant::TopRecordDropped,
        Plant::CapKeepsSmallest,
        Plant::ConvCommLeak,
    ];
}

/// How many of the ordered triples of a script are joined both ways. `Q7DIFF_LAW=all` (the gate's setting)
/// takes every triple; the default takes every `n`th (a fixed sample, the same on every run).
#[derive(Clone, Copy, Debug)]
pub struct Scope(pub usize);

impl Scope {
    pub fn from_env(default_every: usize) -> Self {
        match std::env::var("Q7DIFF_LAW").as_deref() {
            Ok("all") => Scope(1),
            Ok(n) if n.parse::<usize>().is_ok_and(|n| n >= 1) => Scope(n.parse().unwrap()),
            _ => Scope(default_every),
        }
    }
}

/// A member's state and the writes it holds.
pub struct Member<'a> {
    pub state: &'a MergeState,
    pub contained: &'a BTreeSet<MStamp>,
}

impl Counts {
    pub fn add(&mut self, o: &Counts) {
        self.triples += o.triples;
        self.results += o.results;
        self.micros += o.micros;
        self.crowded += o.crowded;
        self.state_checked += o.state_checked;
        self.state_outside_bound += o.state_outside_bound;
        for (k, (r, m, c)) in &o.per_k {
            let e = self.per_k.entry(*k).or_default();
            *e = (e.0 + r, e.1 + m, e.2 + c);
        }
    }

    /// The report lines of the crowd-out per `K`: `crowd-out: K=4: 36104 results, 25368 miss a required head, 1892 of those although the
    /// required heads alone fit under K`.
    pub fn crowd_out_lines(&self) -> Vec<String> {
        self.per_k
            .iter()
            .map(|(k, (r, m, c))| {
                format!(
                    "crowd-out: K={k}: {r} results, {m} miss a required head, {c} of those although the required heads alone fit under K"
                )
            })
            .collect()
    }
}

/// One failure of the check: the tag of the law it breaks and the message.
#[derive(Clone, Debug)]
pub struct Failure {
    pub tag: &'static str,
    pub msg: String,
}

impl Failure {
    fn new(tag: &'static str, msg: String) -> Self {
        Failure { tag, msg }
    }
}

/// The report of a script's failures: the first, then how many there are of each tag (`LAW-STATE=3, LAW-MUST=1`).
pub fn report(bad: &[Failure]) -> String {
    let mut by: BTreeMap<&str, usize> = BTreeMap::new();
    for f in bad {
        *by.entry(f.tag).or_default() += 1;
    }
    let tags: Vec<String> = by.iter().map(|(t, n)| format!("{t}={n}")).collect();
    format!("{} {} (all law failures by tag: {})", bad[0].tag, bad[0].msg, tags.join(", "))
}

/// What the check ran over, for the report line.
#[derive(Clone, Debug, Default)]
pub struct Counts {
    /// The ordered triples joined both ways.
    pub triples: usize,
    /// The triples whose state law was checked, and those outside the per-origin bound of a small-retention script (not checked).
    pub state_checked: usize,
    pub state_outside_bound: usize,
    /// For each `K` a script drew: the results judged, those that miss a required head, and of those the results whose
    /// required heads (older than the horizon) alone fit under `K`: the cap's crowd-out, where an allowed record took a slot.
    pub per_k: BTreeMap<usize, (usize, usize, usize)>,
    /// The results judged.
    pub results: usize,
    /// Microseconds the check took (wall, in the calling thread).
    pub micros: u128,
    /// The required heads that are missing from a result which holds `K` records, at least one of them allowed but not
    /// required (an extra): the heads the cap's under-report cost where an extra took the room (summed over the judged results).
    pub crowded: usize,
}

#[derive(Clone)]
struct Res {
    state: MergeState,
    contained: BTreeSet<MStamp>,
}

/// The oracle over the writes of one result, with what the checks ask of it.
struct Sub {
    log: oracle::Log,
    must: BTreeSet<oracle::Head>,
    ksa: Vec<BTreeSet<oracle::Head>>,
}

/// What the judge says of one result.
#[derive(Clone, Default)]
struct Verdict {
    tags: Vec<&'static str>,
    /// The required heads (older than the horizon) with no record, and how many heads are required.
    absent: usize,
    required: usize,
    /// The heads missing from a result at `K` records that holds an allowed record the oracle does not require.
    crowded: usize,
}

struct Judge<'a> {
    log: &'a oracle::Log,
    ret: u64,
    bound: i64,
    alive: BTreeSet<MStamp>,
    dels: Vec<MStamp>,
    subs: BTreeMap<BTreeSet<MStamp>, Sub>,
    memo: BTreeMap<(BTreeSet<MStamp>, BTreeSet<LossKey>, u64), Verdict>,
}

impl<'a> Judge<'a> {
    fn new(log: &'a oracle::Log, ret: u64, bound: i64) -> Self {
        let (_, alive, dels) = oracle::oracle(log, ret, bound);
        Judge { log, ret, bound, alive, dels, subs: BTreeMap::new(), memo: BTreeMap::new() }
    }

    /// The failures of one result: `LAW-MUST`, `LAW-ALLOWED unsound`, `LAW-ALLOWED stale`, `LAW-ALLOWED group` (each at most once per result).
    fn judge(&mut self, r: &Res, o: &JoinOptions) -> Verdict {
        let horizon = r.state.horizon(o).wall_ms;
        let have: BTreeSet<LossKey> = r.state.losses.keys().cloned().collect();
        let mk = (r.contained.clone(), have.clone(), horizon);
        if let Some(v) = self.memo.get(&mk) {
            return v.clone();
        }
        if !self.subs.contains_key(&r.contained) {
            let sub: oracle::Log = self
                .log
                .iter()
                .filter(|(s, _)| r.contained.contains(s))
                .map(|(s, v)| (*s, v.clone()))
                .collect();
            let (doc, alive, dels) = oracle::oracle(&sub, self.ret, self.bound);
            let must = oracle::loss_oracle(&sub, &alive, &dels, doc.as_ref(), self.ret);
            let ksa = oracle::killed_superseded_any(&sub, self.ret);
            self.subs.insert(r.contained.clone(), Sub { log: sub, must, ksa });
        }
        let sub = &self.subs[&r.contained];
        let haveh: BTreeSet<oracle::Head> =
            have.iter().map(|k| (oracle::at_path(&k.at), k.stamp)).collect();
        let mut out = Vec::new();
        // The cap rule (module doc): a missing required head is the cap's under-report only in a result at `K` records, at or
        // below the lowest held.
        let cap = o.loss_cap();
        let lowest_held = have.iter().min_by(|a, b| a.rank_cmp(b)).map(|k| k.stamp);
        let required: Vec<&oracle::Head> =
            sub.must.iter().filter(|(_, s)| s.hlc.wall_ms >= horizon).collect();
        let absent: Vec<&oracle::Head> =
            required.iter().copied().filter(|h| !oracle::covered(h, &haveh, &sub.log)).collect();
        if absent.iter().any(|(_, s)| have.len() < cap || lowest_held.is_none_or(|low| *s > low)) {
            out.push("LAW-MUST");
        }
        // the heads crowded out: missing from a result at `K` that holds a record the oracle does not require (an extra)
        let has_extra = have.len() >= cap
            && haveh.iter().any(|rec| {
                !sub.must
                    .iter()
                    .any(|h| oracle::covered(h, &BTreeSet::from([rec.clone()]), &sub.log))
            });
        let crowded = if has_extra { absent.len() } else { 0 };
        // soundness is judged against the WHOLE history: a record that arrived in a loss entry, or whose evidence
        // (the concurrent winner) this result does not hold yet, is sound if the history holds its evidence
        let unsound = have
            .iter()
            .any(|k| !oracle::sound_record(k, self.log, &self.alive, &self.dels, self.ret));
        let stale = !oracle::stale_records(&have, self.log, self.ret).is_empty();
        // a group with none of its keys is the cap's under-report too when the result holds `K` records and a key of the
        // group, the one the merge made a record of, is not above the lowest held
        let ksa = sub.ksa.iter().any(|g| {
            g.iter().all(|(_, s)| s.hlc.wall_ms >= horizon)
                && g.is_disjoint(&haveh)
                && !(have.len() >= cap
                    && g.iter().any(|(_, s)| lowest_held.is_some_and(|low| *s <= low)))
        });
        if unsound {
            out.push("LAW-ALLOWED unsound");
        }
        if stale {
            out.push("LAW-ALLOWED stale");
        }
        if ksa {
            out.push("LAW-ALLOWED group");
        }
        let v = Verdict { tags: out, absent: absent.len(), required: required.len(), crowded };
        self.memo.insert(mk, v.clone());
        v
    }
}

/// The state without its loss records.
fn bare(s: &MergeState) -> MergeState {
    let mut s = s.clone();
    s.losses.clear();
    s
}

/// Which parts of two states differ (for the message).
fn parts(a: &MergeState, b: &MergeState) -> String {
    let mut d = Vec::new();
    if a.lives != b.lives {
        d.push("lives");
    }
    if a.deletes != b.deletes {
        d.push("deletes");
    }
    if a.dead != b.dead {
        d.push("dead");
    }
    if a.regs != b.regs {
        d.push("regs");
    }
    if a.slots != b.slots {
        d.push("slots");
    }
    if a.vv != b.vv {
        d.push("vv");
    }
    if a.losses != b.losses {
        d.push("losses");
    }
    if a.ords != b.ords {
        d.push("ords");
    }
    d.join(", ")
}

/// Is the triple inside the strict per-origin bound of the claim (ADR-085, ADR-097): for every origin the three states' version
/// vectors name, the greatest entry minus the least over the three (a missing entry counting as 0) is at most `ret`.
fn inside_the_bound(a: &MergeState, b: &MergeState, c: &MergeState, ret: u64) -> bool {
    let origins: BTreeSet<_> = a.vv.keys().chain(b.vv.keys()).chain(c.vv.keys()).collect();
    origins.into_iter().all(|w| {
        let at = |s: &MergeState| s.vv.get(w).map_or(0, |h| h.wall_ms);
        let (x, y, z) = (at(a), at(b), at(c));
        x.max(y).max(z) - x.min(y).min(z) <= ret
    })
}

/// A planted leak: the state loses one register cell (or, with none, a life).
fn tamper(s: &mut MergeState) {
    if let Some(k) = s.regs.keys().next().cloned() {
        s.regs.remove(&k);
    } else if let Some(k) = s.lives.keys().next().cloned() {
        s.lives.remove(&k);
    }
}

/// Run the law check over one script's pre-sync members. `ret` and `bound` are the script's; `o` is its join
/// options (its `K` included). Returns the failures (each starts with its tag) and what was checked.
pub fn check(
    members: &[Member],
    log: &oracle::Log,
    o: &JoinOptions,
    ret: u64,
    bound: i64,
    scope: Scope,
    plant: Plant,
) -> (Vec<Failure>, Counts) {
    let mut o_join = o.clone();
    if plant == Plant::NoCellReports {
        o_join.set_cells_reported(false);
    }
    if plant == Plant::NoTypeClash {
        o_join.set_no_type_clash(true);
    }
    // the records are judged where nothing outruns the retention; the state law also in the triples of the others inside the bound
    let full = ret == 1_000_000_000;
    let jn = |a: &Res, b: &Res| Res {
        state: a.state.join(&b.state, &o_join).0,
        contained: a.contained.union(&b.contained).copied().collect(),
    };
    let m: Vec<Res> = members
        .iter()
        .map(|x| Res { state: x.state.clone(), contained: x.contained.clone() })
        .collect();
    let n = m.len();
    let started = std::time::Instant::now();
    let mut bad: Vec<Failure> = Vec::new();
    let mut counts = Counts::default();
    let mut results: Vec<Res> = m.clone();
    let mut jj: BTreeMap<(usize, usize), Res> = BTreeMap::new();
    for i in 0..n {
        for j in 0..n {
            jj.insert((i, j), jn(&m[i], &m[j]));
        }
    }
    // pairs: commutative and idempotent on the whole state
    for i in 0..n {
        for j in 0..n {
            let ab = &jj[&(i, j)];
            let mut ba = jj[&(j, i)].clone();
            if plant == Plant::CommLeak && i != j {
                tamper(&mut ba.state);
            }
            if ab.state != ba.state {
                bad.push(Failure::new(
                    "LAW-COMM",
                    format!(
                        "members {i},{j}: a+b != b+a, differing in: {}",
                        parts(&ab.state, &ba.state)
                    ),
                ));
            }
            let mut again = jn(ab, &m[j]);
            if plant == Plant::IdemLeak {
                tamper(&mut again.state);
            }
            if again.state != ab.state {
                bad.push(Failure::new(
                    "LAW-IDEM",
                    format!(
                        "members {i},{j}: (a+b)+b != a+b, differing in: {}",
                        parts(&again.state, &ab.state)
                    ),
                ));
            }
            if full {
                results.push(ab.clone());
            }
        }
    }
    let cap = o.loss_cap();
    let mut t = 0usize;
    for i in 0..n {
        for j in 0..n {
            for k in 0..n {
                t += 1;
                if !(t - 1).is_multiple_of(scope.0) {
                    continue;
                }
                if !full && !inside_the_bound(&m[i].state, &m[j].state, &m[k].state, ret) {
                    counts.state_outside_bound += 1;
                    continue;
                }
                counts.triples += 1;
                counts.state_checked += 1;
                let l = jn(&jj[&(i, j)], &m[k]);
                let mut r = jn(&m[i], &jj[&(j, k)]);
                if plant == Plant::RouteLeak {
                    tamper(&mut r.state);
                }
                if bare(&l.state) != bare(&r.state) {
                    bad.push(Failure::new(
                        "LAW-STATE",
                        format!(
                            "triple {i},{j},{k}: the state without its records differs between (a+b)+c and a+(b+c), in: {}",
                            parts(&bare(&l.state), &bare(&r.state))
                        ),
                    ));
                } else if l.state.loss_free_hash() != r.state.loss_free_hash() {
                    bad.push(Failure::new(
                        "LAW-STATE",
                        format!("triple {i},{j},{k}: the loss-free hash differs between (a+b)+c and a+(b+c) of equal states"),
                    ));
                }
                if !full {
                    continue;
                }
                // convergence: L + R = R + L, and its records are the K greatest of the union that its horizon keeps
                let mut lr = jn(&l, &r);
                let mut rl = jn(&r, &l);
                if plant == Plant::TopRecordDropped {
                    for x in [&mut lr, &mut rl] {
                        if let Some(top) =
                            x.state.losses.keys().max_by(|a, b| a.rank_cmp(b)).cloned()
                        {
                            x.state.losses.remove(&top);
                        }
                    }
                }
                if plant == Plant::ConvCommLeak {
                    tamper(&mut rl.state);
                }
                if plant == Plant::CapKeepsSmallest {
                    let mut all: BTreeMap<LossKey, LossValue> = l.state.losses.clone();
                    all.extend(r.state.losses.clone());
                    let mut keys: Vec<LossKey> = all.keys().cloned().collect();
                    keys.sort_by(|a, b| a.rank_cmp(b));
                    for x in [&mut lr, &mut rl] {
                        x.state.losses =
                            keys.iter().take(cap).map(|k| (k.clone(), all[k].clone())).collect();
                    }
                }
                if lr.state != rl.state {
                    bad.push(Failure::new(
                        "LAW-CONV",
                        format!(
                            "triple {i},{j},{k}: L+R != R+L, differing in: {}",
                            parts(&lr.state, &rl.state)
                        ),
                    ));
                }
                let h = lr.state.horizon(o).wall_ms;
                let mut union: Vec<LossKey> = l
                    .state
                    .losses
                    .keys()
                    .chain(r.state.losses.keys())
                    .filter(|key| key.stamp.hlc.wall_ms >= h)
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                union.sort_by(|a, b| a.rank_cmp(b));
                let want: BTreeSet<LossKey> = union.into_iter().rev().take(cap).collect();
                let got: BTreeSet<LossKey> = lr.state.losses.keys().cloned().collect();
                if got != want {
                    bad.push(Failure::new(
                        "LAW-CONV",
                        format!(
                            "triple {i},{j},{k}: the records of L+R are not the {cap} greatest of the union of those of L and R (held {}, expected {}; only held: {:?}; only expected: {:?})",
                            got.len(),
                            want.len(),
                            got.difference(&want).take(3).collect::<Vec<_>>(),
                            want.difference(&got).take(3).collect::<Vec<_>>()
                        ),
                    ));
                }
                results.push(l);
                results.push(r);
            }
        }
    }
    if !full {
        counts.micros = started.elapsed().as_micros();
        return (bad, counts);
    }
    // the records of every result: required and allowed
    let mut judge = Judge::new(log, ret, bound);
    let bogus = log.keys().next().map(|s| LossKey {
        kind: LossKind::Clash,
        at: LossAt::Document,
        stamp: *s,
    });
    for (idx, mut r) in results.into_iter().enumerate() {
        counts.results += 1;
        if plant == Plant::BogusRecord
            && let Some(b) = &bogus
        {
            r.state.losses.insert(b.clone(), LossValue::Fields(Vec::new()));
        }
        let v = judge.judge(&r, o);
        counts.crowded += v.crowded;
        let e = counts.per_k.entry(cap).or_default();
        *e = (
            e.0 + 1,
            e.1 + usize::from(v.absent > 0),
            e.2 + usize::from(v.absent > 0 && v.required <= cap),
        );
        for tag in v.tags {
            bad.push(Failure::new(
                tag,
                format!(
                    "result {idx} (writes {}, {} records): {}",
                    r.contained.len(),
                    r.state.losses.len(),
                    match tag {
                        "LAW-MUST" => "a required head has no record",
                        "LAW-ALLOWED unsound" => "a record is not sound against the whole history",
                        "LAW-ALLOWED stale" => "a record names an increment that survives",
                        _ => "a killed-and-superseded group has none of its keys",
                    }
                ),
            ));
        }
    }
    counts.micros = started.elapsed().as_micros();
    (bad, counts)
}
