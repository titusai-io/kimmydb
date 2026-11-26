//! Unit 14 (the origin check, spec `Q6-unit14-model-spec.md` section 1): a binding is valid only when its origin is the origin of its stage's creation,
//! and it is stamped at or below the stage's deadline. Tests of the model against itself: the scripted rows of `u14-scripted.py` (the Python model's
//! `swapmodel6i.py`), the restored writer (the harness' form of "a writer whose epoch changed can not bind its old stages": a restore is a new origin,
//! `restore.rs`), and the random grid with the job kind `Foreign` (`Cfg::foreign_bind`). The expected states are written here, not read from the oracle.
//! The mutant `m-bind-foreign-origin` (the members' validity test leaves the origin out) must fail every row and the grid; the base is clean.
//!
//! `swap_sim` has no burial by a later event (DOOM, decision 5): the second way a stage with no binding in effect ends here is the writer's abandon and
//! the coverage safety net (a member covers the writer's origin past the stage's deadline); the DOOM ending is the Python row's.

use crate::entry::*;
use crate::member::{DocOp, Member};
use crate::oracle::finish;
use crate::run::{Cfg, grid, run_grid};
use crate::scenario::Sc;

const RECV: [&str; 4] = ["P1", "P2", "P3", "P4"];
const NAMES: [&str; 6] = ["W", "X", "P1", "P2", "P3", "P4"];

/// A row's failures: (member, checkpoint, what, is it a flip: the member served or was born by the foreign binding).
#[derive(Default)]
struct Fails(Vec<(String, String, String, bool)>);

impl Fails {
    fn chk(&mut self, cond: bool, who: &str, cp: &str, what: &str) {
        self.chk_flip(cond, who, cp, what, false);
    }
    fn chk_flip(&mut self, cond: bool, who: &str, cp: &str, what: &str, flip: bool) {
        if !cond {
            self.0.push((who.into(), cp.into(), what.into(), flip));
        }
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn members(&self) -> Vec<String> {
        let mut v: Vec<String> = self.0.iter().map(|f| f.0.clone()).collect();
        v.sort();
        v.dedup();
        v
    }
    fn flip_members(&self) -> Vec<String> {
        let mut v: Vec<String> = self.0.iter().filter(|f| f.3).map(|f| f.0.clone()).collect();
        v.sort();
        v.dedup();
        v
    }
}

fn m<'a>(sc: &'a Sc, name: &str) -> &'a Member {
    &sc.ms[sc.idx(name)]
}

fn never_in_effect(f: &mut Fails, sc: &Sc, who: &str, bx: &EntryRef, cp: &str) {
    let me = m(sc, who);
    f.chk(!me.events.contains(&bx.idx), who, cp, "the foreign binding is an event in effect");
    f.chk(!me.info.contains_key(&bx.idx), who, cp, "the foreign binding is held as information");
    f.chk(
        me.cur.as_ref().is_none_or(|c| c.idx != bx.idx),
        who,
        cp,
        "the foreign binding is the visible event",
    );
    f.chk(
        !me.evl.contains(&bx.idx),
        who,
        cp,
        "the foreign binding is in the member's sorted events",
    );
}

fn unbound_identity(f: &mut Fails, sc: &Sc, who: &str, stage: &str, cp: &str) {
    let me = m(sc, who);
    f.chk_flip(
        me.serving().as_deref() == Some(L),
        who,
        cp,
        &format!("the name serves {:?}, not the identity", me.serving()),
        true,
    );
    f.chk(me.phys.get(stage).is_some_and(|p| p.alive), who, cp, "the stage is not alive");
    f.chk_flip(!me.bmap.contains_key(stage), who, cp, "the stage has a birth: it is bound", true);
}

fn bound_by(f: &mut Fails, sc: &Sc, who: &str, stage: &str, bw: &EntryRef, cp: &str) {
    let me = m(sc, who);
    f.chk_flip(
        me.serving().as_deref() == Some(stage),
        who,
        cp,
        &format!("the name serves {:?}, not the stage", me.serving()),
        true,
    );
    f.chk_flip(
        me.cur.as_ref().is_some_and(|c| c.idx == bw.idx),
        who,
        cp,
        "the visible event is not the creator's binding",
        true,
    );
    f.chk_flip(
        me.bmap.get(stage) == Some(&bw.stamp),
        who,
        cp,
        "the stage's birth is not the creator's stamp",
        true,
    );
    let fs = me.first_superseding(&sc.w, L);
    f.chk_flip(
        fs.is_some_and(|e| e.stamp == bw.stamp),
        who,
        cp,
        "first_superseding of the identity is not the creator's binding",
        true,
    );
}

/// W stages S1 (its creation and staging rows, the deadline well above), every member takes them; X writes one document of its own, so that its binding
/// is not its first entry (it can arrive ahead of its position). Returns the world, the job and the stage.
fn stage_world(mode: &str, names: &[&str]) -> (Sc, Job, String) {
    let members: Vec<(&str, i64)> = names.iter().map(|n| (*n, 0)).collect();
    let mut sc = Sc::new(mode, &members, 60);
    let w = sc.idx(names[0]);
    sc.advance(1);
    for k in 0..3 {
        sc.ms[w].local_doc(&mut sc.w, k, DocOp::Insert).expect("a seed document");
    }
    for n in &names[1..] {
        sc.pull(n, names[0]);
    }
    sc.advance(10);
    let job = sc.ms[w].start_out(&mut sc.w, vec![0, 1], JobKind::Good, 0);
    for n in &names[1..] {
        sc.pull(n, names[0]);
    }
    (sc, job.clone(), job.stage)
}

/// `who` mints a binding of the job's stage at its next stamp (a member that is not the stage's creator).
fn mint_by(sc: &mut Sc, who: &str, job: &Job) -> EntryRef {
    let i = sc.idx(who);
    let h = sc.ms[i].next_hlc(&sc.w);
    let mut digest = job.digest.clone();
    digest.sort();
    let b = sc.w.record(
        Entry::new(i, h, Kind::Bind).phys(&job.stage).dl(job.dl).extra(Extra::Digest(digest)),
        true,
    );
    sc.ms[i].take(&mut sc.w, &b);
    sc.w.stats.bump("foreign_bind_minted");
    b
}

/// The row `foreign`: X mints `bind S1` below the deadline; W then mints its own valid `bind S1` at a higher stamp. Four receivers, four delivery orders (P1:
/// X's binding first then W's; P2: W's first then X's; P3: X's binding ahead of its position, parked, then W's, then X's earlier entries; P4: X's binding
/// alone, W's last). Expected at every checkpoint: X's binding is never in effect, never information, never visible; before W's is in effect the name serves
/// the identity and the stage is alive and unbound; after it the name serves the stage, bound at W's stamp.
fn row_foreign(mode: &str) -> Fails {
    let mut f = Fails::default();
    let (mut sc, mut job, s1) = stage_world(mode, &NAMES);
    let (w, x) = (sc.idx("W"), sc.idx("X"));
    sc.advance(12);
    sc.ms[x].local_doc(&mut sc.w, 2, DocOp::Update).expect("X's own document");
    sc.advance(14);
    let bx = mint_by(&mut sc, "X", &job);
    sc.advance(20);
    let bw = sc.ms[w].do_bind(&mut sc.w, &mut job, 6).expect("W's binding");
    f.chk(
        bw.stamp > bx.stamp && sc.w.valid(&bw),
        "W",
        "setup",
        "W's binding is not valid above X's",
    );
    f.chk(
        sc.w.f.bind_foreign_origin_valid || !sc.w.valid(&bx),
        "W",
        "setup",
        "the world calls X's binding valid with the check on",
    );
    // P1: X's binding first, then W's
    sc.pull_origins("P1", "X", &["X"]);
    never_in_effect(&mut f, &sc, "P1", &bx, "P1 after X's window");
    unbound_identity(&mut f, &sc, "P1", &s1, "P1 after X's window");
    sc.pull_origins("P1", "W", &["W"]);
    never_in_effect(&mut f, &sc, "P1", &bx, "P1 after W's window");
    bound_by(&mut f, &sc, "P1", &s1, &bw, "P1 after W's window");
    // P2: W's first, then X's
    sc.pull_origins("P2", "W", &["W"]);
    never_in_effect(&mut f, &sc, "P2", &bx, "P2 after W's window");
    bound_by(&mut f, &sc, "P2", &s1, &bw, "P2 after W's window");
    sc.pull_origins("P2", "X", &["X"]);
    never_in_effect(&mut f, &sc, "P2", &bx, "P2 after X's window");
    bound_by(&mut f, &sc, "P2", &s1, &bw, "P2 after X's window");
    // P3: X's binding ahead of its position (held, parked as information if it were valid), then W's, then X's earlier entries release it
    sc.w.reg_in_position_check = false; // the scenario delivers a binding out of position on purpose (rule B's shape)
    let p3 = sc.idx("P3");
    sc.ms[p3].take_hold(&mut sc.w, &bx);
    never_in_effect(&mut f, &sc, "P3", &bx, "P3 with X's binding ahead of its position");
    unbound_identity(&mut f, &sc, "P3", &s1, "P3 with X's binding ahead of its position");
    sc.pull_origins("P3", "W", &["W"]);
    never_in_effect(&mut f, &sc, "P3", &bx, "P3 after W's binding");
    bound_by(&mut f, &sc, "P3", &s1, &bw, "P3 after W's binding");
    sc.pull_origins("P3", "X", &["X"]);
    sc.ms[p3].settle(&mut sc.w);
    never_in_effect(&mut f, &sc, "P3", &bx, "P3 after X's earlier entries released the binding");
    bound_by(&mut f, &sc, "P3", &s1, &bw, "P3 after X's earlier entries released the binding");
    // P4: X's binding alone; W's arrives last
    sc.pull_origins("P4", "X", &["X"]);
    let p4 = sc.idx("P4");
    sc.ms[p4].settle(&mut sc.w);
    never_in_effect(&mut f, &sc, "P4", &bx, "P4 with X's binding alone");
    unbound_identity(&mut f, &sc, "P4", &s1, "P4 with X's binding alone");
    f.chk(
        sc.ms[p4].first_superseding(&sc.w, L).is_none(),
        "P4",
        "P4 with X's binding alone",
        "first_superseding of the identity is not None",
    );
    sc.pull_origins("P4", "W", &["W"]);
    never_in_effect(&mut f, &sc, "P4", &bx, "P4 after W's binding");
    bound_by(&mut f, &sc, "P4", &s1, &bw, "P4 after W's binding");
    // the end: every member holds every entry
    let live = sc.quiesce();
    for i in live {
        let who = sc.ms[i].name.clone();
        never_in_effect(&mut f, &sc, &who, &bx, &format!("{who} at the end"));
        bound_by(&mut f, &sc, &who, &s1, &bw, &format!("{who} at the end"));
    }
    let res = finish(&mut sc.w, &mut sc.ms);
    for k in problems_of(&res) {
        f.chk(false, "model", "finish", &k);
    }
    f
}

/// The checks of `finish` that must be 0 on the base.
fn problems_of(res: &crate::oracle::Res) -> Vec<String> {
    res.iter()
        .filter(|(k, v)| {
            **v != 0
                && (k.starts_with("inv_")
                    || [
                        "diverged",
                        "div_state",
                        "div_loss",
                        "oracle_state",
                        "loss_mismatch",
                        "loss_missing",
                        "loss_extra",
                        "alive_mismatch",
                    ]
                    .contains(&k.as_str()))
        })
        .map(|(k, v)| format!("{k}={v}"))
        .collect()
}

/// One run of the row `norescue`: the writer never binds; the stage ends by the abandon, or by the coverage safety net (a later entry of the writer above the
/// deadline, covered). With X's foreign binding, or with nothing. Returns the world and the stage.
fn norescue_run(mode: &str, ending: &str, with_foreign: bool) -> (Sc, String, Option<EntryRef>) {
    let (mut sc, job, s1) = stage_world(mode, &NAMES);
    let w = sc.idx("W");
    sc.advance(12);
    let x = sc.idx("X");
    sc.ms[x].local_doc(&mut sc.w, 2, DocOp::Update).expect("X's own document");
    sc.advance(14);
    let bx = if with_foreign { Some(mint_by(&mut sc, "X", &job)) } else { None };
    sc.advance(20);
    if ending == "abandon" {
        sc.ms[w].abandon_stage(&mut sc.w, &s1);
    } else {
        sc.advance(job.dl + 10); // past the stage's deadline: a later entry of the writer, covered by every member
        sc.ms[w].local_doc(&mut sc.w, 2, DocOp::Update).expect("W's later document");
    }
    sc.quiesce();
    let res = finish(&mut sc.w, &mut sc.ms);
    assert!(
        problems_of(&res).is_empty() || mode != "v6",
        "the model's own checks on a base norescue run: {:?}",
        problems_of(&res)
    );
    (sc, s1, bx)
}

fn state_of(sc: &Sc, who: &str, stage: &str) -> String {
    let me = m(sc, who);
    let mut alive: Vec<String> =
        me.phys.iter().filter(|(_, p)| p.alive).map(|(n, _)| n.clone()).collect();
    alive.sort();
    format!(
        "serving={:?} cur={:?} alive={:?} stage={:?}",
        me.serving(),
        me.cur.as_ref().map(|c| c.stamp),
        alive,
        me.phys.get(stage).map(|p| (p.alive, p.orphan, p.kill))
    )
}

/// The row `norescue`: without W's binding the name serves the identity to the end and the stage ends by the orphan rules exactly as an unbound stage with no
/// binding at all does: the end state of every member is the same with X's foreign binding and with none (two endings).
fn row_norescue(mode: &str) -> Fails {
    let mut f = Fails::default();
    for ending in ["abandon", "safety_net"] {
        let (with, s1, bx) = norescue_run(mode, ending, true);
        let (without, s2, _) = norescue_run(mode, ending, false);
        let bx = bx.expect("the run with the foreign binding minted it");
        for n in NAMES {
            let cp = ending;
            let (a, b) = (state_of(&with, n, &s1), state_of(&without, n, &s2));
            // the stage names differ by nothing (same writer, same stamp): compare as printed
            f.chk_flip(
                a == b,
                n,
                cp,
                &format!("the end state differs with X's binding and with none: {a} vs {b}"),
                true,
            );
            // the writer buries its own stage at once (its abandon, or its later entry above the deadline), so the foreign binding changes no end state at W: the member
            // that took it into its register is still a difference, whoever it is
            never_in_effect(&mut f, &with, n, &bx, cp);
            let me = m(&with, n);
            f.chk_flip(
                me.serving().as_deref() == Some(L),
                n,
                cp,
                "the name does not serve the identity",
                true,
            );
            f.chk(
                me.phys.get(&s1).is_some_and(|p| !p.alive),
                n,
                cp,
                "the stage is alive at the end (not buried by the orphan rules)",
            );
        }
    }
    f
}

/// The control: W's own binding alone is stored and visible on every member.
fn row_control(mode: &str) -> Fails {
    let mut f = Fails::default();
    let (mut sc, mut job, s1) = stage_world(mode, &NAMES);
    let w = sc.idx("W");
    sc.advance(20);
    let bw = sc.ms[w].do_bind(&mut sc.w, &mut job, 6).expect("W's binding");
    let live = sc.quiesce();
    for i in live {
        let who = sc.ms[i].name.clone();
        bound_by(&mut f, &sc, &who, &s1, &bw, &format!("{who} at the end"));
    }
    f
}

/// The restored writer: B stages S1 and is backed up; B never writes again; N is restored from the backup (a NEW ORIGIN: the harness' form of a changed epoch);
/// N mints the binding of B's stage. Expected: the binding is invalid on every member (never in effect, never information, never visible) and the stage is an
/// orphan: the name serves the identity and the stage stays alive and unbound.
fn row_restored(mode: &str) -> Fails {
    let mut f = Fails::default();
    let (mut sc, job, s1) = stage_world(mode, &["B", "N", "P1", "P2"]);
    let (b, n) = (sc.idx("B"), sc.idx("N"));
    sc.advance(12);
    let backup = sc.ms[b].clone();
    sc.advance(14);
    sc.vanish(&["B"]);
    let mut restored = backup;
    restored.id = n;
    restored.name = sc.ms[n].name.clone();
    restored.skew = sc.ms[n].skew;
    sc.ms[n] = restored;
    sc.advance(20);
    let bn = mint_by(&mut sc, "N", &job);
    for p in ["P1", "P2"] {
        sc.pull_origins(p, "N", &["N"]);
        never_in_effect(&mut f, &sc, p, &bn, &format!("{p} after N's binding"));
        unbound_identity(&mut f, &sc, p, &s1, &format!("{p} after N's binding"));
    }
    never_in_effect(&mut f, &sc, "N", &bn, "N after minting");
    unbound_identity(&mut f, &sc, "N", &s1, "N after minting");
    let live = sc.quiesce();
    for i in live {
        let who = sc.ms[i].name.clone();
        never_in_effect(&mut f, &sc, &who, &bn, &format!("{who} at the end"));
        unbound_identity(&mut f, &sc, &who, &s1, &format!("{who} at the end"));
    }
    let res = finish(&mut sc.w, &mut sc.ms);
    for k in problems_of(&res) {
        f.chk(false, "model", "finish", &k);
    }
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    const MUTANT: &str = "m-bind-foreign-origin";

    fn show(f: &Fails) -> String {
        f.0.iter()
            .take(6)
            .map(|x| format!("{} at {}: {}", x.0, x.1, x.2))
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn a_binding_from_another_origin_is_never_in_effect_never_visible_and_the_creators_binding_wins()
     {
        let f = row_foreign("v6");
        assert!(f.is_empty(), "the base fails the foreign-binding row: {}", show(&f));
    }

    #[test]
    fn a_stage_whose_only_binding_is_foreign_ends_as_a_stage_with_no_binding_at_all() {
        let f = row_norescue("v6");
        assert!(f.is_empty(), "the base fails the no-rescue row: {}", show(&f));
    }

    #[test]
    fn the_creators_own_binding_alone_is_stored_and_visible() {
        let f = row_control("v6");
        assert!(f.is_empty(), "the base fails the control row: {}", show(&f));
    }

    #[test]
    fn a_restored_writer_is_a_new_origin_and_cannot_bind_the_stage_its_old_origin_created() {
        let f = row_restored("v6");
        assert!(f.is_empty(), "the base fails the restored-writer row: {}", show(&f));
    }

    /// The mutant fails the first row on the members that take X's binding before W's (they flip at X's stamp: P1 and P4), the second row on every member
    /// (a member takes X's binding into its register; the writer itself ends in the same state, having buried its own stage), and the restored-writer row; the control row is clean on it (the creator's own binding is valid either way).
    #[test]
    fn the_mutant_that_drops_the_origin_check_fails_every_row_but_the_control() {
        let f1 = row_foreign(MUTANT);
        assert!(!f1.is_empty(), "m-bind-foreign-origin survived the foreign-binding row");
        let flips = f1.flip_members();
        assert!(
            flips.contains(&"P1".to_string()) && flips.contains(&"P4".to_string()),
            "the members that take X's binding before W's flip: {flips:?} ({})",
            show(&f1)
        );
        let f2 = row_norescue(MUTANT);
        let mut want: Vec<String> = NAMES.iter().map(|s| s.to_string()).collect();
        want.sort();
        assert_eq!(f2.members(), want, "the no-rescue row fails on every member: {}", show(&f2));
        assert!(
            !row_restored(MUTANT).is_empty(),
            "m-bind-foreign-origin survived the restored-writer row"
        );
        let c = row_control(MUTANT);
        assert!(c.is_empty(), "the control row fails on the mutant: {}", show(&c));
    }

    fn foreign_cfgs() -> Vec<Cfg> {
        let mut cfgs = grid(&[3, 4, 5], &[2, 3], &[4]);
        for c in cfgs.iter_mut() {
            c.foreign_bind = true;
        }
        cfgs
    }

    /// The grid with the job kind `Foreign`: 0 problems on the base, and the run is not vacuous (a foreign binding is minted in many runs, and none takes effect).
    #[test]
    fn the_foreign_binding_grid_is_clean_and_not_vacuous() {
        let seeds = crate::env_u64("Q6_RUNS", 40);
        let s = run_grid("v6", "v6", &foreign_cfgs(), 0, seeds, crate::threads());
        let p = s.problems();
        eprintln!(
            "foreign grid: {} runs, foreign_bind_minted {} (runs {}), foreign_bind_took_effect {}",
            s.runs,
            s.get("cnt_foreign_bind_minted"),
            s.get("nv_foreign_bind_minted"),
            s.get("cnt_foreign_bind_took_effect")
        );
        assert!(p.is_empty(), "problems on the foreign-binding grid: {p:?}");
        assert!(s.get("cnt_foreign_bind_minted") > 0, "vacuous: no foreign binding was minted");
        assert!(
            s.get("nv_foreign_bind_minted") * 20 >= s.runs as i64,
            "vacuous: a foreign binding in only {} of {} runs",
            s.get("nv_foreign_bind_minted"),
            s.runs
        );
        assert_eq!(
            s.get("cnt_foreign_bind_took_effect"),
            0,
            "a foreign binding took effect on the base"
        );
    }

    /// The mutant fails the foreign-binding grid: more than 0 problems, and foreign bindings take effect on its members.
    #[test]
    fn the_mutant_that_drops_the_origin_check_fails_the_foreign_binding_grid() {
        let seeds = crate::env_u64("Q6_MUTANT_RUNS", 40);
        let s = run_grid(MUTANT, "v6", &foreign_cfgs(), 0, seeds, crate::threads());
        let p = s.problems();
        eprintln!(
            "{MUTANT}: {} kinds {:?}; foreign_bind_took_effect {}",
            p.len(),
            p.keys().collect::<Vec<_>>(),
            s.get("cnt_foreign_bind_took_effect")
        );
        assert!(!p.is_empty(), "{MUTANT} survived the foreign-binding grid");
        assert!(
            s.get("cnt_foreign_bind_took_effect") > 0,
            "no foreign binding took effect on the mutant"
        );
    }

    /// The identity control: with no foreign binding drawn (`foreign_bind` off) the existing draws do not move, so the base grid is the same as without the unit.
    #[test]
    fn the_origin_check_changes_nothing_where_no_job_mints_a_foreign_binding() {
        let seeds = crate::env_u64("Q6_RUNS", 40).min(20);
        let cfgs = grid(&[3, 4], &[2, 3], &[4]);
        let on = run_grid("v6", "v6", &cfgs, 0, seeds, crate::threads());
        assert!(on.problems().is_empty(), "problems on the base grid: {:?}", on.problems());
        assert_eq!(on.get("cnt_foreign_bind_minted"), 0);
        assert_eq!(on.get("cnt_foreign_bind_took_effect"), 0);
    }
}
