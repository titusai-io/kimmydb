//! Restore scenarios (Q6 v2.5 delta review, section 5.1, and the follow-up 2 edge H-Q6-R3b). The model has no restore and no
//! epochs; a restore is emulated with the harness' own primitives, as `q6rev/v25-delta/probe_restore.py` does: the restored
//! member N is a copy of B's state at a backup, under a new origin (Q7 D13's new epoch), B never writes again.
//!
//! * 5.1 (FIX-3): the own-origin replay. Option 1 (the build's choice): the replay is served as ordinary windows of origin B
//!   from a peer, so the sender's skip rule lets N witness an entry no live member appended. Option 2 (the mutant
//!   `m-restore-replay-raise`): a plain take of the entries a peer appended (`Position::Raise`, no window): a witness-only hole
//!   in the old epoch is never filled; N parks B's bind as information for ever, and D10 refuses writes on it.
//! * H-Q6-R3b: the member's own Q6 entry above its own servable vector (a torn backup: the state holds the write, the
//!   oplog's vector does not cover it) which no peer holds. After the restore it is marked and nobody can redeliver it. The
//!   fix: the own-origin replay releases an own-origin mark once the restored store's own oplog covers the predecessors
//!   (`Member::release_own_origin_marks`); the mutant `m-restore-own-mark-parked` does not.

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::{DocOp, Member};
use crate::oracle::finish;
use crate::scenario::Sc;
use crate::world::World;

impl Member {
    /// The restored member's own-origin replay for entries it holds in its own store above its servable vector (marked): once
    /// every earlier entry of the old origin is covered, the mark is released: the entry is covered and appended (served
    /// onward). Returns whether anything was released.
    pub fn release_own_origin_marks(
        &mut self,
        w: &mut World,
        old: usize,
        marked: &[EntryId],
    ) -> bool {
        let mut prog = false;
        let ids: Vec<EntryId> = w.by_origin[old].clone();
        for id in ids {
            if self.cv.has(id) {
                continue;
            }
            if !marked.contains(&id) {
                break; // an earlier entry the store does not hold: the replay stops below it
            }
            self.cv.add(id);
            self.appended.insert(id);
            prog = true;
        }
        if prog {
            self.settle(w);
        }
        prog
    }
}

/// What a restore scenario reports.
pub struct Restored {
    pub n_serving_stage: bool,
    pub n_covers_bind: bool,
    pub n_bind_info: bool,
    pub n_refuses: bool,
    pub n_write_accepted: bool,
    pub a_c_serve_stage: bool,
    pub problems: Vec<(String, i64)>,
}

fn new_member_from(sc: &mut Sc, backup: Member, n: usize) {
    let mut m = backup;
    m.id = n;
    m.name = sc.ms[n].name.clone();
    m.skew = sc.ms[n].skew;
    sc.ms[n] = m;
}

fn problems(sc: &mut Sc, live: &[&str]) -> Vec<(String, i64)> {
    let idx: Vec<usize> = live.iter().map(|n| sc.idx(n)).collect();
    // the end-of-run delivery would witness the hole away (`witnessed_at_quiescence`): finish is called without it
    sc.w.lost =
        sc.w.all
            .iter()
            .filter(|e| !idx.iter().any(|&i| sc.ms[i].holds(e.idx)))
            .map(|e| e.idx)
            .collect();
    let res = finish(&mut sc.w, &mut sc.ms);
    res.iter()
        .filter(|(k, v)| {
            **v != 0
                && (k.starts_with("inv_")
                    || ["diverged", "div_state", "div_loss", "alive_mismatch", "loss_missing"]
                        .contains(&k.as_str()))
        })
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

fn report(sc: &mut Sc, bind: &EntryRef, stage: &str) -> Restored {
    let n = sc.idx("N");
    let w = &sc.w;
    let m = &sc.ms[n];
    let s = m.serving();
    let refuses = s
        .as_ref()
        .is_some_and(|s| m.first_superseding(w, s).is_some() || m.info_superseding(w, s).is_some());
    let covers = m.covers(w, bind.origin, bind.hlc);
    let info = m.info.contains_key(&bind.idx);
    let serving_stage = s.as_deref() == Some(stage);
    let ac = ["A", "C"].iter().all(|x| sc.ms[sc.idx(x)].serving().as_deref() == Some(stage));
    let problems = problems(sc, &["A", "C", "N"]);
    // the write attempt comes last: an accepted write is not yet replicated and would show as a divergence
    let accepted = sc.ms[n].local_doc(&mut sc.w, 0, DocOp::Update).is_some();
    Restored {
        n_serving_stage: serving_stage,
        n_covers_bind: covers,
        n_bind_info: info,
        n_refuses: refuses,
        n_write_accepted: accepted,
        a_c_serve_stage: ac,
        problems,
    }
}

/// Section 5.1: x is witnessed and appended nowhere but at B (it loses last-writer-wins at A and C); B stages S and binds; A and
/// C flip; B crashes and is restored as N from a backup before x; the own-origin replay delivers B's entries above the backup.
pub fn restore_5_1(mode: &str) -> Restored {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("N", 0)], 60);
    let (a, b, c, n) = (sc.idx("A"), sc.idx("B"), sc.idx("C"), sc.idx("N"));
    sc.advance(1);
    sc.ms[a].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    sc.ms[a].local_doc(&mut sc.w, 7, DocOp::Insert).unwrap();
    for m in ["B", "C"] {
        sc.pull(m, "A");
    }
    sc.advance(5);
    let backup = sc.ms[b].clone(); // the backup T0
    sc.advance(20);
    let x = sc.ms[b].local_doc(&mut sc.w, 6, DocOp::Update).unwrap();
    sc.advance(21);
    sc.ms[a].local_doc(&mut sc.w, 6, DocOp::Update).unwrap();
    sc.pull("C", "A");
    sc.advance(22);
    sc.pull("A", "B");
    sc.pull("C", "B"); // x reaches A and C after the newer write: it loses, witnessed and not appended
    sc.advance(30);
    let mut job = sc.ms[b].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(34);
    let bind = sc.ms[b].do_bind(&mut sc.w, &mut job, 4).unwrap();
    sc.advance(36);
    sc.pull("A", "B");
    sc.pull("C", "B");
    assert_eq!(sc.ms[a].serving().as_deref(), Some(job.stage.as_str()));
    if sc.w.policy == crate::world::Policy::Document {
        // (under `field` the two writes merge and x is appended: the witness-only hole is a `document` shape)
        assert!(
            ![a, c].iter().any(|&i| sc.ms[i].appended.contains(&x.idx)),
            "x is appended by nobody"
        );
    }
    sc.advance(40);
    sc.vanish(&["B"]);
    new_member_from(&mut sc, backup, n); // the restore: N = B at T0, under a new origin
    sc.advance(42);
    if sc.w.f.fm_restore_raise {
        // MUTANT: a plain take of the entries a peer appended (Position::Raise): no window, no skip-rule witness
        let mut es: Vec<EntryRef> =
            sc.w.all
                .iter()
                .filter(|e| {
                    e.origin == b
                        && !sc.ms[n].cv.has(e.idx)
                        && [a, c].iter().any(|&i| sc.ms[i].appended.contains(&e.idx))
                })
                .cloned()
                .collect();
        es.sort_by_key(|e| e.stamp);
        for e in &es {
            sc.ms[n].take(&mut sc.w, e);
        }
    } else {
        sc.pull_origins("N", "A", &["B"]); // option 1: served as windows of origin B
    }
    sc.advance(46);
    sc.pull("N", "A");
    sc.pull("N", "C");
    for i in [a, c, n] {
        sc.ms[i].settle(&mut sc.w);
    }
    report(&mut sc, &bind, &job.stage)
}

/// H-Q6-R3b: B stages S and binds; no peer pulls from B before it crashes; the backup holds the bind in its state but its
/// vector does not cover B's entries from the stage's create (a torn backup): they are marked. N is restored from it.
pub fn restore_own_mark(mode: &str) -> Restored {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("N", 0)], 60);
    let (a, b, c, n) = (sc.idx("A"), sc.idx("B"), sc.idx("C"), sc.idx("N"));
    sc.advance(1);
    sc.ms[a].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    sc.ms[a].local_doc(&mut sc.w, 7, DocOp::Insert).unwrap();
    for m in ["B", "C"] {
        sc.pull(m, "A");
    }
    sc.advance(30);
    let mut job = sc.ms[b].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(34);
    let bind = sc.ms[b].do_bind(&mut sc.w, &mut job, 4).unwrap();
    let create_h =
        sc.w.all.iter().find(|e| e.kind == Kind::Create && e.phys == job.stage).unwrap().hlc;
    let marked: Vec<EntryId> =
        sc.w.all.iter().filter(|e| e.origin == b && e.hlc >= create_h).map(|e| e.idx).collect();
    let mut backup = sc.ms[b].clone();
    let mut cv = crate::coverage::PrefixCoverage::new();
    for i in backup.cv.iter().filter(|i| !marked.contains(i)) {
        cv.add(i);
    }
    backup.cv = cv;
    for i in &marked {
        backup.appended.remove(i); // the marked entries are not served
    }
    sc.advance(40);
    sc.vanish(&["B"]);
    new_member_from(&mut sc, backup, n);
    sc.advance(42);
    let marked_held = marked.clone();
    assert!(
        !sc.ms[n].covers(&sc.w, bind.origin, bind.hlc),
        "the bind is above N's servable vector"
    );
    if !sc.w.f.fm_restore_mark_parked {
        sc.ms[n].release_own_origin_marks(&mut sc.w, b, &marked_held);
    }
    sc.advance(46);
    sc.pull("A", "N");
    sc.pull("C", "N");
    sc.pull("N", "A");
    sc.pull("N", "C");
    for i in [a, c, n] {
        sc.ms[i].settle(&mut sc.w);
    }
    report(&mut sc, &bind, &job.stage)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(r: &Restored) -> Vec<&str> {
        r.problems.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// 5.1 under option 1 (windows of origin B): N witnesses the hole, covers the bind, serves the stage and accepts writes.
    /// Under option 2 (`m-restore-replay-raise`) N parks the bind as information for ever and D10 refuses its writes.
    #[test]
    fn restore_the_own_origin_replay_is_served_as_windows_with_the_skip_rule_witness() {
        {
            let pre = "";
            let r = restore_5_1(&format!("{pre}v6"));
            assert!(
                r.n_covers_bind && r.n_serving_stage && !r.n_refuses && r.n_write_accepted,
                "covers {} stage {} refuses {} accepted {} {:?}",
                r.n_covers_bind,
                r.n_serving_stage,
                r.n_refuses,
                r.n_write_accepted,
                r.problems
            );
            assert!(r.problems.is_empty(), "{:?}", r.problems);
            let m = restore_5_1(&format!("{pre}m-restore-replay-raise"));
            eprintln!(
                "m-restore-replay-raise: covers {} info {} serving stage {} refuses {} accepted {} {:?}",
                m.n_covers_bind,
                m.n_bind_info,
                m.n_serving_stage,
                m.n_refuses,
                m.n_write_accepted,
                m.problems
            );
            assert!(!m.n_covers_bind && m.n_bind_info && m.n_refuses && !m.n_write_accepted);
            assert!(names(&m).contains(&"inv_open_refusal_at_run_end"), "{:?}", m.problems);
        }
    }

    /// H-Q6-R3b: the member's own marked Q6 entries are released by the own-origin replay. Without it (`m-restore-own-mark-parked`)
    /// the restored member serves what no peer has and its peers never get the entries.
    #[test]
    fn restore_an_own_origin_mark_is_released_once_the_stores_own_oplog_covers_the_predecessors() {
        for pre in ["", "field:"] {
            let r = restore_own_mark(&format!("{pre}v6"));
            assert!(r.n_covers_bind && r.a_c_serve_stage && !r.n_refuses);
            assert!(r.problems.is_empty(), "{:?}", r.problems);
            let m = restore_own_mark(&format!("{pre}m-restore-own-mark-parked"));
            eprintln!(
                "m-restore-own-mark-parked: covers {} serving stage {} A,C stage {} refuses {} accepted {} {:?}",
                m.n_covers_bind,
                m.n_serving_stage,
                m.a_c_serve_stage,
                m.n_refuses,
                m.n_write_accepted,
                m.problems
            );
            assert!(!m.n_covers_bind && !m.a_c_serve_stage);
            assert!(!m.problems.is_empty());
        }
    }
}
