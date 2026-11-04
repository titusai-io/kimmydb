//! The `field` policy (phase F): a physical's document is a `kimmy_core::merge_state::MergeState`, an entry carries the
//! writer's whole state after the write (Q7 D1/D11), apply is the join, and a late write is reported per lost head.
//! Everything here drives `merge_state` as it is on q7 (`Writer`, `join`, `MergeState`); nothing there is changed.
//!
//! Design: `kimmydb-reviews-0470-0480/q6-phaseF-design.md`. Decisions the note marks ASSUMED are marked here too.

use std::collections::BTreeMap;
use std::rc::Rc;

use kimmy_core::merge_state::{
    JoinOptions, LossAt, LossValue, MStamp, MergeState, Path, Value, Writer, join,
};

use crate::entry::*;

/// RULED (the manager, after phase F report 1): an insert's own field cells are covered by the whole-insert write (Q7 D10 (1),
/// made the `field` contract by Chris's note 3), so the REQUIRED set holds the insert's document-level write only. The one place
/// to change if the ruling changes: `false` requires every field cell of an insert as well (the allowed set holds them anyway).
pub const REQUIRE_INSERT_FIELD_CELLS: bool = false;

/// The cut of a loss record's value (Chris's L34-B ruling: 64 B, per value, with a `truncated` flag).
pub const VALUE_CUT: usize = 64;

pub fn p(name: &str) -> Path {
    Path::flat(name).expect("a valid field name")
}

/// A value as the harness renders it (a deterministic stand-in for the wire form, which is Step 4's).
pub fn render(v: &Value) -> String {
    match v {
        Value::String(s) => format!("{s:?}"),
        other => format!("{other:?}"),
    }
}

/// Cut `s` to at most `max` bytes at a character boundary; the flag says it was cut.
pub fn cut(s: &str, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s.to_string(), false);
    }
    let mut i = max;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    (s[..i].to_string(), true)
}

/// The visible document of a state as a body: `path=value` lines in path order, `None` for a deleted document.
pub fn body_of(st: &MergeState, o: &JoinOptions) -> Body {
    st.visible(o).map(|m| {
        Rc::new(
            m.iter()
                .map(|(path, v)| format!("{}={}", path.segments().join("."), render(v)))
                .collect(),
        )
    })
}

/// A version from a state and the stamp of the latest write merged into it.
pub fn ver_of(st: Rc<MergeState>, stamp: Stamp, o: &JoinOptions) -> Ver {
    Ver { stamp, body: body_of(&st, o), state: Some(st), applied: None }
}

/// Does `a`'s version vector cover every entry of `b`'s? (The covered-entry skip: "skip iff the receiver's vv covers the
/// entry's whole vv", frozen.)
pub fn vv_covers(a: &MergeState, b: &MergeState) -> bool {
    b.vv.iter().all(|(w, h)| a.vv.get(w).is_some_and(|x| x >= h))
}

/// What an entry did to the version it was applied to.
pub struct Applied {
    pub ver: Ver,
    /// the join changed the joined state (Q7 D11: "if unchanged only witness"): the entry is appended
    pub changed: bool,
    /// the entry changed nothing only because the page states of an unfinished join already cover it (the gap-1
    /// analogue, ASSUMED in the design note, section 1.3)
    pub held_state: bool,
    /// The entry's stamp is the held version's stamp: the same write, held as a page state (the model's equal stamp: "the same
    /// entry", `wins = e.stamp >= cur[0]`); appended as a winner whatever the append-in-position flag. A write whose elements
    /// survive in the joined state under a later version (the version stamp is higher) is not this: that is the gap-1 case
    pub same_write: bool,
    /// the covered-entry skip fired
    pub skipped: bool,
    /// the records the join detected that were new to the joined state (Q7's family)
    pub fresh: Vec<(kimmy_core::merge_state::LossKey, LossValue)>,
}

/// Apply the entry `e` to the version `cur` (`None`: the document is unknown here). `missing`: the physical is a
/// placeholder of an unfinished join, so everything in it came from pages.
pub fn apply_entry(
    cur: Option<&Ver>,
    e: &Entry,
    o: &JoinOptions,
    missing: bool,
    stale_stamp: bool,
) -> Applied {
    let es = e.fstate.as_ref().expect("a field entry carries its state");
    let Some(cur) = cur else {
        return Applied {
            ver: ver_of(es.clone(), e.stamp, o),
            changed: true,
            held_state: false,
            same_write: false,
            skipped: false,
            fresh: es.losses.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        };
    };
    let base = cur.state.as_ref().expect("a field version carries its state");
    let covered = vv_covers(base, es);
    if covered {
        // the covered-entry skip decides "witnessed only"; the self-check of the design: it never hides a change
        debug_assert!(
            unchanged_but_records(&join(base, es, o).0, base),
            "a skipped entry would have changed the state (beyond the records its join detected)"
        );
        // unchanged only because pages cover it: a placeholder is all page state; an alive physical keeps `applied`
        let held_state =
            if cur.applied.is_some() { applied_would_change(cur, es, o) } else { missing };
        return Applied {
            ver: cur.clone(),
            changed: false,
            held_state,
            same_write: cur.stamp == e.stamp,
            skipped: true,
            fresh: Vec::new(),
        };
    }
    let (merged, fresh) = join(base, es, o);
    let changed = merged != **base;
    let (applied, held_state) = match &cur.applied {
        Some(a) => {
            let (ma, _) = join(a, es, o);
            let held = !changed && ma != **a;
            (Some(Rc::new(ma)), held)
        }
        None => (None, missing && !changed),
    };
    // `stale_stamp` is the mutant `m-field-stale-version-stamp`: the version keeps its old stamp after a join
    let stamp = if e.stamp > cur.stamp && !stale_stamp { e.stamp } else { cur.stamp };
    let new_fresh: Vec<_> = fresh
        .iter()
        .filter(|k| !base.losses.contains_key(*k))
        .filter_map(|k| merged.losses.get(k).map(|v| (k.clone(), v.clone())))
        .collect();
    Applied {
        ver: Ver { stamp, body: body_of(&merged, o), state: Some(Rc::new(merged)), applied },
        changed,
        held_state,
        same_write: cur.stamp == e.stamp,
        skipped: false,
        fresh: new_fresh,
    }
}

/// Equal states, the loss records apart: the records a join detects converge through the loss entries, not through the
/// join (Q7 D10), so a skipped entry may carry records the receiver lacks.
fn unchanged_but_records(a: &MergeState, b: &MergeState) -> bool {
    let (mut a, mut b) = (a.clone(), b.clone());
    a.losses.clear();
    b.losses.clear();
    a == b
}

fn applied_would_change(cur: &Ver, es: &MergeState, o: &JoinOptions) -> bool {
    match &cur.applied {
        Some(a) => join(a, es, o).0 != **a,
        None => false,
    }
}

/// Merge a snapshot page's version into the version held here. While the join is unfinished (`ungranted`) the page state
/// is held out of position: `applied` keeps the state the in-position entries built.
pub fn merge_page(
    cur: &Ver,
    page: &Ver,
    ungranted: bool,
    o: &JoinOptions,
) -> (Ver, Vec<(kimmy_core::merge_state::LossKey, LossValue)>) {
    let (cs, ps) = (cur.state.as_ref().expect("state"), page.state.as_ref().expect("state"));
    let (merged, fresh) = join(cs, ps, o);
    let fresh: Vec<_> = fresh
        .iter()
        .filter(|k| !cs.losses.contains_key(*k))
        .filter_map(|k| merged.losses.get(k).map(|v| (k.clone(), v.clone())))
        .collect();
    let applied = if ungranted {
        Some(cur.applied.clone().unwrap_or_else(|| cs.clone()))
    } else {
        cur.applied.clone()
    };
    let stamp = if page.stamp > cur.stamp { page.stamp } else { cur.stamp };
    (Ver { stamp, body: body_of(&merged, o), state: Some(Rc::new(merged)), applied }, fresh)
}

/// The records of a page version that are new to a member that held no version of the document.
pub fn page_records(v: &Ver) -> Vec<(kimmy_core::merge_state::LossKey, LossValue)> {
    v.state
        .as_ref()
        .map(|s| s.losses.iter().map(|(k, x)| (k.clone(), x.clone())).collect())
        .unwrap_or_default()
}

/// A write of a state that some cell of which is after `after`: one lost cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LateCell {
    /// `None`: the whole document (a delete)
    pub path: Option<Path>,
    pub stamp: Stamp,
    /// the lost value, rendered (not yet cut)
    pub value: String,
}

fn is_marker(v: &Value) -> bool {
    matches!(v, Value::Map | Value::ImplicitMap)
}

fn render_loss(v: &LossValue) -> String {
    match v {
        LossValue::Value(x) => render(x),
        LossValue::Total(n) => format!("{n:?}"),
        LossValue::Fields(f) => format!("{f:?}"),
    }
}

/// The writes of a state that are after `after` (the earliest superseding event): its registers, its counter slots, its
/// deletes and the losers its join recorded (`MergeState.losses`: a loss record names a write whose cell the state no
/// longer holds). `only`: restrict to the cells of one write (an entry's own write); with `None` every late cell (a page
/// version, ASSUMED: evaluated cell by cell, design note 1.4).
pub fn late_cells(st: &MergeState, after: Stamp, only: Option<Stamp>) -> Vec<LateCell> {
    let want = |s: &MStamp| *s > after && only.is_none_or(|o| o == *s);
    let mut out: BTreeMap<(Option<Path>, Stamp), String> = BTreeMap::new();
    for ((_, path), r) in st.regs.iter() {
        if want(&r.stamp) && !is_marker(&r.value) {
            out.entry((Some(path.clone()), r.stamp)).or_insert_with(|| render(&r.value));
        }
    }
    for ((_, path, _, _), s) in st.slots.iter() {
        if want(&s.stamp) {
            out.entry((Some(path.clone()), s.stamp)).or_insert_with(|| format!("{:?}", s.total));
        }
    }
    // an insert is also a write of the whole document (Q7's `insertLost` names it at the document level, with the field
    // list); a life's stamp is the insert's stamp
    for (ls, life) in st.lives.iter() {
        if want(ls) {
            out.entry((None, *ls)).or_insert_with(|| format!("{:?}", life.order));
        }
    }
    for d in st.deletes.keys() {
        if want(d) {
            out.entry((None, *d)).or_insert_with(|| "deleted".to_string());
        }
    }
    if only.is_none() {
        for (k, v) in st.losses.iter() {
            if want(&k.stamp) {
                let path = match &k.at {
                    LossAt::Document => None,
                    LossAt::Field(p) => Some(p.clone()),
                };
                out.entry((path, k.stamp)).or_insert_with(|| render_loss(v));
            }
        }
    }
    out.into_iter().map(|((path, stamp), value)| LateCell { path, stamp, value }).collect()
}

/// The body of a loss record: the lost value cut at 64 B, with a `truncated` marker line when it was cut.
pub fn record_body(value: &str) -> Body {
    let (v, truncated) = cut(value, VALUE_CUT);
    let mut lines = vec![v];
    if truncated {
        lines.push("truncated".to_string());
    }
    Some(Rc::new(lines))
}

/// Run one local write on a document state with the writer `id`, the stamp coming from the harness member. `None`: a no-op
/// or a refusal (nothing changed, no stamp spent as far as the state goes).
pub fn local_write(
    id: usize,
    state: Option<&Rc<MergeState>>,
    op: crate::member::DocOp,
    h: i64,
    mark: &str,
    o: &JoinOptions,
    counter: bool,
) -> Result<Option<Rc<MergeState>>, kimmy_core::merge_state::StampError> {
    use crate::member::DocOp;
    let mut wr =
        Writer::from_state(writer(id), state.map(|s| (**s).clone()).unwrap_or_default(), o.clone());
    let st = stamp(h, id);
    let mut mint = || st;
    let r = match op {
        DocOp::Insert => wr.insert_stamped(&[(p("v"), Value::String(mark.to_string()))], &mut mint),
        DocOp::Update if counter => wr.write_inc_stamped(&p("n"), 1, &mut mint),
        DocOp::Update => {
            let old = wr.visible().and_then(|m| m.get(&p("v")).cloned());
            let nv = match old {
                Some(Value::String(s)) => format!("{s}|{mark}"),
                _ => mark.to_string(),
            };
            wr.write_set_stamped(&p("v"), Value::String(nv), &mut mint)
        }
        DocOp::Delete => wr.write_delete_stamped(&mut mint),
    };
    match r {
        Ok(Some(_)) => Ok(Some(Rc::new(wr.state))),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Is the document alive in this state?
pub fn alive(st: &MergeState, o: &JoinOptions) -> bool {
    st.visible(o).is_some()
}
