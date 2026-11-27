//! The register of a logical name as a function of its facts: `kimmy_core::register_function`.
//!
//! `table.rs` holds the definition's answers for sets of facts (named rows, drawn sets and a constructed family of the dead binding;
//! generated from `u14_fold.py`, its sha in the table's header), so that the product's function and the definition cannot drift. Every
//! case is in exactly one of three groups by the facts it holds, one test each: no stage fact (`plain`), stage facts and no database drop
//! (`staged`), and both (`dbdrop`). The named rows are a second look at cases that are already in those groups. The rest: a database
//! drop's own entry changes nothing, the order of the facts says nothing, the table reaches the branches it is meant to, a tie in
//! one `hlc` is the drop's, and which entry kinds are a touch.

// no case holds a stage's withdrawal (`Kind::Abandon`): it is in the table's types so that their form does not change with its rule
#[allow(dead_code)]
mod table;

use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::register_function::{
    Answer, FoldEvent, FoldFact, FoldKind, Serves, fold, is_a_touch,
};
use kimmy_core::{Epoch, Hlc, NodeId, OpKind, StageInfo, Stamp};

use table::{CASES, Case, Fact, Kind, MODEL_SHA, Served};

/// The identity's name in every case of the table.
const IDENTITY: &str = "I";

fn st(hlc: u64, origin: usize) -> Stamp {
    Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([origin as u8; 16]), Epoch::ZERO)
}

fn fact(f: &Fact) -> FoldFact {
    let physical = || f.name.to_string();
    let kind = match f.kind {
        Kind::Create => FoldKind::Create,
        Kind::Drop => FoldKind::Drop { database_drop: f.db_entry.map(|hlc| Hlc::new(hlc, 0)) },
        Kind::DbDrop => FoldKind::DatabaseDrop,
        Kind::StageCreate => FoldKind::StageCreate {
            physical: physical(),
            stage: StageInfo { deadline: Hlc::new(f.deadline, 0) },
        },
        Kind::Bind => FoldKind::Bind { physical: physical() },
        Kind::Recreate => FoldKind::Recreate { physical: physical() },
        Kind::Touch => FoldKind::Touch,
        Kind::Abandon => panic!("no case holds a stage's withdrawal: it has no rule yet"),
    };
    FoldFact { stamp: st(f.hlc, f.origin), kind }
}

fn facts_of(case: &Case) -> Vec<FoldFact> {
    case.facts.iter().map(fact).collect()
}

/// The answer the table gives for `case`. The identity is in `alive` there when it stands; its entry in `dead` is left out, since the
/// stamp the identity ends at is each member's own.
fn expected(case: &Case) -> Answer {
    let serves = match case.serves {
        Served::Absent => Serves::Absent,
        Served::Identity => Serves::Identity,
        Served::Stage(p) => Serves::Stage(p.to_string()),
        Served::Recreation(p) => Serves::Recreation(p.to_string()),
    };
    let register = case
        .register
        .iter()
        .map(|&(kind, hlc, origin, p)| {
            let event = match kind {
                "Bind" => FoldEvent::Bind { physical: p.to_string() },
                "Recreate" => FoldEvent::Recreate { physical: p.to_string() },
                "Drop" => FoldEvent::Drop,
                "DbDrop" => FoldEvent::DatabaseDrop,
                other => panic!("{other}"),
            };
            (st(hlc, origin), event)
        })
        .collect();
    Answer {
        serves,
        floor: case.life.map(|(hlc, origin)| st(hlc, origin)),
        identity_stands: case.alive.iter().any(|&(p, _)| p == IDENTITY),
        alive: case
            .alive
            .iter()
            .filter(|&&(p, _)| p != IDENTITY)
            .map(|&(p, created)| (p.to_string(), Hlc::new(created, 0)))
            .collect(),
        dead: case
            .dead
            .iter()
            .filter(|&&(p, _, _)| p != IDENTITY)
            .map(|&(p, hlc, origin)| (p.to_string(), st(hlc, origin)))
            .collect(),
        register,
        superseded: case
            .superseded
            .iter()
            .map(|&(p, above)| (p.to_string(), above.map(|(hlc, origin)| st(hlc, origin))))
            .collect(),
        invalid: case
            .invalid
            .iter()
            .map(|&i| st(case.facts[i].hlc, case.facts[i].origin))
            .collect(),
    }
}

/// The columns in which `got` differs from what the table gives for `case`; empty when it agrees. `late` is the case's write probes.
fn differences(case: &Case, got: &Answer) -> Vec<&'static str> {
    let want = expected(case);
    // taken apart whole: a new field of the answer does not compile until it is compared here
    let Answer { serves, floor, identity_stands, alive, dead, register, superseded, invalid } =
        want;
    let mut out = Vec::new();
    let mut differs = |column: &'static str, is: bool| {
        if is {
            out.push(column);
        }
    };
    differs("serves", got.serves != serves);
    differs("life", got.floor != floor);
    differs("alive (the identity)", got.identity_stands != identity_stands);
    differs("alive", got.alive != alive);
    differs("dead", got.dead != dead);
    differs("register", got.register != register);
    differs("superseded", got.superseded != superseded);
    differs("invalid", got.invalid != invalid);
    // a write probe carries an hlc and no origin: every probe's hlc is apart from the stamp it is judged against
    differs("late", case.late.iter().any(|&(p, hlc, late)| got.is_late(p, &st(hlc, 0)) != late));
    out
}

/// Every case `of` selects answers as the table says, and there are at least `at_least` of them.
fn the_cases(label: &str, at_least: usize, of: impl Fn(&Case) -> bool) {
    let mut total = 0;
    let mut failed = Vec::new();
    for (i, case) in CASES.iter().enumerate().filter(|(_, case)| of(case)) {
        total += 1;
        let columns = differences(case, &fold(IDENTITY, &facts_of(case)));
        if !columns.is_empty() {
            failed.push(format!("case {i} {:?}: {columns:?}", case.tag));
        }
    }
    assert!(total >= at_least, "{total} {label} cases, at least {at_least} expected");
    assert!(
        failed.is_empty(),
        "{} of {total} {label} cases differ; the first: {:#?}",
        failed.len(),
        &failed[..failed.len().min(10)]
    );
}

fn holds(case: &Case, kind: Kind) -> bool {
    case.facts.iter().any(|f| f.kind == kind)
}

fn has_a_stage_fact(case: &Case) -> bool {
    holds(case, Kind::StageCreate) || holds(case, Kind::Bind) || holds(case, Kind::Recreate)
}

fn is_plain(case: &Case) -> bool {
    !has_a_stage_fact(case)
}

fn is_staged(case: &Case) -> bool {
    has_a_stage_fact(case) && !holds(case, Kind::DbDrop)
}

fn is_dbdrop(case: &Case) -> bool {
    has_a_stage_fact(case) && holds(case, Kind::DbDrop)
}

#[test]
fn the_partition_covers_every_case_once() {
    assert_eq!(MODEL_SHA, "1017347f61df4ce5", "the definition the table was generated from");
    let mut counts = [0usize; 3];
    for (i, case) in CASES.iter().enumerate() {
        let of = [is_plain(case), is_staged(case), is_dbdrop(case)];
        assert_eq!(of.iter().filter(|&&is| is).count(), 1, "case {i} {:?}: {of:?}", case.tag);
        for (count, is) in counts.iter_mut().zip(of) {
            *count += usize::from(is);
        }
    }
    assert_eq!(counts.iter().sum::<usize>(), CASES.len());
    // 329, 331 and 423 as generated
    assert!(counts[0] >= 260 && counts[1] >= 260 && counts[2] >= 330, "{counts:?}");
}

#[test]
fn plain_cases() {
    the_cases("plain", 260, is_plain);
}

#[test]
fn staged_cases() {
    the_cases("staged", 260, is_staged);
}

#[test]
fn dbdrop_cases() {
    the_cases("dbdrop", 330, is_dbdrop);
}

#[test]
fn named_rows() {
    the_cases("named", 23, |case| !case.tag.is_empty());
}

#[test]
fn a_db_entry_fact_changes_nothing() {
    let mut compared = 0;
    for (i, case) in CASES.iter().enumerate() {
        if case.facts.iter().all(|f| f.db_entry.is_none()) {
            continue;
        }
        compared += 1;
        let without: Vec<FoldFact> =
            case.facts.iter().filter(|f| f.db_entry.is_none()).map(fact).collect();
        assert_eq!(
            fold(IDENTITY, &facts_of(case)),
            fold(IDENTITY, &without),
            "case {i} {:?}: with and without the database drop's own entries",
            case.tag
        );
    }
    assert!(compared >= 100, "{compared} cases hold a database drop's own entry");
}

/// A small deterministic generator.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

#[test]
fn the_function_answers_alike_in_every_order_of_the_facts() {
    let mut random = Lcg(20_261_127);
    let (mut shuffles, mut reordered) = (0, 0);
    let mut failed = Vec::new();
    for (i, case) in CASES.iter().enumerate() {
        let listed = facts_of(case);
        for round in 0..3 {
            let mut facts = listed.clone();
            for last in (1..facts.len()).rev() {
                facts.swap(last, random.next(last as u64 + 1) as usize);
            }
            shuffles += 1;
            reordered += usize::from(facts != listed);
            let columns = differences(case, &fold(IDENTITY, &facts));
            if !columns.is_empty() {
                failed.push(format!("case {i} {:?}, shuffle {round}: {columns:?}", case.tag));
            }
        }
    }
    assert!(reordered * 2 > shuffles, "{reordered} of {shuffles} shuffles changed the order");
    assert!(
        failed.is_empty(),
        "{} of {shuffles} shuffled cases differ; the first: {:#?}",
        failed.len(),
        &failed[..failed.len().min(10)]
    );
}

// ---------------------------------------------------------------------------------------------------------------------
// the table reaches what it is meant to

/// The stage facts of `case`.
fn stages(case: &Case) -> impl Iterator<Item = &Fact> {
    case.facts.iter().filter(|f| f.kind == Kind::StageCreate)
}

/// The drops of the collection itself, a database drop's own entries apart.
fn drops(case: &Case) -> impl Iterator<Item = &Fact> {
    case.facts.iter().filter(|f| f.kind == Kind::Drop && f.db_entry.is_none())
}

fn database_drops(case: &Case) -> impl Iterator<Item = &Fact> {
    case.facts.iter().filter(|f| f.kind == Kind::DbDrop)
}

fn binding_of<'a>(case: &'a Case, stage: &Fact) -> Option<&'a Fact> {
    case.facts.iter().find(|f| f.kind == Kind::Bind && f.name == stage.name)
}

#[test]
fn the_function_table_exercises_every_branch_it_is_meant_to() {
    // each count is of cases; the minimum is about four fifths of the table header's, so that other draws do not break it
    let at_least = |branch: &str, minimum: usize, of: &dyn Fn(&Case) -> bool| {
        let count = CASES.iter().filter(|case| of(case)).count();
        assert!(count >= minimum, "{branch}: {count} cases, at least {minimum} expected");
    };
    at_least("a drop below a stage's creation", 130, &|c| {
        stages(c).any(|s| drops(c).any(|d| d.hlc < s.hlc))
    });
    at_least("a drop above a stage's creation", 330, &|c| {
        stages(c).any(|s| drops(c).any(|d| d.hlc > s.hlc))
    });
    at_least("a database drop below a stage's creation", 120, &|c| {
        stages(c).any(|s| database_drops(c).any(|d| d.hlc < s.hlc))
    });
    at_least("a database drop between a stage's creation and its binding", 200, &|c| {
        stages(c).any(|s| {
            binding_of(c, s)
                .is_some_and(|b| database_drops(c).any(|d| s.hlc <= d.hlc && d.hlc < b.hlc))
        })
    });
    at_least("a database drop above a binding", 120, &|c| {
        stages(c)
            .any(|s| binding_of(c, s).is_some_and(|b| database_drops(c).any(|d| d.hlc > b.hlc)))
    });
    at_least("a database drop above an unbound stage", 85, &|c| {
        stages(c).any(|s| binding_of(c, s).is_none() && database_drops(c).any(|d| d.hlc > s.hlc))
    });
    // The next two are counted by the generator as the cases that change when the rule is removed; counted here as the cases that
    // hold the shape the rule is about, which is at least as many.
    at_least("a valid binding of a stage a database drop ended", 50, &|c| {
        c.register.iter().any(|&(kind, bound, _, p)| {
            kind == "Bind"
                && stages(c)
                    .filter(|s| s.name == p)
                    .any(|s| database_drops(c).any(|d| s.hlc <= d.hlc && d.hlc < bound))
        })
    });
    at_least("an ordinary creation above an event of the register", 240, &|c| {
        let first = c.register.iter().map(|&(_, hlc, _, _)| hlc).min();
        first.is_some_and(|first| c.facts.iter().any(|f| f.kind == Kind::Create && f.hlc > first))
    });
    at_least("a creation stamped inside a database drop's own entry", 120, &|c| {
        c.facts.iter().any(|entry| {
            entry.db_entry.is_some_and(|of| {
                c.facts.iter().any(|f| {
                    matches!(f.kind, Kind::Create | Kind::Recreate)
                        && of < f.hlc
                        && f.hlc < entry.hlc
                })
            })
        })
    });
    at_least("an invalid binding", 400, &|c| !c.invalid.is_empty());
    at_least(
        "a binding from another origin than its stage's creation, by its deadline",
        35,
        &|c| {
            c.invalid.iter().any(|&i| {
                let b = &c.facts[i];
                stages(c).any(|s| s.name == b.name && s.origin != b.origin && b.hlc <= s.deadline)
            })
        },
    );
    at_least("two database drops", 100, &|c| database_drops(c).count() >= 2);
    at_least("more than one stage", 360, &|c| stages(c).count() >= 2);
    at_least("a case that begins with no collection", 120, &|c| {
        !c.facts.iter().any(|f| f.kind == Kind::Create && f.hlc == 0)
    });
    // the three touch branches, again by the shape each is about
    at_least("a touch in a case that leaves the identity standing", 160, &|c| {
        holds(c, Kind::Touch) && c.alive.iter().any(|&(p, _)| p == IDENTITY)
    });
    at_least("a touch below a drop", 230, &|c| {
        c.facts
            .iter()
            .filter(|t| t.kind == Kind::Touch)
            .any(|t| drops(c).chain(database_drops(c)).any(|d| d.hlc > t.hlc))
    });
    at_least("a touch of a name with an event in its register", 120, &|c| {
        holds(c, Kind::Touch) && !c.register.is_empty()
    });
    // every case's facts are apart in their hlcs, which the stamps of the answer and the write probes rely on
    for (i, case) in CASES.iter().enumerate() {
        let hlcs: BTreeSet<u64> = case.facts.iter().map(|f| f.hlc).collect();
        assert_eq!(hlcs.len(), case.facts.len(), "case {i} {:?}: two facts at one hlc", case.tag);
        let named: BTreeMap<&str, u64> = case.alive.iter().copied().collect();
        assert_eq!(named.len(), case.alive.len(), "case {i}: a physical alive twice");
        if let Some(&created) = named.get(IDENTITY) {
            assert_eq!(created, 0, "case {i}: the identity carries no creation stamp");
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// a tie in one hlc

/// A tie in one `hlc` is the drop's (ADR-236), whichever of the two origins sorts first: a creation or a touch at the drop's `hlc`
/// is of the life the drop ended, and one a tick above it is not. The table holds one fact per `hlc`, so it cannot say this.
#[test]
fn a_tie_in_one_hlc_is_the_drops() {
    for drop in [FoldKind::Drop { database_drop: None }, FoldKind::DatabaseDrop] {
        for other in [FoldKind::Create, FoldKind::Touch] {
            for (dropper, writer) in [(1, 2), (2, 1)] {
                for (hlc, stands) in [(10, false), (11, true)] {
                    let mut facts = vec![
                        FoldFact { stamp: st(1, 0), kind: FoldKind::Create },
                        FoldFact { stamp: st(10, dropper), kind: drop.clone() },
                        FoldFact { stamp: st(hlc, writer), kind: other.clone() },
                    ];
                    for listed in ["as listed", "reversed"] {
                        let what = format!(
                            "{drop:?} of origin {dropper} at 10, {other:?} of origin {writer} at {hlc}, {listed}"
                        );
                        let answer = fold(IDENTITY, &facts);
                        assert_eq!(answer.identity_stands, stands, "{what}");
                        let serves = if stands { Serves::Identity } else { Serves::Absent };
                        assert_eq!(answer.serves, serves, "{what}");
                        assert_eq!(answer.floor, Some(st(10, dropper)), "{what}");
                        facts.reverse();
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// what a touch is

#[test]
fn a_touch_is_a_document_write_a_delete_or_a_change_of_definitions() {
    for (kind, touch) in [
        (OpKind::Insert, true),
        (OpKind::Update, true),
        (OpKind::Replace, true),
        (OpKind::Delete, true),
        (OpKind::CreateIndex, true),
        (OpKind::DropIndex, true),
        (OpKind::ConfigureValidator, true),
        (OpKind::ConfigureVectors, true),
        (OpKind::Collection, false),
        (OpKind::UniqueViolation, false),
        (OpKind::CreateCollection, false),
        (OpKind::DropCollection, false),
        (OpKind::UnkeyedDocument, false),
        (OpKind::LossRecord, false),
        (OpKind::BindCollection, false),
        (OpKind::AbandonStage, false),
        (OpKind::OutLossRecord, false),
    ] {
        assert_eq!(is_a_touch(kind), touch, "{kind:?}");
    }
}

#[test]
fn a_touch_with_no_creation_and_no_floor_makes_nothing() {
    let answer = fold(IDENTITY, &[FoldFact { stamp: st(10, 0), kind: FoldKind::Touch }]);
    assert_eq!(answer.serves, Serves::Absent);
    assert!(!answer.identity_stands, "{answer:?}");
    assert!(answer.alive.is_empty() && answer.register.is_empty(), "{answer:?}");
    assert_eq!(answer.floor, None);
}
