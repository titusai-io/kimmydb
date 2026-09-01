//! Documentation this repository checks rather than only writes down.
//!
//! [docs/compatibility.md](../../../docs/compatibility.md) draws the line
//! between mechanism and prose, and says why it matters: this project has been
//! wrong before about claims nothing checked. The threat model is prose, and
//! ADR-110 named the mitigation for prose going stale — file references, and
//! *next release* markers removed once the settings ship. The second half of
//! that was a convention nobody enforced, and 0.17.0 shipped with seven rows
//! still promising controls the release had already delivered.
//!
//! An unswept marker is not a cosmetic staleness. It reads as a control the
//! operator does not have yet, which is the direction a threat model must not
//! be wrong in: it invites somebody to build a compensating control they do
//! not need, or to conclude the node is weaker than it is and not deploy it.
//!
//! The guard below only bites on a release commit, which would leave it
//! unexercised on every other one — so the two decisions it rests on are pure
//! functions over their input, tested against fixtures here, and applied to the
//! real files in the two tests at the end.

const CHANGELOG: &str = include_str!("../../../CHANGELOG.md");
const THREAT_MODEL: &str = include_str!("../../../docs/threat-model.md");

/// The marker ADR-110 puts on a control that is merged but not yet released.
const MARKER: &str = "next release";

/// The heading that means the top of the changelog is still accumulating.
const UNRELEASED: &str = "## Unreleased";

/// Whether the newest changelog section is a dated release rather than the open
/// `## Unreleased` one.
///
/// This is the whole condition. While work is unreleased the markers are
/// correct and must be left alone; the moment a release is dated they are
/// claims about a version somebody can already run.
fn at_a_dated_release(changelog: &str) -> bool {
    let first_section =
        changelog.lines().find(|line| line.starts_with("## ")).expect("the changelog has sections");
    first_section.trim() != UNRELEASED
}

/// The threat table rows still carrying the marker, as `line number: text`.
///
/// Table rows only. The document's preamble explains the convention and so
/// names the marker legitimately; the markers themselves live in the control
/// column of the threat tables, which is what a reader acts on.
fn unswept_markers(threat_model: &str) -> Vec<String> {
    threat_model
        .lines()
        .enumerate()
        .filter(|(_, line)| line.trim_start().starts_with('|'))
        .filter(|(_, line)| line.to_lowercase().contains(MARKER))
        .map(|(n, line)| format!("  docs/threat-model.md:{}: {}", n + 1, line.trim()))
        .collect()
}

#[test]
fn an_open_unreleased_section_is_not_a_dated_release() {
    assert!(!at_a_dated_release(
        "# Changelog\n\nprose\n\n## Unreleased\n\n## 0.17.0 - 2026-08-31\n"
    ));
}

#[test]
fn a_dated_section_at_the_top_is_a_dated_release() {
    assert!(at_a_dated_release("# Changelog\n\nprose\n\n## 0.17.0 - 2026-08-31\n\n## 0.16.4\n"));
}

#[test]
fn the_marker_is_found_in_a_row_and_ignored_in_the_prose() {
    // The exact shape 0.17.0 shipped: a preamble that explains the marker, and
    // rows that still carry it. Only the rows are findings.
    let doc = "Several controls below are marked **next release**.\n\
               \n\
               | Threat | Control | Where |\n\
               |---|---|---|\n\
               | Something | **Next release:** `a.setting` (ADR-000) | `x.rs` |\n\
               | Cased differently | **next release:** `b.setting` | `y.rs` |\n\
               | Already shipped | `c.setting` refuses it | `z.rs` |\n";

    let found = unswept_markers(doc);
    assert_eq!(found.len(), 2, "{found:#?}");
    assert!(found[0].contains("a.setting"), "{found:#?}");
    assert!(found[1].contains("b.setting"), "{found:#?}");
    // Line numbers are 1-indexed so the message can be pasted into an editor.
    assert!(found[0].starts_with("  docs/threat-model.md:5:"), "{found:#?}");
}

#[test]
fn a_swept_table_is_clean() {
    let doc = "| Threat | Control | Where |\n| Something | `a.setting` refuses it | `x.rs` |\n";
    assert!(unswept_markers(doc).is_empty());
}

#[test]
fn a_dated_release_leaves_no_next_release_markers_in_the_threat_model() {
    if !at_a_dated_release(CHANGELOG) {
        return;
    }

    let unswept = unswept_markers(THREAT_MODEL);
    assert!(
        unswept.is_empty(),
        "the changelog's newest section is a dated release, so every control in the threat \
         model has shipped, but {} row(s) still promise one for a later one. Sweep them to \
         the present tense as part of cutting the release (ADR-110):\n{}",
        unswept.len(),
        unswept.join("\n")
    );
}

#[test]
fn the_threat_model_still_explains_what_the_marker_means() {
    // `unswept_markers` is scoped to table rows precisely so the preamble can go
    // on describing the convention. If that description is ever removed the
    // scoping becomes arbitrary rather than deliberate, and the next person to
    // read it cannot tell which it was.
    let preamble: String = THREAT_MODEL
        .lines()
        .take_while(|line| !line.trim_start().starts_with('|'))
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        preamble.to_lowercase().contains(MARKER),
        "the threat model no longer explains the {MARKER:?} marker, so the row-scoped check \
         has nothing to exempt and should be simplified to the whole document"
    );
}
