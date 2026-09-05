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
//! real files in the two tests that close that half of the file.
//!
//! The second half holds `docs/operations.md`'s log levels to the error-code
//! enum, in the same shape and for a sharper version of the same reason: that
//! section is what an operator writes an alert rule from, and a level that
//! drifts out of it does not read as stale — it reads as a page.

use std::collections::{BTreeMap, BTreeSet};

const CHANGELOG: &str = include_str!("../../../CHANGELOG.md");
const THREAT_MODEL: &str = include_str!("../../../docs/threat-model.md");
const OPERATIONS: &str = include_str!("../../../docs/operations.md");

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

// ---------------------------------------------------------------------------
// The log levels an operator writes an alert rule against
// ---------------------------------------------------------------------------
//
// ADR-136 makes the level a property of `ErrorCode`, and the reason it is a
// property of the enum rather than a table beside it is so that the list an
// operator alerts on falls out of the server instead of being copied next to
// it. A copy drifts, and this one drifts in the worst direction: a code that
// quietly starts logging `ERROR` turns a correct alert rule into a pager that
// fires on somebody else's bad input, which is exactly the defect the ADR was
// written about. So `docs/operations.md` is held to the enum here, both ways
// round — a level that disagrees, and a code that appears in neither half.

/// The heading of the section this checks, and the level it sits at.
const LEVELS_SECTION: &str = "#### What a failed request logs, and what to alert on";

/// The lines of `operations.md` under [`LEVELS_SECTION`], to the next heading.
///
/// Any heading, at any depth: the `skip(1)` has already eaten this section's
/// own, so the first `#` after it belongs to something else. A sibling `####`
/// would otherwise be absorbed, and its backticked prose read as part of the
/// silent-code list — which could paper over a code deleted from the real one.
fn levels_section(operations: &str) -> Vec<&str> {
    operations
        .lines()
        .skip_while(|line| line.trim() != LEVELS_SECTION)
        .skip(1)
        .take_while(|line| !line.starts_with('#'))
        .collect()
}

/// The `| \`code\` | \`LEVEL\` | …` rows of that section, as code and level.
///
/// Table rows only, so the prose either side can name a code and a level
/// without being read as a claim about the mapping.
fn documented_levels(section: &[&str]) -> Vec<(String, String)> {
    section
        .iter()
        .filter(|line| line.starts_with("| `"))
        .filter_map(|line| {
            let mut columns = line.split('|').skip(1);
            let code = columns.next()?.trim().trim_matches('`').to_string();
            let level = columns.next()?.trim().trim_matches('`').to_string();
            Some((code, level))
        })
        .collect()
}

/// Every backticked token in the section's prose that is one of `known`.
///
/// Filtered against the known set rather than taken whole, because the same
/// paragraph legitimately names settings and metrics in backticks. Filtering
/// cannot hide a code that was left out — a missing one simply does not appear
/// — and a code that moved from silent to logged fails the equality below.
fn silent_codes(section: &[&str], known: &BTreeSet<String>) -> BTreeSet<String> {
    section
        .iter()
        .filter(|line| !line.starts_with('|'))
        .flat_map(|line| line.split('`').skip(1).step_by(2))
        .filter(|token| known.contains(*token))
        .map(str::to_string)
        .collect()
}

#[test]
fn a_level_table_is_read_from_its_rows_and_not_from_the_prose_around_it() {
    let doc = "## Logs\n\
               \n\
               #### What a failed request logs, and what to alert on\n\
               \n\
               Alert on `ERROR`. A `bad_request` is never logged.\n\
               \n\
               | `error` | Level | Meaning |\n\
               |---|---|---|\n\
               | `internal` | `ERROR` | A fault |\n\
               | `timeout` | `WARN` | A wait |\n\
               \n\
               They are `bad_request` and `stale`, and `log.level` does not change that.\n\
               \n\
               #### A sibling at the same depth\n\
               \n\
               Prose about `not_found`, which is not part of the list above.\n\
               \n\
               ### Health\n\
               \n\
               | `not_a_code` | `ERROR` | out of the section |\n";

    let section = levels_section(doc);
    assert!(
        !section.iter().any(|line| line.contains("not_found")),
        "a sibling heading at the same depth ends the section: {section:#?}"
    );
    assert_eq!(
        documented_levels(&section),
        vec![
            ("error".to_string(), "Level".to_string()),
            ("internal".to_string(), "ERROR".to_string()),
            ("timeout".to_string(), "WARN".to_string()),
        ],
        "the header row is a row like any other; the caller drops it by looking up real codes"
    );

    // `not_found` is a real code named in the sibling section, so it is in the
    // known set precisely to prove the section boundary keeps it out.
    let known =
        ["bad_request", "stale", "internal", "not_found"].iter().map(|c| c.to_string()).collect();
    assert_eq!(
        silent_codes(&section, &known),
        BTreeSet::from(["bad_request".to_string(), "stale".to_string()]),
        "`log.level` is not a code, `internal` is named only in a row, and `not_found` is in \
         the next section"
    );
}

#[test]
fn operations_publishes_the_level_of_every_code_the_server_logs() {
    use kimmy_api::error::ErrorCode;

    let section = levels_section(OPERATIONS);
    assert!(
        !section.is_empty(),
        "{LEVELS_SECTION:?} is gone from docs/operations.md — an operator has nowhere to read \
         which failures wake them"
    );

    let documented: BTreeMap<String, String> = documented_levels(&section).into_iter().collect();
    let served: BTreeMap<String, String> = ErrorCode::ALL
        .iter()
        .filter_map(|code| {
            code.log_level().map(|level| (code.as_str().to_string(), level.to_string()))
        })
        .collect();

    for (code, level) in &served {
        let row = documented.get(code).unwrap_or_else(|| {
            panic!(
                "`{code}` is logged at {level} and has no row in {LEVELS_SECTION:?}. A code that \
                 writes a line nobody documented is one an operator meets first as a page"
            )
        });
        assert_eq!(
            row, level,
            "`{code}` is logged at {level} and documented as {row} in docs/operations.md"
        );
    }

    // And nothing is documented as logging that does not. The header row is
    // excluded by name, being the one row whose first cell is not a code.
    for code in documented.keys().filter(|c| c.as_str() != "error") {
        assert!(
            served.contains_key(code),
            "docs/operations.md gives `{code}` a level, but the server either does not log it or \
             has no such code"
        );
    }
}

#[test]
fn operations_names_every_code_that_is_never_logged() {
    // The half an alert rule actually rests on. A caller sending refusals this
    // API documents must not be able to move a member's error count, and an
    // operator has to be able to see which codes those are without reading
    // `error.rs`.
    use kimmy_api::error::ErrorCode;

    let known: BTreeSet<String> = ErrorCode::ALL.iter().map(|c| c.as_str().to_string()).collect();
    let section = levels_section(OPERATIONS);

    let documented = silent_codes(&section, &known);
    let served: BTreeSet<String> = ErrorCode::ALL
        .iter()
        .filter(|code| code.log_level().is_none())
        .map(|code| code.as_str().to_string())
        .collect();

    assert_eq!(
        documented,
        served,
        "the codes docs/operations.md says are never logged and the codes the server never logs \
         disagree. Missing from the document: {:?}. Named there but logged: {:?}",
        served.difference(&documented).collect::<Vec<_>>(),
        documented.difference(&served).collect::<Vec<_>>(),
    );
}
