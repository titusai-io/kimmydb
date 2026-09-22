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
const VECTORS: &str = include_str!("../../../docs/vectors.md");

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

/// The lines of `operations.md` under `heading`, to the next heading.
///
/// Any heading, at any depth: the `skip(1)` has already eaten this section's
/// own, so the first `#` after it belongs to something else. A sibling `####`
/// would otherwise be absorbed, and its backticked prose read as part of the
/// silent-code list — which could paper over a code deleted from the real one.
fn section<'a>(operations: &'a str, heading: &str) -> Vec<&'a str> {
    operations
        .lines()
        .skip_while(|line| line.trim() != heading)
        .skip(1)
        .take_while(|line| !line.starts_with('#'))
        .collect()
}

/// The lines of `operations.md` under [`LEVELS_SECTION`], to the next heading.
fn levels_section(operations: &str) -> Vec<&str> {
    section(operations, LEVELS_SECTION)
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

// ---------------------------------------------------------------------------
// The metrics table lists every series `/metrics` exposes
// ---------------------------------------------------------------------------
//
// The Metrics section of `docs/operations.md` says of its table that "every
// series the endpoint exposes has a row", and until now nothing in this
// repository made that true. It has been wrong once, and expensively: the
// 0.20.0 test round found fourteen exposed series with no row — the whole
// embedding worker, the webhook block, both TTL counters, `kimmy_fsyncs` and
// `kimmy_commits_grouped_total` — and it took a harness outside this
// repository to notice, a release after the last of them shipped. Every series
// since has landed with its row in the same commit that added it to
// `metrics.rs` (ADR-145, ADR-147, ADR-148 each did). That is a habit; this is
// what makes it a rule.
//
// A missing row is not a documentation defect the way a typo is. This table is
// where an operator goes to find out what they can alert on; a series that is
// not in it is one nobody builds a dashboard against, and several the section
// tells you to alert on by name arrived that way. So the table is held to the
// render here, both ways round and down to the label keys, for the same reason
// the levels above are held to the enum.
//
// What this gate does *not* cover: label values. It compares series names and
// label keys, and a row's enumerated values — `ran`/`skipped` on
// `{outcome}`, `2xx`/`4xx`/`5xx` on `{class}` — live in the description cell,
// which nothing here reads. A series can gain a value for a label it already
// carries and this test will pass. The dimensions are held; the vocabulary of
// a dimension is not.

/// The heading of the metrics table's section, and the level it sits at.
const METRICS_SECTION: &str = "### Metrics";

/// The series an exposition contains, as base name to the label keys on it.
///
/// `# TYPE` lines are the register of names — one per series, whatever the
/// sample lines beneath it are called — and the labels come from the samples,
/// folded back onto the base name so a histogram is one entry here exactly as
/// it is one row in the table. `le` is dropped with them: it is the bucket
/// dimension of the histogram type itself, not a label the series carries, and
/// the table documents `_bucket`/`_sum`/`_count` no more than it documents the
/// exposition format.
fn exposed_series(exposition: &str) -> BTreeMap<String, BTreeSet<String>> {
    let kinds: BTreeMap<&str, &str> = exposition
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_once(' '))
        .map(|(name, kind)| (name.trim(), kind.trim()))
        .collect();

    // The base series a sample line belongs to: itself, or the histogram whose
    // suffixed sample it is. `None` is a sample with no `# TYPE` above it,
    // which the caller reports rather than skips.
    let base_of = |sample: &str| -> Option<&str> {
        if let Some((name, _)) = kinds.get_key_value(sample) {
            return Some(name);
        }
        ["_bucket", "_sum", "_count"].iter().find_map(|suffix| {
            let base = sample.strip_suffix(suffix)?;
            let (name, kind) = kinds.get_key_value(base)?;
            (*kind == "histogram").then_some(*name)
        })
    };

    let mut series: BTreeMap<String, BTreeSet<String>> =
        kinds.keys().map(|name| ((*name).to_string(), BTreeSet::new())).collect();

    for line in exposition.lines().filter(|l| !l.starts_with('#') && !l.trim().is_empty()) {
        let (name, labels) = match line.split_once('{') {
            Some((name, rest)) => {
                let inside = rest.split_once('}').map_or(rest, |(inside, _)| inside);
                let keys: Vec<&str> = inside
                    .split(',')
                    .filter_map(|kv| kv.split_once('='))
                    .map(|(k, _)| k.trim())
                    .collect();
                (name, keys)
            }
            None => (line.split(' ').next().unwrap_or(line), Vec::new()),
        };
        let base = base_of(name)
            .unwrap_or_else(|| panic!("`{name}` is sampled with no `# TYPE` line above it"));
        let histogram = kinds.get(base) == Some(&"histogram");
        let entry = series.entry(base.to_string()).or_default();
        for key in labels {
            if histogram && key == "le" {
                continue;
            }
            entry.insert(key.to_string());
        }
    }
    series
}

/// The metrics table's rows, as series name to the label keys its row names.
///
/// The first cell only, so the descriptions beside it can go on naming other
/// series in backticks without being read as rows of their own — and one cell
/// may carry several names, as `kimmy_databases`, `kimmy_collections` does.
/// A name is followed by its labels in braces, `{outcome}` or `{a,b}`, which is
/// how the table has always written them.
fn documented_series(section: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
    let mut rows = BTreeMap::new();
    for line in section.iter().filter(|line| line.trim_start().starts_with('|')) {
        let Some(first) = line.split('|').nth(1) else { continue };
        for token in first.split('`').skip(1).step_by(2) {
            if !token.starts_with("kimmy_") {
                continue;
            }
            let (name, labels) = match token.split_once('{') {
                Some((name, rest)) => (
                    name,
                    rest.trim_end_matches('}')
                        .split(',')
                        .map(|key| key.trim().to_string())
                        .filter(|key| !key.is_empty())
                        .collect(),
                ),
                None => (token, BTreeSet::new()),
            };
            assert!(
                rows.insert(name.to_string(), labels).is_none(),
                "`{name}` has two rows in the metrics table. Whichever is read second wins \
                 here, so one of them could disagree with the render and never be checked"
            );
        }
    }
    rows
}

#[test]
fn an_exposition_is_read_as_one_entry_per_series_whatever_its_samples_are_called() {
    let page = "# HELP kimmy_up Always 1.\n\
                # TYPE kimmy_up gauge\n\
                kimmy_up 1\n\
                # HELP kimmy_responses_total By class.\n\
                # TYPE kimmy_responses_total counter\n\
                kimmy_responses_total{class=\"2xx\"} 3\n\
                kimmy_responses_total{class=\"5xx\"} 0\n\
                # HELP kimmy_request_duration_seconds Latency.\n\
                # TYPE kimmy_request_duration_seconds histogram\n\
                kimmy_request_duration_seconds_bucket{le=\"0.001\"} 2\n\
                kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 3\n\
                kimmy_request_duration_seconds_sum 0.0034\n\
                kimmy_request_duration_seconds_count 3\n";

    let series = exposed_series(page);
    assert_eq!(
        series.keys().collect::<Vec<_>>(),
        vec!["kimmy_request_duration_seconds", "kimmy_responses_total", "kimmy_up"],
        "the three suffixed samples are the histogram, not three series: {series:#?}"
    );
    assert_eq!(series["kimmy_responses_total"], BTreeSet::from(["class".to_string()]));
    assert!(series["kimmy_up"].is_empty());
    assert!(
        series["kimmy_request_duration_seconds"].is_empty(),
        "`le` is the histogram's own bucket dimension, not a label a row would name"
    );
}

#[test]
fn a_metrics_table_is_read_from_the_first_cell_of_its_rows() {
    let doc = "### Metrics\n\
               \n\
               Prose naming `kimmy_never_exposed` in passing.\n\
               \n\
               | Series | |\n\
               |---|---|\n\
               | `kimmy_up` | Always 1 |\n\
               | `kimmy_databases`, `kimmy_collections` | Counts, not names |\n\
               | `kimmy_responses_total{class}` | `2xx`, `4xx`, `5xx` |\n\
               | `kimmy_sync_entries_skipped_total{reason,peer}` | Two labels |\n\
               \n\
               ### The next section\n\
               \n\
               | `kimmy_out_of_section` | not this table |\n";

    let rows = documented_series(&section(doc, METRICS_SECTION));
    assert_eq!(
        rows.keys().collect::<Vec<_>>(),
        vec![
            "kimmy_collections",
            "kimmy_databases",
            "kimmy_responses_total",
            "kimmy_sync_entries_skipped_total",
            "kimmy_up",
        ],
        "prose, description cells and the next section are all out: {rows:#?}"
    );
    assert_eq!(rows["kimmy_responses_total"], BTreeSet::from(["class".to_string()]));
    assert_eq!(
        rows["kimmy_sync_entries_skipped_total"],
        BTreeSet::from(["reason".to_string(), "peer".to_string()])
    );
    assert!(rows["kimmy_up"].is_empty());
}

#[test]
#[should_panic(expected = "`kimmy_up` has two rows in the metrics table")]
fn a_series_documented_twice_is_a_finding_and_not_the_second_row_winning() {
    // Two rows for one series is how a correct row and a stale one coexist:
    // the equality below would read whichever came second and pass, while the
    // operator reads whichever they scrolled to first.
    let doc = "### Metrics\n\
               \n\
               | `kimmy_up` | Always 1 |\n\
               | `kimmy_up` | Also always 1, said differently |\n";
    documented_series(&section(doc, METRICS_SECTION));
}

#[test]
fn nothing_the_endpoint_exposes_depends_on_the_engine_readings() {
    // What lets the test below render the page without a database and still
    // claim to have seen every series: the readings are values in the page,
    // never the reason a series is on it. The two `/proc` gauges are the ones
    // to watch — they read 0 on a platform without `/proc` rather than being
    // left out, deliberately, so a dashboard does not go blank (ADR-147).
    use kimmy_api::metrics::{Metrics, StorageReadings};

    let metrics = Metrics::default();
    let readings = StorageReadings {
        databases: 1,
        collections: 2,
        unique_violations: 3,
        commits: 4,
        fsyncs: 5,
        commits_grouped: 6,
        storage_bytes: 7,
        vector_index_cache_bytes: 8,
        process_resident_bytes: 9,
        process_resident_peak_bytes: 10,
        index_unkeyed: 11,
        index_undecidable: 17,
        sync_ddl_relogged: 16,
        writer_wait: kimmy_storage::WriterWaitSnapshot::default(),
        writer_wait_timeouts: 12,
        writer_hold_max_us: 13,
        writer_hold: kimmy_storage::WriterHoldSnapshot::default(),
        writer_hold_decomposition: kimmy_storage::HoldDecomposition::default(),
        serve: kimmy_storage::ServeSnapshot::default(),
        held_marks_released: 14,
        held_marks: 15,
    };
    assert_eq!(
        exposed_series(&metrics.render()).keys().collect::<Vec<_>>(),
        exposed_series(&metrics.render_with(&readings)).keys().collect::<Vec<_>>(),
        "a series appears or vanishes with the engine readings, so an empty render is no \
         longer the whole endpoint and the gate below has stopped covering it"
    );
}

#[test]
fn operations_lists_every_series_the_metrics_endpoint_exposes() {
    // The page the `/metrics` route serves, rendered the way the route renders
    // it — `render_with`, through `render`'s zeroed readings. Nothing here is
    // conditional: every series is written by one of the two unconditional
    // writes — `render_with`'s `format!` and the `render_latency` it calls,
    // which is where the histogram is — the embedding series render 0 with no
    // worker attached, and the two `/proc` gauges render 0
    // where there is no `/proc`. So there is no allowlist, and there should
    // not need to be one: a series that could be absent from this render is a
    // series an operator's dashboard can lose, which is a defect in the
    // exposition rather than an exception for this test to carry.
    use kimmy_api::metrics::Metrics;

    let exposed = exposed_series(&Metrics::default().render());
    let section = section(OPERATIONS, METRICS_SECTION);
    assert!(
        !section.is_empty(),
        "{METRICS_SECTION:?} is gone from docs/operations.md — an operator has nowhere to read \
         what the endpoint offers"
    );
    let documented = documented_series(&section);

    for name in exposed.keys() {
        assert!(
            documented.contains_key(name),
            "`{name}` is exposed but has no row in docs/operations.md#metrics. The section \
             promises that every series the endpoint exposes has one, and a series with no row \
             is one nobody alerts on"
        );
    }
    for name in documented.keys() {
        assert!(
            exposed.contains_key(name),
            "docs/operations.md#metrics documents `{name}` but the server does not expose it"
        );
    }

    for (name, labels) in &exposed {
        let row = &documented[name];
        assert_eq!(
            labels, row,
            "`{name}` carries the labels {labels:?} and its row in docs/operations.md#metrics \
             names {row:?}. A label the row does not name is a dimension nobody knows they can \
             split on; one it names that is not there is a query that returns nothing"
        );
    }
}

// ---------------------------------------------------------------------------
// Search bounds
// ---------------------------------------------------------------------------

/// `docs/vectors.md` states the clamp on `k` with the numbers the server uses.
///
/// The clamp is prose because it is not a refusal — `docs/openapi.yaml`
/// carries no `maximum` for it, and `tests/openapi.rs` insists on that — so
/// this is the only thing holding the numbers in the guide to the constants.
#[test]
fn vectors_states_the_k_clamp_the_server_applies() {
    use kimmy_api::vectors::{DEFAULT_K, MAX_K};
    let thousands = |n: usize| {
        let digits = n.to_string();
        let mut out = String::new();
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
        }
        out
    };
    let paragraph = VECTORS
        .split("\n\n")
        .find(|p| p.starts_with("`k` defaults to"))
        .expect("docs/vectors.md has a paragraph on `k`");
    let expected = format!(
        "`k` defaults to {DEFAULT_K} and is clamped to the range 1 to {} rather than refused",
        thousands(MAX_K)
    );
    assert!(
        paragraph.starts_with(&expected),
        "docs/vectors.md must state the clamp the server applies:\n  want: {expected}\n  have: \
         {paragraph}"
    );
}

// ---------------------------------------------------------------------------
// The JSON boundary lists every wrapper the edge reads
// ---------------------------------------------------------------------------

const HTTP_API: &str = include_str!("../../../docs/http-api.md");

/// The rows of the Extended JSON table in `docs/http-api.md`, by BSON type.
fn extended_json_rows(http_api: &str) -> Vec<(String, String)> {
    let section = http_api.split("## The JSON boundary").nth(1).expect("the JSON boundary section");
    section
        .lines()
        .skip_while(|l| !l.starts_with("| BSON type |"))
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let cells: Vec<&str> = l.trim_matches('|').split('|').map(str::trim).collect();
            (cells[0].to_string(), cells[1].to_string())
        })
        .collect()
}

#[test]
fn the_json_boundary_lists_decimal128_as_read_and_written() {
    // The table is what a client author reads to learn which wrappers the
    // edge understands. `$numberDecimal` was emitted for years and not
    // read, and the table did not say so either way; now that the edge
    // reads it, the row is the promise (ADR-139).
    let rows = extended_json_rows(HTTP_API);
    let (_, form) = rows
        .iter()
        .find(|(ty, _)| ty == "Decimal128")
        .expect("docs/http-api.md lists Decimal128 in the Extended JSON table");
    assert!(form.contains("$numberDecimal"), "the row names the wrapper: {form}");
    for wrapper in ["$oid", "$date", "$numberLong", "$numberDecimal", "$binary", "$minKey"] {
        assert!(
            rows.iter().any(|(_, form)| form.contains(wrapper)),
            "docs/http-api.md's Extended JSON table has no row for {wrapper}"
        );
    }
}

// ---------------------------------------------------------------------------
// A table keeps its rows
// ---------------------------------------------------------------------------

/// Where a run of table rows starts without a header, by line number.
///
/// GitHub renders a table only from a header row followed by a delimiter row.
/// A run of rows that does not open that way is not a table: after a paragraph
/// it renders as text run into the paragraph, and after a blank line as a
/// paragraph of pipes. Either way the rows are gone from the table they were
/// written for, and nothing reports it. The usual cause is a paragraph added
/// in the middle of a table, which cuts the rows below it off from the header.
///
/// A row is any line whose first non-blank character is `|`. What that reads
/// wrongly, and what is done about it:
/// - an example table inside a fenced code block is skipped, fence by fence;
/// - a line of prose that happens to contain a pipe is not a row, because it
///   does not start with one;
/// - a line of prose or an indented code block that does **start** with a pipe
///   is taken for a row and reported. No file has one; if one is ever needed,
///   fence it.
fn rows_without_a_header(markdown: &str) -> Vec<usize> {
    let lines: Vec<&str> = markdown.lines().collect();
    let is_row = |line: &str| line.trim_start().starts_with('|');
    let is_delimiter = |line: &str| {
        let cells = line.trim().trim_matches('|');
        !cells.is_empty()
            && cells.split('|').all(|cell| {
                let cell = cell.trim().trim_start_matches(':').trim_end_matches(':');
                cell.len() >= 3 && cell.chars().all(|c| c == '-')
            })
    };
    let mut found = Vec::new();
    let mut fenced = false;
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim_start().starts_with("```") {
            fenced = !fenced;
            i += 1;
            continue;
        }
        if fenced || !is_row(lines[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < lines.len() && is_row(lines[i]) {
            i += 1;
        }
        if !(start + 1 < i && is_delimiter(lines[start + 1])) {
            found.push(start + 1);
        }
    }
    found
}

/// Every test that reads a documentation file lives in this crate.
///
/// The workflow's documentation-only path runs `cargo nextest run -p kimmy-api`
/// and nothing else (`.github/workflows/ci.yml`). That is the whole set of
/// doc-reading targets only while this holds, so a target in another crate that
/// read a file matching CI's own `\.md$|^docs/` would not run on the changes
/// that can break it -- re-opening the gap one crate along. A read in another
/// crate's `src`, `build.rs` or `benches` counts for the same reason: the
/// content is compiled in, and a documentation-only change neither rebuilds nor
/// exercises it.
///
/// Three ways of reaching documentation are recognised, which are the ways this
/// workspace reaches it:
/// - `include_str!` or `include_bytes!` of a path holding `docs/` or ending
///   `.md`, which is a compile-time read: the file's content is baked into the
///   binary, so the target fails to *compile* if the file goes;
/// - `join("../..")`, the idiom a whole-repository walk starts from. Bare,
///   because from the root a walk can reach any documentation file;
/// - `join("../../`...`)` naming a documentation path directly.
///
/// Reaching above the crate for something that is not documentation is not a
/// match, and three places do it: `../../crates`, `../../Cargo.toml` and
/// `../../fuzz/corpus`.
///
/// **What this does not see.** A walk that reaches the repository root some
/// other way -- an environment variable, `current_dir` and its ancestors, a
/// path assembled from pieces rather than written as one literal -- is invisible
/// to a rule that reads source text. If one is ever written, this test will not
/// name it, and the workflow's documentation-only path has to be widened by
/// hand. The rule is a tripwire on the idioms in use, not a proof.
#[test]
fn documentation_tests_live_in_this_crate() {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("a readable directory") {
            let path = entry.expect("a directory entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if path.is_dir() {
                if !name.starts_with('.') && name != "target" {
                    walk(&path, out);
                }
            } else if name.ends_with(".rs") {
                out.push(path);
            }
        }
    }

    // Canonical, so that a walked path does not carry this crate's own name in
    // a `kimmy-api/../..` prefix -- which is what the premise below caught when
    // the root was left as written.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root");
    let this_crate = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("this crate's directory");
    let mut sources = Vec::new();
    walk(&root, &mut sources);

    // Premise: the walk reached other crates' sources, and this crate's own are
    // the ones excluded. Without this a walk that found nothing would pass.
    let outside: Vec<&std::path::PathBuf> = sources
        .iter()
        .filter(|p| !p.canonicalize().expect("a readable file").starts_with(&this_crate))
        .collect();
    assert!(outside.len() >= 50, "premise: other crates' sources were read ({})", outside.len());
    let leaked: Vec<String> = outside
        .iter()
        .filter(|p| p.to_string_lossy().contains("kimmy-api"))
        .map(|p| p.display().to_string())
        .collect();
    assert!(leaked.is_empty(), "premise: this crate is excluded, but: {leaked:#?}");
    for must in ["kimmy-storage", "kimmyd", "kimmy-core", "kimmy-cluster"] {
        assert!(
            outside.iter().any(|p| p.to_string_lossy().contains(must)),
            "premise: {must}'s sources were read"
        );
    }

    let mut offenders = Vec::new();
    for path in outside {
        let body = std::fs::read_to_string(path).expect("a readable source file");
        for (n, line) in body.lines().enumerate() {
            let names_a_doc = line.contains("docs/") || line.contains(".md");
            let compiled_in =
                (line.contains("include_str!") || line.contains("include_bytes!")) && names_a_doc;
            let from_the_root = line.contains(r#"join("../..")"#);
            let above_to_a_doc = line.contains(r#"join("../../"#) && names_a_doc;
            if compiled_in || from_the_root || above_to_a_doc {
                let shown = path.strip_prefix(&root).unwrap_or(path).display();
                offenders.push(format!("{shown}:{}: {}", n + 1, line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these read a documentation file from outside kimmy-api, so CI's \
         documentation-only path -- `cargo nextest run -p kimmy-api` -- would not run them on \
         the changes that can break them. Move the test into kimmy-api, or widen that step and \
         this rule together:\n  {}",
        offenders.join("\n  ")
    );
}

/// The documented Kubernetes manifest allows more than twice the open its own
/// example describes, computed from the code's rate rather than from a second
/// copy of it.
///
/// The startup probe exists so that a first start which rebuilds partial
/// indexes is not killed part-way (ADR-183). It is therefore the one figure in
/// the guide that must not drift from the code: when the constant said 8 µs and
/// the guide said 7, the manifest's own example needed 252 s against the 240 s
/// it allowed, so following the guide exactly would have killed the node during
/// the migration the manifest exists to allow for -- the failure it is there to
/// prevent, written into it.
#[test]
fn the_documented_startup_probe_allows_the_open_it_describes() {
    // Scoped to the startup probe's own block, which ends where the next probe
    // begins. Searching from `startupProbe:` to the end of the file would read
    // the liveness or readiness probe's fields if the startup probe ever lost
    // one, and silently grade the wrong numbers.
    let at = OPERATIONS.find("startupProbe:").expect("the manifest has a startup probe");
    let after = &OPERATIONS[at..];
    let block = &after[..after.find("livenessProbe:").unwrap_or(after.len())];
    assert!(block.contains("failureThreshold:"), "the startup probe's block was not found");
    let field = |name: &str| -> u64 {
        let line = block
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{name}:")))
            .unwrap_or_else(|| panic!("the startup probe sets {name}"));
        line.split(':').nth(1).expect("a value").trim().parse().expect("a number")
    };
    let budget = field("periodSeconds") * field("failureThreshold");

    // The example the manifest's own comment describes: ten million retained
    // oplog entries, and one partial index over ten million documents. Both
    // figures are hardcoded here, deliberately: the 46 s oplog walk is a
    // measurement that lives only in prose, and the example's size is the
    // comment's own choice. Only the per-document rate is taken from the code,
    // because that is the one the code can change under the guide.
    const EXAMPLE_DOCUMENTS: u64 = 10_000_000;
    const EXAMPLE_OPLOG_SECS: u64 = 46;
    let rebuild = EXAMPLE_DOCUMENTS * kimmy_storage::migrate::MICROS_PER_DOCUMENT / 1_000_000;
    let open = EXAMPLE_OPLOG_SECS + rebuild;

    assert!(
        budget >= 2 * open,
        "the manifest allows {budget} s, and its own example needs {open} s ({EXAMPLE_OPLOG_SECS} \
         s of oplog walk plus {rebuild} s of rebuild at {} µs per document), so the guide's rule \
         of at least twice the expected open needs {} s. Following the guide would kill the node \
         part-way through the migration the probe exists for.",
        kimmy_storage::migrate::MICROS_PER_DOCUMENT,
        2 * open
    );

    // And the comment quotes the code's figure, so the prose cannot drift back.
    // As a whole token: a bare `contains` would accept "18 us per" as a match
    // for 8, which is the direction that hides a drift rather than reporting it.
    let quoted = format!("{} us per", kimmy_storage::migrate::MICROS_PER_DOCUMENT);
    let quoted_as_a_token = OPERATIONS.match_indices(&quoted).any(|(i, _)| {
        i == 0 || !OPERATIONS[..i].chars().next_back().is_some_and(|c| c.is_ascii_digit())
    });
    assert!(
        quoted_as_a_token,
        "the manifest's comment does not quote the code's {quoted:?} as its own number"
    );
}

/// Every Markdown file under the repository root, relative to it, outside
/// build output and hidden directories.
fn markdown_files() -> (std::path::PathBuf, Vec<std::path::PathBuf>) {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("a readable directory") {
            let path = entry.expect("a directory entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if path.is_dir() {
                if !name.starts_with('.') && name != "target" {
                    walk(&path, out);
                }
            } else if name.ends_with(".md") {
                out.push(path);
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut out = Vec::new();
    walk(&root, &mut out);
    let mut out: Vec<_> =
        out.iter().map(|p| p.strip_prefix(&root).expect("under the root").to_path_buf()).collect();
    out.sort();
    (root, out)
}

#[test]
fn rows_cut_off_from_their_header_are_found() {
    let whole = "| a | b |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n";
    assert_eq!(rows_without_a_header(whole), Vec::<usize>::new(), "a whole table");
    let aligned = "| a | b |\n| :--- | ---: |\n| 1 | 2 |\n";
    assert_eq!(rows_without_a_header(aligned), Vec::<usize>::new(), "aligned columns");

    // A paragraph added in the middle, with and without a blank line after.
    let run_into = "| a | b |\n|---|---|\n| 1 | 2 |\n\nA note.\n| 3 | 4 |\n";
    assert_eq!(rows_without_a_header(run_into), [6], "rows run into a paragraph");
    let after_blank = "| a | b |\n|---|---|\n| 1 | 2 |\n\nA note.\n\n| 3 | 4 |\n| 5 | 6 |\n";
    assert_eq!(rows_without_a_header(after_blank), [7], "rows after a blank line");

    // What is not a table is not asked to have a header.
    let fenced = "```\n| not | a table |\n```\n";
    assert_eq!(rows_without_a_header(fenced), Vec::<usize>::new(), "inside a fence");
    let prose = "A pipe in a sentence, `a | b`, is not a row.\n";
    assert_eq!(rows_without_a_header(prose), Vec::<usize>::new(), "a pipe inside prose");
}

#[test]
fn every_table_in_the_documentation_keeps_its_rows() {
    let (root, files) = markdown_files();
    // Premise: the walk reaches the files the documentation lives in, among
    // them the two where cut-off rows were first found, so an empty answer
    // is not an empty walk.
    for known in ["docs/decisions.md", "docs/testing.md", "CHANGELOG.md"] {
        assert!(
            files.iter().any(|f| f.ends_with(known)),
            "the walk did not reach {known}: {files:?}"
        );
    }
    let mut cut_off = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(root.join(file)).expect("a readable file");
        for line in rows_without_a_header(&text) {
            cut_off.push(format!("{}:{line}", file.display()));
        }
    }
    assert!(
        cut_off.is_empty(),
        "table rows with no header above them, which render as text rather than as rows; \
         move whatever was added between them and their table:\n  {}",
        cut_off.join("\n  ")
    );
}
