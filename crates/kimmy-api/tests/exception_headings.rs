//! Every exception of a recorded table that the engine keeps cites the entry of
//! `docs/deviations.md` that covers it by the text of its heading, and that heading must be there
//! (a line number goes stale when the file changes above it).
//!
//! The tables are in kimmy-query, which keeps its exceptions lists and checks that a label is a
//! row of its table; the documentation file is read here, because only kimmy-api tests read one
//! (`documentation_tests_live_in_this_crate`). The lists are read by path from where they are, so
//! there is one source for each.

const DEVIATIONS: &str = include_str!("../../../docs/deviations.md");

/// An exceptions file: a label, a tab, and the heading text the entry ends with. A line that
/// starts with `#` is a comment.
fn cited(file: &str) -> Vec<(&str, &str)> {
    file.lines().filter(|l| !l.starts_with('#')).filter_map(|l| l.split_once('\t')).collect()
}

fn assert_headings_exist(name: &str, file: &str) -> usize {
    let entries = cited(file);
    for (label, heading) in &entries {
        assert!(
            DEVIATIONS.lines().any(|line| line.starts_with("## ") && line.ends_with(heading)),
            "{name}: {label}: no heading of docs/deviations.md ends with {heading:?}"
        );
    }
    entries.len()
}

#[test]
fn every_exception_cites_a_heading_of_the_deviations_that_exists() {
    let checked = assert_headings_exist(
        "resolver_elements_exceptions.txt",
        include_str!("../../kimmy-query/tests/query_conformance/resolver_elements_exceptions.txt"),
    );
    let table = assert_headings_exist(
        "update_table_exceptions.txt",
        include_str!("../../kimmy-query/tests/data/update_table_exceptions.txt"),
    );
    println!("exception headings checked: {checked} (resolver elements), {table} (update table)");
    assert!(checked > 0 && table > 0, "an exceptions list names no heading at all");
}
