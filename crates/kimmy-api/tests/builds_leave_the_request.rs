//! A request never waits for an HNSW build (ADR-245), and the one way to
//! wait for one, the tests' `settle`, is in no build that ships.
//!
//! Here rather than in `kimmy-vector` because it reads the workspace from
//! `join("../..")`, which `documentation_tests_live_in_this_crate` accounts
//! for in this crate only (see `supervision.rs`).

use std::path::Path;

/// `settle` is in no build that ships (ADR-245): it waits on the build
/// queue, which no request may do. It is compiled only for `kimmy-vector`'s
/// own tests and under `test-hooks`, and no manifest in the workspace turns
/// `test-hooks` on but in a dev-dependency, which a release build does
/// not resolve (resolver 3).
#[test]
fn settle_is_in_no_build_that_ships() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = std::fs::read_to_string(root.join("crates/kimmy-vector/src/cache.rs")).unwrap();
    let at = source.find("    pub fn settle(&self)").expect("settle");
    let above = source[..at]
        .lines()
        .rev()
        .find(|line| !line.trim_start().starts_with("///"))
        .expect("an attribute");
    assert_eq!(above.trim(), "#[cfg(any(test, feature = \"test-hooks\"))]");

    let workspace = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(workspace.lines().any(|line| line.trim() == "resolver = \"3\""));
    let mut manifests = vec![root.join("Cargo.toml")];
    for crate_dir in std::fs::read_dir(root.join("crates")).unwrap() {
        let manifest = crate_dir.unwrap().path().join("Cargo.toml");
        if manifest.exists() {
            manifests.push(manifest);
        }
    }
    assert!(manifests.len() > 5, "the workspace's manifests: {manifests:?}");
    let mut seen = 0;
    for manifest in manifests {
        let text = std::fs::read_to_string(&manifest).unwrap();
        let mut section = String::new();
        for line in text.lines().map(str::trim) {
            if line.starts_with('[') {
                section = line.to_string();
            } else if line.contains("kimmy-vector")
                && line.contains("test-hooks")
                && !line.starts_with('#')
            {
                seen += 1;
                assert!(
                    section.contains("dev-dependencies"),
                    "{}: {line} in {section}",
                    manifest.display()
                );
            }
        }
    }
    assert!(seen > 0, "the dev-dependency that turns the hooks on for kimmy-api's tests");
}
