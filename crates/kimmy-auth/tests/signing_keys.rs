//! The justification for ignoring RUSTSEC-2026-0387 in `deny.toml`.
//!
//! `crypto-bigint` 0.5.5 (through `jsonwebtoken`'s `rust_crypto` backend) has
//! non-constant-time arithmetic. That is harmless while the server only
//! *verifies* EC and RSA signatures, whose operands are all public. This test
//! fails the day production code builds an EC or RSA **signing** key, which is
//! when the ignore stops being true and `deny.toml` must be revisited.
//!
//! Test code and the fuzz harness may build such keys: they are fixtures.

use std::path::{Path, PathBuf};

/// Constructors of a private signing key for an algorithm that uses `crypto-bigint`.
/// `from_secret` (HMAC) is not among them, and neither is `DecodingKey`.
const SIGNING_KEY_CONSTRUCTORS: [&str; 5] = [
    "EncodingKey::from_ec_der",
    "EncodingKey::from_ec_pem",
    "EncodingKey::from_rsa_der",
    "EncodingKey::from_rsa_pem",
    "EncodingKey::from_rsa_raw_components",
];

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_production_code_builds_an_ec_or_rsa_signing_key() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut files = Vec::new();
    for entry in std::fs::read_dir(crates).unwrap() {
        let krate = entry.unwrap().path();
        // The fuzz harness signs with fixture keys on purpose.
        if krate.file_name().is_some_and(|n| n == "kimmy-fuzz-harness") {
            continue;
        }
        let src = krate.join("src");
        if src.is_dir() {
            sources(&src, &mut files);
        }
    }
    assert!(files.len() > 50, "premise: the walk found the workspace's sources");
    let mut found = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        // Everything from the first test module on is test code. Files that keep
        // their tests in a sibling file are named `*tests.rs` and skipped whole.
        if file.file_name().is_some_and(|n| n.to_string_lossy().ends_with("tests.rs")) {
            continue;
        }
        let production = text.split("#[cfg(test)]").next().unwrap_or("");
        for constructor in SIGNING_KEY_CONSTRUCTORS {
            if production.contains(constructor) {
                found.push(format!("{}: {constructor}", file.display()));
            }
        }
    }
    assert!(
        found.is_empty(),
        "production code builds an EC or RSA signing key, so the RUSTSEC-2026-0387 ignore in \
         deny.toml no longer holds (the server was verify-only): {found:#?}"
    );
}

/// The guard sees what it is meant to: this file's own list is not production
/// source, but a snippet that names a constructor is found.
#[test]
fn the_guard_finds_a_constructor_in_production_text() {
    let snippet = "fn sign() { let k = EncodingKey::from_ec_der(&der); }\n#[cfg(test)]\nmod t {}";
    let production = snippet.split("#[cfg(test)]").next().unwrap();
    assert!(SIGNING_KEY_CONSTRUCTORS.iter().any(|c| production.contains(c)));
}
