//! A JSON number reaches storage as the double it denotes, to the last bit, in
//! the binary this crate builds.
//!
//! `serde_json` parses floats with a fast algorithm that is off by one unit in
//! the last place for about a third of long or extreme inputs (668,661 of
//! 2,000,000 in the decimal-arithmetic review's run), unless its
//! `float_roundtrip` feature is on. The workspace declares the feature; before
//! it did, the only thing enabling it was `jsonschema`, a dev-dependency of
//! `kimmy-api`, so the tests of that crate parsed correctly while the shipped
//! `kimmyd`, built without dev-dependencies, did not.
//!
//! **Why here.** A test in `kimmy-api` cannot see the feature go: its own build
//! has `jsonschema`. This crate's does not, so `cargo test -p kimmyd` compiles
//! `serde_json` with the features the release binary gets, and the hard cases
//! below fail without the declaration. A workspace-wide run unifies features
//! across every crate's dev-dependencies and would pass regardless, which is
//! what the declaration guard is for.

use bson::Bson;

/// Inputs the fast path gets wrong, and the bits a correctly rounded parse
/// gives (Rust's own `f64::from_str`, which is correctly rounded).
const HARD: [(&str, u64); 5] = [
    // The largest subnormal, written to 17 digits: the fast path rounds it
    // up into the normal range.
    ("2.2250738585072011e-308", 0x000f_ffff_ffff_ffff),
    // Exactly halfway between 1 and the next double: ties to even, down.
    ("1.00000000000000011102230246251565404236316680908203125", 0x3ff0_0000_0000_0000),
    ("1.2345678901234567e-300", 0x01aa_74fe_1c1e_8908),
    // Two of the review's generated strings.
    ("7.90847020955810e-174", 0x1bff_4bcc_042b_636d),
    ("5.01927345144758955970e8", 0x41bd_eacd_b125_0eec),
];

#[test]
fn a_hard_json_float_reaches_the_document_with_its_exact_bits() {
    for (text, bits) in HARD {
        assert_eq!(text.parse::<f64>().unwrap().to_bits(), bits, "premise: {text}");
        // As a request body arrives: text, then `serde_json`, then the edge.
        let body: serde_json::Value = serde_json::from_str(&format!(r#"{{"v": {text}}}"#)).unwrap();
        let doc = kimmy_api::json::json_to_document(&body).unwrap();
        match doc.get("v") {
            Some(Bson::Double(v)) => assert_eq!(
                v.to_bits(),
                bits,
                "{text} parsed to {v:e} ({:#018x}), not {bits:#018x}: is serde_json's \
                 float_roundtrip feature declared in the workspace Cargo.toml?",
                v.to_bits()
            ),
            other => panic!("{text} became {other:?}"),
        }
    }
}

/// The declaration itself, so a workspace-wide test run, whose feature
/// unification hides its loss, still sees it go; and every crate takes
/// `serde_json` from the workspace, so it gets the feature.
#[test]
fn the_workspace_declares_serde_jsons_float_roundtrip_and_every_crate_inherits_it() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let declaration = manifest
        .lines()
        .find(|l| l.starts_with("serde_json = "))
        .expect("serde_json is a workspace dependency");
    assert!(declaration.contains("\"float_roundtrip\""), "{declaration}");
    assert!(declaration.contains("\"preserve_order\""), "{declaration}");

    let mut checked = 0;
    for entry in std::fs::read_dir(root.join("crates")).unwrap() {
        let path = entry.unwrap().path().join("Cargo.toml");
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        for line in text.lines().filter(|l| l.trim_start().starts_with("serde_json")) {
            assert_eq!(
                line.trim(),
                "serde_json.workspace = true",
                "{}: serde_json must come from the workspace, with its features",
                path.display()
            );
            checked += 1;
        }
    }
    assert!(checked >= 8, "premise: {checked} crates declare serde_json");
}
