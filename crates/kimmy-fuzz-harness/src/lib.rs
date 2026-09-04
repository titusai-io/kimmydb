//! Fuzz harnesses for the surfaces that take attacker-controlled bytes.
//!
//! Every function here has the same shape: it takes arbitrary bytes and must
//! return. Not "must succeed" — most inputs are garbage and are refused with an
//! `Err` somewhere inside — but it must never panic, never overflow the stack
//! and never allocate without bound. Where the code under test promises an
//! invariant beyond "no panic" (the key encoder's order, an update leaving
//! `_id` alone, the JSON boundary reaching a fixed point), the harness asserts
//! it, because a fuzzer that only looks for crashes would walk straight past a
//! wrong answer — and wrong answers are the failures this project treats as the
//! serious ones (see `docs/testing.md`).
//!
//! # Why a crate, and why this one is in the workspace
//!
//! `cargo fuzz` needs nightly and a sanitizer build, so the libFuzzer entry
//! points cannot compile under the stable PR gate. They live in `fuzz/`, a
//! separate root, and each is one line: decode the bytes and call a function
//! from here. The functions themselves are ordinary stable Rust, so this crate
//! is a workspace member and `cargo clippy --workspace --all-targets` compiles
//! it on every pull request. A harness that only built on a weekly schedule
//! would break in the week something it calls was renamed, and be discovered
//! the week after. This layout is what ADR-111 chose.
//!
//! The same split lets the seed corpora double as regression tests: the tests
//! at the bottom of this file run every file under `fuzz/corpus/<target>/`
//! through its harness under plain `cargo test`.
//!
//! # Input decoding
//!
//! Targets over a JSON document decode the bytes with `serde_json` and then the
//! API's own `json_to_document`, so the fuzzer walks the same path a request
//! body does, Extended JSON wrappers included. `serde_json`'s recursion limit
//! (128) is what bounds the depth of everything downstream.
//!
//! Targets that want a typed value rather than text — the key encoder wants
//! BSON values, the verifiers want a *signed* token so the claims parser is
//! reached — draw it from the bytes through [`arbitrary::Unstructured`]. The
//! generator lives in [`arb`] and is written to concentrate on the boundaries
//! where an order-preserving encoding breaks: the same edges the property test
//! in `kimmy-core` generates, plus every BSON variant the property test leaves
//! out.

use std::cmp::Ordering;
use std::sync::LazyLock;

use arbitrary::Unstructured;
use bson::{Bson, Document, doc};
use serde_json::Value;

pub mod arb;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A small document set that spans the type table.
///
/// Every parser target evaluates what it parsed against these, so a filter
/// that parses cleanly is also *run* — over arrays, nested documents, a `NaN`,
/// an infinity, a NUL inside a string, `MinKey`/`MaxKey`, a missing field, and
/// an empty document. Small on purpose: what matters is that each interesting
/// shape appears once, not that there are many of them.
pub static FIXTURES: LazyLock<Vec<Document>> = LazyLock::new(|| {
    vec![
        doc! {
            "_id": 1, "item": "widget", "qty": 5, "price": 2.5, "status": "new",
            "tags": ["a", "b"], "placed": bson::DateTime::from_millis(1_700_000_000_000),
            "address": { "city": "Lisbon", "zip": "1000" },
        },
        doc! {
            "_id": 2, "item": "gadget", "qty": 25, "price": 10, "status": "paid",
            "tags": [], "placed": bson::DateTime::from_millis(1_700_086_400_000),
            "n": [1, 2, 3], "nested": { "a": [{ "b": 1 }, { "b": 2 }] },
        },
        doc! {
            "_id": 3, "item": Bson::Null, "qty": -3, "price": f64::NAN, "status": "shipped",
            "total": 9_007_199_254_740_993_i64,
            "items": [{ "sku": "a", "qty": 9 }, { "sku": "b", "qty": 1 }],
        },
        doc! {
            "_id": bson::oid::ObjectId::from_bytes([7; 12]), "qty": 0.0, "price": f64::INFINITY,
            "flag": true, "s": "ab\u{0}c", "empty": {}, "arr": [[1], [2]],
            "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0, 255] },
        },
        doc! {
            "_id": "str-id", "qty": 1_i64 << 40, "status": "new", "tags": ["b", "c", "a"],
            "deep": { "a": { "b": { "c": { "d": 1 } } } },
            "min": Bson::MinKey, "max": Bson::MaxKey,
            "ts": bson::Timestamp { time: 1, increment: 2 },
        },
        doc! {},
    ]
});

/// A fixed "now" for `$currentDate`, so a harness is a pure function of its
/// input and a crash reproduces.
const NOW_MS: i64 = 1_700_000_000_000;

/// Indexes for the planner to choose among: single-field, compound with a
/// descending component, multikey, and partial.
static INDEXES: LazyLock<Vec<kimmy_core::IndexMeta>> = LazyLock::new(|| {
    use kimmy_core::{IndexField, IndexMeta};
    let index = |name: &str, fields: Vec<IndexField>| IndexMeta {
        id: IndexMeta::derive_id(name),
        name: name.to_string(),
        fields,
        unique: false,
        enforcement: Default::default(),
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        // The planner never reads the creation stamp; it settles replicated
        // conflicts, which no fuzz target reaches.
        created: None,
    };
    vec![
        index("qty_1", vec![IndexField::ascending("qty")]),
        index(
            "status_1_placed_-1",
            vec![IndexField::ascending("status"), IndexField::descending("placed")],
        ),
        IndexMeta { multikey: true, ..index("tags_1", vec![IndexField::ascending("tags")]) },
        IndexMeta {
            partial_filter: Some(doc! { "status": "new" }),
            ..index("item_1_partial", vec![IndexField::ascending("item")])
        },
    ]
});

/// Bytes → JSON → BSON document, the way a request body arrives.
fn json_document(data: &[u8]) -> Option<Document> {
    let value: Value = serde_json::from_slice(data).ok()?;
    kimmy_api::json::json_to_document(&value).ok()
}

// ---------------------------------------------------------------------------
// Query language
// ---------------------------------------------------------------------------

/// `find`'s filter: parse, then evaluate and plan.
pub fn filter_parse(data: &[u8]) {
    use kimmy_query::{filter, plan};

    let Some(doc) = json_document(data) else { return };
    let Ok(filter) = filter::parse(&doc) else { return };
    for fixture in FIXTURES.iter() {
        let _ = filter::matches(&filter, fixture);
    }
    // The planner reads the same AST, and a wrong range bound there is a
    // silently short result — so it runs over every filter that parses.
    let _ = plan::choose(&filter, &INDEXES);
}

/// An update document: parse, then apply to each fixture.
///
/// Beyond "no panic", one promise is checked: an update that applies never
/// changes `_id`. The parser refuses operators that name it directly, but the
/// path language reaches under it (`_id.x`) and the replacement form carries
/// its own — an `_id` that changes under an update is a document that has
/// silently left every index that pointed at it.
pub fn update_parse_apply(data: &[u8]) {
    use kimmy_query::update;

    let Some(doc) = json_document(data) else { return };
    let Ok(update) = update::parse(&doc) else { return };
    for fixture in FIXTURES.iter() {
        let mut target = fixture.clone();
        let id_before = target.get("_id").cloned();
        if update::apply(&update, &mut target, NOW_MS).is_ok() {
            assert_eq!(target.get("_id"), id_before.as_ref(), "an update changed _id: {doc:?}");
        }
    }
}

/// Projection and sort documents: parse, then project and sort the fixtures.
///
/// The sort comparator is also checked for antisymmetry over every pair of
/// fixtures. `slice::sort_by` may panic on a comparator that is not a total
/// order, and the fixtures include `NaN`, mixed types and arrays precisely
/// because those are where a comparator stops being one.
pub fn projection_sort_parse(data: &[u8]) {
    use kimmy_query::shape;

    let Some(doc) = json_document(data) else { return };

    if let Ok(projection) = shape::parse_projection(&doc) {
        for fixture in FIXTURES.iter() {
            let _ = shape::project(projection.as_ref(), fixture);
        }
    }

    if let Ok(keys) = shape::parse_sort(&doc) {
        let mut docs = FIXTURES.clone();
        shape::sort(&keys, &mut docs);
        for a in FIXTURES.iter() {
            assert_eq!(shape::compare(&keys, a, a), Ordering::Equal, "compare(a, a) != Equal");
            for b in FIXTURES.iter() {
                assert_eq!(
                    shape::compare(&keys, a, b),
                    shape::compare(&keys, b, a).reverse(),
                    "sort comparator is not antisymmetric under {keys:?}"
                );
            }
        }
    }
}

/// An aggregation pipeline: parse, then run it in memory over a fixed input.
///
/// Everything but `$lookup` runs without a storage handle, so the pipeline is
/// executed the way `kimmy-api`'s executor runs it, stage by stage, over the
/// fixtures plus thirty generated orders. `$lookup` is parsed and then refused
/// by `apply`, which is the documented contract; the executor's join is not
/// reached from here. The limit is set low so `$unwind` and `$group` growth
/// hits the cap inside the harness rather than the fuzzer's memory limit.
pub fn aggregate_parse_run(data: &[u8]) {
    use kimmy_query::aggregate;

    let Ok(value) = serde_json::from_slice::<Value>(data) else { return };
    let Some(array) = value.as_array() else { return };
    let Ok(stages) =
        array.iter().map(kimmy_api::json::json_to_document).collect::<Result<Vec<_>, _>>()
    else {
        return;
    };
    let Ok(pipeline) = aggregate::parse(&stages) else { return };

    let limits = aggregate::Limits { max_documents: 2_000 };
    let mut docs = PIPELINE_INPUT.clone();
    for stage in &pipeline {
        match aggregate::apply(stage, docs, &limits) {
            Ok(next) => docs = next,
            Err(_) => return,
        }
    }
}

static PIPELINE_INPUT: LazyLock<Vec<Document>> = LazyLock::new(|| {
    let cities = ["Lisbon", "Porto", "Faro"];
    let statuses = ["new", "paid", "shipped"];
    let mut docs = FIXTURES.clone();
    for i in 0..30_i32 {
        docs.push(doc! {
            "_id": 100 + i,
            "city": cities[i as usize % 3],
            "status": statuses[i as usize % 3],
            "qty": i * 3 - 10,
            "total": f64::from(i) * 12.5,
            "tags": ["x", "y"],
            "placed": bson::DateTime::from_millis(NOW_MS + i64::from(i) * 86_400_000),
        });
    }
    docs
});

/// An aggregation expression: parse, then evaluate against each fixture.
pub fn expr_eval(data: &[u8]) {
    use kimmy_query::Expr;

    let Ok(value) = serde_json::from_slice::<Value>(data) else { return };
    let Ok(bson) = kimmy_api::json::json_to_bson(&value) else { return };
    let Ok(expr) = Expr::parse(&bson) else { return };
    for fixture in FIXTURES.iter() {
        let _ = expr.eval(fixture);
    }
}

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

/// The HTTP edge's JSON ⇄ BSON conversion, from both sides.
///
/// Neither direction is exactly invertible — `{"$numberLong": "5"}` becomes an
/// `Int64` that prints as `5` and reads back as an `Int32`, and a binary
/// subtype is not carried inward — so the invariant is a **fixed point**: one
/// round trip may normalise, the second must change nothing. From the BSON
/// side, anything the encoder emits must be accepted by the decoder, because a
/// stored value the API cannot read back is a document that has vanished.
pub fn extended_json_bson(data: &[u8]) {
    use kimmy_api::json::{bson_to_json, json_to_bson, json_to_document};

    // JSON in, as a request body.
    if let Ok(value) = serde_json::from_slice::<Value>(data) {
        let _ = json_to_document(&value);
        if let Ok(b1) = json_to_bson(&value) {
            let j1 = bson_to_json(&b1);
            let b2 = json_to_bson(&j1).expect("the boundary refused its own output");
            let j2 = bson_to_json(&b2);
            assert_eq!(j1, j2, "the JSON form did not reach a fixed point after one round trip");
        }
    }

    // BSON out, as a stored document. A generated document may carry a key
    // like `$oid` with a value no wrapper accepts, which the decoder refuses
    // exactly as it would in a request body — so a refusal is allowed there.
    // For everything else the claim is the strong one: what the API shows
    // for a stored value must read back as that value, so a client that
    // re-submits what it was given changes nothing. That is the assertion
    // that found the binary subtype being dropped on the way in.
    let mut u = Unstructured::new(data);
    let value = arb::bson(&mut u);
    let j0 = bson_to_json(&value);
    let b1 = match json_to_bson(&j0) {
        Ok(b1) => b1,
        Err(_) if arb::has_dollar_key(&value) => return,
        Err(e) => panic!("the boundary refused a value it emitted: {e:?} for {value:?}"),
    };
    let j1 = bson_to_json(&b1);
    if !arb::has_dollar_key(&value) {
        assert_eq!(j0, j1, "a stored value read back differently after one round trip");
    }
    let b2 = json_to_bson(&j1).expect("the boundary refused its own normalised output");
    assert_eq!(j1, bson_to_json(&b2), "the JSON form did not reach a fixed point");
}

/// The order-preserving key encoder against the canonical comparator.
///
/// This is invariant #1 in `docs/testing.md`: `encode(a).cmp(encode(b)) ==
/// canonical_cmp(a, b)`, for every pair of encodable values. The property test
/// in `kimmy-core` states the same thing over its own generator; this one
/// states it over a coverage-guided one, and adds the variants that generator
/// omits (`Symbol`, `Timestamp`, regular expressions, JavaScript, `Undefined`,
/// non-generic binary subtypes, and `Decimal128`, which must be *refused*).
pub fn key_encoding(data: &[u8]) {
    use kimmy_core::canonical_cmp;
    use kimmy_core::keyenc::{encode, encode_compound_ordered};

    let mut u = Unstructured::new(data);
    let a = arb::bson(&mut u);
    let b = arb::bson(&mut u);

    // The comparator is an order on its own, whatever the encoder does.
    assert_eq!(canonical_cmp(&a, &a), Ordering::Equal, "canonical_cmp(a, a) != Equal: {a:?}");
    let order = canonical_cmp(&a, &b);
    assert_eq!(order, canonical_cmp(&b, &a).reverse(), "canonical_cmp is not antisymmetric");

    let (Ok(ea), Ok(eb)) = (encode(&a), encode(&b)) else {
        // Only `Decimal128` may be refused, and it must be refused wherever
        // it appears, so a value holding one never becomes half a key.
        assert!(arb::holds_decimal(&a) || arb::holds_decimal(&b), "encode refused {a:?} / {b:?}");
        return;
    };

    assert_eq!(ea.cmp(&eb), order, "encoding disagrees with canonical_cmp for {a:?} vs {b:?}");
    if order == Ordering::Equal {
        assert_eq!(ea, eb, "equal values encoded differently: {a:?} vs {b:?}");
    }

    let desc = |v: &Bson| encode_compound_ordered(&[(v.clone(), true)]).expect("encodable");
    assert_eq!(
        desc(&a).cmp(&desc(&b)),
        order.reverse(),
        "descending did not invert the order for {a:?} vs {b:?}"
    );
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// The signing secret every local-token fixture here uses. Not a secret: it
/// exists so a crash reproduces, and it never leaves the harness.
const LOCAL_SECRET: &str = "fuzz-harness-signing-secret-that-is-long-enough";

static LOCAL_ISSUER: LazyLock<kimmy_auth::TokenIssuer> = LazyLock::new(|| {
    kimmy_auth::TokenIssuer::new(LOCAL_SECRET, 3_600).expect("the fixture secret is long enough")
});

/// The HS256 verifier, two ways.
///
/// First as the bytes stand: the token as an `Authorization` header would
/// carry it, which exercises the header and signature parsing that runs
/// before any key is consulted. Then *signed*: a claims document drawn from
/// the same bytes is signed with the fixture secret and verified, which is the
/// only way to reach the claims deserialiser and the principal it builds — an
/// unsigned input never gets past the signature check.
pub fn jwt_local_verify(data: &[u8]) {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use kimmy_auth::Action;

    let raw = String::from_utf8_lossy(data);
    let _ = LOCAL_ISSUER.verify(raw.trim());
    let _ = kimmy_auth::oidc::claimed_issuer(raw.trim());

    // No audience: the local verifier asks for none, and the JWT library
    // refuses a token carrying an `aud` nobody asked it to check — which is
    // the right answer, and also why a federated token can never be accepted
    // here by accident (invariant #10 in docs/testing.md).
    let mut u = Unstructured::new(data);
    let claims = arb::claims(&mut u, OIDC_ISSUER, None);
    let key = EncodingKey::from_secret(LOCAL_SECRET.as_bytes());
    let Ok(token) = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &key) else {
        return;
    };
    if let Ok(principal) = LOCAL_ISSUER.verify(&token) {
        let _ = principal.can(Action::Read, "sales", Some("orders"));
        let _ = principal.can(Action::Admin, "__kimmy", None);
    }
}

/// A PKCS#8 P-256 private key, the same one `kimmy-auth`'s tests sign with.
/// Its public half is the fixture JWKS, so a token signed here verifies.
const EC_DER_B64: &str = concat!(
    "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdYt6Sm2yyfFR8Bic5yJIzy6A",
    "Ra59sojVUjw/3t5rwyOhRANCAASTbia99nDdIMlZG1ND4yE0aYr4lybfQbD2whxMikG8lbsH",
    "O6OtfLKUpjzwvieZriD+AhtalEtnc1pXO6GvNSrL",
);
const OIDC_ISSUER: &str = "https://idp.example.com";
const OIDC_AUDIENCE: &str = "https://db.example.com/";
const OIDC_KID: &str = "fuzz-es256";

struct OidcFixture {
    verifier: kimmy_auth::OidcVerifier,
    signing_key: jsonwebtoken::EncodingKey,
}

static OIDC: LazyLock<OidcFixture> = LazyLock::new(|| {
    use base64::Engine as _;
    use jsonwebtoken::{Algorithm, EncodingKey};
    use kimmy_auth::{Action, Grant, Jwk, JwkSet, OidcSettings, OidcVerifier, RoleMapping};

    let der = base64::engine::general_purpose::STANDARD.decode(EC_DER_B64).expect("valid base64");
    let signing_key = EncodingKey::from_ec_der(&der);
    let mut jwk = Jwk::from_encoding_key(&signing_key, Algorithm::ES256).expect("public half");
    jwk.common.key_id = Some(OIDC_KID.to_string());

    let settings = OidcSettings {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        roles_claim: "roles".into(),
        role_mappings: vec![RoleMapping {
            role: Some("analyst".into()),
            claim_value: "kimmydb-analyst".into(),
            grants: vec![Grant::new("sales", "orders*", vec![Action::Read, Action::Search])],
        }],
        require_at_jwt: false,
        allow_federated_admin: false,
        // The production defaults, so the fuzzer runs the path a deployment
        // runs: the lifetime ceiling (ADR-096) is part of what verification
        // decides, and an unset `subject_claim` keeps `sub` as the identity.
        subject_claim: None,
    };
    let verifier =
        OidcVerifier::new(settings).expect("valid settings").with_keys(JwkSet { keys: vec![jwk] });
    OidcFixture { verifier, signing_key }
});

/// The OIDC verifier against a fixed in-memory JWKS, two ways.
///
/// The verifier takes its key set by injection (`with_keys`), so no network
/// is involved and the full path — header, `kid` lookup, signature, issuer,
/// audience, expiry, not-before, the lifetime ceiling, then the roles claim
/// and the grants it maps to — runs in process. As with the local verifier, the raw bytes cover the
/// parsing that precedes the signature check and a token signed with the
/// fixture key covers everything after it. The claims generator leans on the
/// right issuer and audience often enough that the fuzzer gets past both
/// checks and into the claim-mapping code.
pub fn jwt_oidc_verify(data: &[u8]) {
    use jsonwebtoken::{Algorithm, Header};
    use kimmy_auth::Action;

    let raw = String::from_utf8_lossy(data);
    let _ = OIDC.verifier.claims_this_issuer(raw.trim());
    let _ = OIDC.verifier.verify(raw.trim());

    let mut u = Unstructured::new(data);
    let claims = arb::claims(&mut u, OIDC_ISSUER, Some(OIDC_AUDIENCE));
    let mut header = Header::new(Algorithm::ES256);
    header.kid = u.arbitrary::<bool>().unwrap_or(true).then(|| OIDC_KID.to_string());
    if u.arbitrary::<bool>().unwrap_or(false) {
        header.typ =
            Some(if u.arbitrary::<bool>().unwrap_or(true) { "at+jwt" } else { "JWT" }.into());
    }
    let Ok(token) = jsonwebtoken::encode(&header, &claims, &OIDC.signing_key) else { return };
    if let Ok(principal) = OIDC.verifier.verify(&token) {
        assert!(
            principal.federated,
            "a token the OIDC verifier accepted produced a local principal"
        );
        let _ = principal.can(Action::Read, "sales", Some("orders_2024"));
        let _ = principal.can(Action::Admin, "sales", None);
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// The shape every harness has: bytes in, nothing out, no panic.
pub type Harness = fn(&[u8]);

/// Every target, by the name its `fuzz/fuzz_targets/<name>.rs` and
/// `fuzz/corpus/<name>/` carry. The corpus test below iterates this, so a
/// target added without a seed corpus fails under `cargo test`.
pub const TARGETS: &[(&str, Harness)] = &[
    ("filter_parse", filter_parse),
    ("update_parse_apply", update_parse_apply),
    ("projection_sort_parse", projection_sort_parse),
    ("aggregate_parse_run", aggregate_parse_run),
    ("expr_eval", expr_eval),
    ("extended_json_bson", extended_json_bson),
    ("key_encoding", key_encoding),
    ("jwt_local_verify", jwt_local_verify),
    ("jwt_oidc_verify", jwt_oidc_verify),
];

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn corpus_dir(target: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus").join(target)
    }

    /// The seeds are the inputs the fuzzer starts from and, once a crash has
    /// been minimised and fixed, where its reproducer is kept. Running them
    /// under `cargo test` makes every past finding a regression test on the
    /// stable PR path, and makes a target that lost its corpus directory a
    /// failure rather than a fuzzer that starts from nothing.
    #[test]
    fn every_seed_in_every_corpus_runs_clean() {
        for (name, harness) in TARGETS {
            let dir = corpus_dir(name);
            let entries: Vec<_> = std::fs::read_dir(&dir)
                .unwrap_or_else(|e| panic!("no seed corpus at {}: {e}", dir.display()))
                .map(|entry| entry.expect("readable directory entry").path())
                .filter(|path| path.is_file())
                .collect();
            assert!(!entries.is_empty(), "the seed corpus for {name} is empty");
            for path in entries {
                let bytes = std::fs::read(&path).expect("readable seed");
                harness(&bytes);
            }
        }
    }

    /// The inputs a fuzzer finds first, without waiting for it to find them.
    #[test]
    fn degenerate_inputs_run_clean() {
        let deep_json = format!("{}1{}", "[".repeat(400), "]".repeat(400));
        let deep_and = {
            let mut s = String::from(r#"{"a":1}"#);
            for _ in 0..100 {
                s = format!(r#"{{"$and":[{s}]}}"#);
            }
            s
        };
        let inputs: [&[u8]; 8] = [
            b"",
            b"{}",
            b"[]",
            b"null",
            &[0xff, 0xfe, 0x00, 0x01],
            deep_json.as_bytes(),
            deep_and.as_bytes(),
            &[0xAA; 4096],
        ];
        for (_, harness) in TARGETS {
            for input in inputs {
                harness(input);
            }
        }
    }

    /// The signed paths must actually be reached, or the verifier targets
    /// only ever test the signature check.
    #[test]
    fn the_signed_token_paths_verify_for_well_formed_claims() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let claims = serde_json::json!({
            "sub": "ada", "iss": OIDC_ISSUER, "aud": OIDC_AUDIENCE,
            "exp": now + 600, "iat": now, "roles": ["kimmydb-analyst"],
        });
        let local_claims = serde_json::json!({
            "sub": "ada", "exp": now + 600, "iat": now, "tv": 3,
            "grants": [{ "db": "sales", "collection": "orders*", "actions": ["read"] }],
        });

        let key = EncodingKey::from_secret(LOCAL_SECRET.as_bytes());
        let header = Header::new(Algorithm::HS256);
        let local = jsonwebtoken::encode(&header, &local_claims, &key).unwrap();
        let principal = LOCAL_ISSUER.verify(&local).unwrap();
        assert!(principal.can(kimmy_auth::Action::Read, "sales", Some("orders")));
        // A federated-looking token is refused by the local verifier: it
        // carries an audience the verifier was never told to accept.
        let federated_shape = jsonwebtoken::encode(&header, &claims, &key).unwrap();
        assert!(LOCAL_ISSUER.verify(&federated_shape).is_err());

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(OIDC_KID.into());
        let federated = jsonwebtoken::encode(&header, &claims, &OIDC.signing_key).unwrap();
        let principal = OIDC.verifier.verify(&federated).unwrap();
        assert!(principal.federated);
        assert!(principal.can(kimmy_auth::Action::Read, "sales", Some("orders")));
    }
}
