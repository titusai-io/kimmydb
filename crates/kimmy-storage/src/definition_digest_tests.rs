//! The definition digest (B3 half 2, design 3.2): what it hashes, how it is
//! framed, and what it must not see. Written before the hash.

use bson::{Document, doc};
use kimmy_core::index_meta::stored_filter;
use kimmy_core::{
    ChunkConfig, CollectionId, Enforcement, Hlc, IndexField, IndexMeta, Metric, NodeId,
    ProviderConfig, Stamp, VectorConfig,
};
use proptest::prelude::*;

use crate::definition_digest::{
    VECTOR_NAME, definition_digest, definition_list, index_hash, vector_hash,
};
use crate::meta::CollectionMeta;

fn index(name: &str, fields: &[(&str, bool)]) -> IndexMeta {
    IndexMeta {
        id: IndexMeta::derive_id(name),
        name: name.into(),
        fields: fields
            .iter()
            .map(|(path, descending)| IndexField { path: (*path).into(), descending: *descending })
            .collect(),
        unique: false,
        enforcement: Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    }
}

fn collection(indexes: Vec<IndexMeta>, vector: Option<VectorConfig>) -> CollectionMeta {
    let mut meta = CollectionMeta::new(
        CollectionId::derive("shop", "orders"),
        "shop",
        "orders",
        Hlc::new(10, 0),
        None,
    );
    meta.vector = vector;
    // The index list is private to the module that allocates ids: set it
    // through the public field.
    meta.indexes = indexes;
    meta
}

fn config(dim: usize) -> VectorConfig {
    VectorConfig {
        fields: vec!["text".into()],
        provider: ProviderConfig::Byo {},
        dim,
        metric: Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: ChunkConfig { max_chars: 1_000, overlap: 100, max_tokens: None },
    }
}

fn stamp(wall: u64, node: u8) -> Stamp {
    Stamp::new(Hlc::new(wall, 0), NodeId::from_bytes([node; 16]))
}

// -- The property: the digest is equal exactly when `differences` is empty -----

fn filters() -> impl Strategy<Value = Option<Document>> {
    prop_oneof![
        Just(None),
        Just(Some(doc! {})),
        Just(Some(doc! { "a": 1 })),
        Just(Some(doc! { "a": 1i64 })),
        Just(Some(doc! { "a": 1, "b": 2 })),
        Just(Some(doc! { "b": 2, "a": 1 })),
        Just(Some(doc! { "k": { "a": 1, "b": 2 } })),
        Just(Some(doc! { "k": { "b": 2, "a": 1 } })),
        Just(Some(doc! { "x": f64::NAN })),
        Just(Some(doc! { "x": 0.0 })),
        Just(Some(doc! { "x": -0.0 })),
    ]
}

fn field_lists() -> impl Strategy<Value = Vec<IndexField>> {
    // Short paths that split differently when concatenated: ("a","bc") and
    // ("ab","c") must not hash alike.
    let path = prop_oneof![Just("a"), Just("bc"), Just("ab"), Just("c")];
    prop::collection::vec(
        (path, any::<bool>()).prop_map(|(p, d)| IndexField { path: p.into(), descending: d }),
        1..=3,
    )
}

fn definitions() -> impl Strategy<Value = IndexMeta> {
    (
        field_lists(),
        any::<bool>(),
        prop_oneof![Just(Enforcement::Local), Just(Enforcement::Coordinated)],
        prop_oneof![Just(None), Just(Some(0i64)), Just(Some(60i64))],
        filters(),
        any::<bool>(),
        prop_oneof![Just(None), Just(Some(stamp(5, 1))), Just(Some(stamp(9, 2)))],
    )
        .prop_map(|(fields, unique, enforcement, expire, filter, multikey, created)| {
            IndexMeta {
                id: IndexMeta::derive_id("x"),
                name: "x".into(),
                fields,
                unique,
                enforcement,
                multikey,
                expire_after_secs: expire,
                partial_filter: filter,
                created,
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4_000))]

    /// Over pairs of definitions under one name, `differences(a, b)` is empty
    /// iff the two hashes are equal. `multikey`, `created` and `id` are not in
    /// the comparison and not in the hash.
    #[test]
    fn two_definitions_under_one_name_hash_alike_iff_the_engine_calls_them_the_same(
        a in definitions(),
        b in definitions(),
    ) {
        prop_assert_eq!(
            a.differences(&b).is_empty(),
            index_hash(&a) == index_hash(&b),
            "{:?} against {:?}", a, b
        );
    }
}

/// The framing is injective on the pairs the property test is most likely to
/// miss: strings that split differently, an option that is `None` against one
/// that holds zero, and a missing filter against an empty one.
#[test]
fn framing_keeps_the_adversarial_pairs_apart() {
    let one = index("x", &[("a", false), ("bc", false)]);
    let other = index("x", &[("ab", false), ("c", false)]);
    assert_ne!(index_hash(&one), index_hash(&other), "(a, bc) against (ab, c)");

    let mut none = index("x", &[("a", false)]);
    let mut zero = none.clone();
    none.expire_after_secs = None;
    zero.expire_after_secs = Some(0);
    assert_ne!(index_hash(&none), index_hash(&zero), "no TTL against a TTL of zero");

    let mut missing = index("x", &[("a", false)]);
    let mut empty = missing.clone();
    missing.partial_filter = None;
    empty.partial_filter = Some(doc! {});
    assert_ne!(index_hash(&missing), index_hash(&empty), "no filter against an empty one");

    // The name is part of the hash: the same shape under two names is two
    // definitions to the list.
    let named = index("x", &[("a", false)]);
    let renamed = index("y", &[("a", false)]);
    assert_ne!(index_hash(&named), index_hash(&renamed));
}

/// The top level of a partial filter is a conjunction, so its order selects
/// nothing and is sorted away; inside a field's operand it is part of the
/// value and is kept (`stored_filter::compared`).
#[test]
fn top_level_order_is_one_definition_and_nested_order_is_two() {
    let with = |filter: Document| {
        let mut index = index("x", &[("a", false)]);
        index.partial_filter = Some(filter);
        index
    };
    assert_eq!(
        index_hash(&with(doc! { "a": 1, "b": 2 })),
        index_hash(&with(doc! { "b": 2, "a": 1 })),
        "top-level order"
    );
    assert_ne!(
        index_hash(&with(doc! { "k": { "a": 1, "b": 2 } })),
        index_hash(&with(doc! { "k": { "b": 2, "a": 1 } })),
        "nested order"
    );
}

// -- The vector configuration -------------------------------------------------

/// A vector configuration is part of the digest by what it says, and not by the
/// stamp of its last change: a configuration made before 0.46 sits on its origin
/// with `vector_changed: None` while a replica that applied the same old entry
/// stores `Some(stamp)`, and nothing would ever heal that.
#[test]
fn a_vector_configuration_is_hashed_by_what_it_says_and_not_by_its_stamp() {
    let mut a = collection(vec![], Some(config(4)));
    let mut b = collection(vec![], Some(config(4)));
    a.vector_changed = None;
    b.vector_changed = Some(stamp(77, 3));
    assert_eq!(definition_digest(&a), definition_digest(&b), "None against Some");
    let c = collection(vec![], Some(config(8)));
    assert_ne!(definition_digest(&a), definition_digest(&c), "a different width");
    assert_ne!(definition_digest(&a), definition_digest(&collection(vec![], None)), "off");
    assert_ne!(vector_hash(&config(4)), vector_hash(&config(8)));
}

#[test]
fn every_part_of_a_vector_configuration_is_in_the_hash() {
    let base = config(4);
    let mut variants: Vec<(&str, VectorConfig)> = Vec::new();
    let mut v = base.clone();
    v.fields = vec!["text".into(), "title".into()];
    variants.push(("fields", v));
    let mut v = base.clone();
    v.metric = Metric::Dot;
    variants.push(("metric", v));
    let mut v = base.clone();
    v.document_prefix = Some("passage: ".into());
    variants.push(("document prefix", v));
    let mut v = base.clone();
    v.query_prefix = Some("query: ".into());
    variants.push(("query prefix", v));
    let mut v = base.clone();
    v.chunk.max_chars = 2_000;
    variants.push(("chunk size", v));
    let mut v = base.clone();
    v.chunk.overlap = 50;
    variants.push(("chunk overlap", v));
    let mut v = base.clone();
    v.chunk.max_tokens = Some(0);
    variants.push(("chunk tokens", v));
    let mut v = base.clone();
    v.provider = ProviderConfig::Local { model: "m".into() };
    variants.push(("provider", v));
    let mut v = base.clone();
    v.provider = ProviderConfig::Profile { name: "p".into() };
    variants.push(("profile", v));
    let mut v = base.clone();
    v.provider = ProviderConfig::Ollama { model: "m".into(), endpoint: "e".into() };
    variants.push(("ollama", v));
    for (what, changed) in &variants {
        assert_ne!(vector_hash(&base), vector_hash(changed), "{what}");
    }
    for (i, (a, x)) in variants.iter().enumerate() {
        for (b, y) in &variants[i + 1..] {
            assert_ne!(vector_hash(x), vector_hash(y), "{a} against {b}");
        }
    }
}

// -- The collection's digest ----------------------------------------------------

#[test]
fn the_collections_digest_does_not_see_what_the_comparison_does_not() {
    let mut a = index("by_a", &[("a", false)]);
    let b = index("by_b", &[("b", true)]);
    let one = collection(vec![a.clone(), b.clone()], None);
    // Order in the list, `created`, `multikey`, the collection's own `created`
    // and its floor are not part of the definition.
    a.created = Some(stamp(5, 1));
    a.multikey = true;
    let mut two = collection(vec![b.clone(), a.clone()], None);
    two.created = Hlc::new(99, 0);
    two.incarnation_floor = Some(Hlc::new(42, 0));
    assert_eq!(definition_digest(&one), definition_digest(&two));
    assert_eq!(definition_list(&one), definition_list(&two));
}

#[test]
fn the_list_is_ordered_by_derived_id_then_name_and_the_vector_comes_last() {
    let meta = collection(
        vec![index("zeta", &[("z", false)]), index("alpha", &[("a", false)])],
        Some(config(4)),
    );
    let list = definition_list(&meta);
    let names: Vec<&str> = list.iter().map(|(name, _)| name.as_str()).collect();
    let mut by_id = ["zeta", "alpha"];
    by_id.sort_by_key(|name| (IndexMeta::derive_id(name), *name));
    assert_eq!(names, vec![by_id[0], by_id[1], VECTOR_NAME]);
    // And each entry carries the hash of the definition under that name.
    assert_eq!(list[2].1, vector_hash(&config(4)));
}

#[test]
fn a_digest_differs_for_an_absent_index_and_for_a_different_set() {
    let a = index("by_a", &[("a", false)]);
    let b = index("by_b", &[("b", false)]);
    let both = collection(vec![a.clone(), b.clone()], None);
    let only_a = collection(vec![a], None);
    assert_ne!(definition_digest(&both), definition_digest(&only_a));
    assert_ne!(definition_digest(&only_a), definition_digest(&collection(vec![b], None)));
    assert_ne!(definition_digest(&collection(vec![], None)), definition_digest(&only_a));
}

/// The digest is a function of the metadata alone: an empty collection and one
/// whose list is empty hash alike, and the empty digest is not the zero a stub
/// returns.
#[test]
fn an_empty_set_of_definitions_has_a_digest_of_its_own() {
    let empty = collection(vec![], None);
    assert_eq!(definition_digest(&empty), definition_digest(&collection(vec![], None)));
    assert_ne!(definition_digest(&empty), 0);
}

// -- The library's output is pinned ---------------------------------------------

/// Members on different builds compare hashes of strings `compared()` produced
/// (`bson`'s canonical Extended JSON). A dependency change in how a number or a
/// key renders would make every partial-filter index differ between builds, and
/// [`FORMAT`](crate::definition_digest::FORMAT) covers this module's framing and
/// not the library's output. Pinned for each type the filter language accepts;
/// a deliberate change bumps the format.
#[test]
fn the_rendering_of_every_filter_type_is_pinned() {
    use bson::Bson;
    let rendered = |value: Bson| stored_filter::compared(&Some(doc! { "k": value })).unwrap();
    let cases: Vec<(&str, Bson, &str)> = vec![
        ("int32", Bson::Int32(5), r#"{"k":{"$numberInt":"5"}}"#),
        ("int64", Bson::Int64(5), r#"{"k":{"$numberLong":"5"}}"#),
        ("double", Bson::Double(1.5), r#"{"k":{"$numberDouble":"1.5"}}"#),
        ("nan", Bson::Double(f64::NAN), r#"{"k":{"$numberDouble":"NaN"}}"#),
        ("negative zero", Bson::Double(-0.0), r#"{"k":{"$numberDouble":"-0.0"}}"#),
        ("string", Bson::String("s".into()), r#"{"k":"s"}"#),
        ("bool", Bson::Boolean(true), r#"{"k":true}"#),
        ("null", Bson::Null, r#"{"k":null}"#),
        (
            "date",
            Bson::DateTime(bson::DateTime::from_millis(1_000)),
            r#"{"k":{"$date":{"$numberLong":"1000"}}}"#,
        ),
        (
            "object id",
            Bson::ObjectId(bson::oid::ObjectId::parse_str("0123456789abcdef01234567").unwrap()),
            r#"{"k":{"$oid":"0123456789abcdef01234567"}}"#,
        ),
        (
            "binary",
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![1, 2],
            }),
            r#"{"k":{"$binary":{"base64":"AQI=","subType":"00"}}}"#,
        ),
        (
            "array",
            Bson::Array(vec![Bson::Int32(1), Bson::String("a".into())]),
            r#"{"k":[{"$numberInt":"1"},"a"]}"#,
        ),
        (
            "embedded document",
            Bson::Document(doc! { "b": 2, "a": 1 }),
            r#"{"k":{"b":{"$numberInt":"2"},"a":{"$numberInt":"1"}}}"#,
        ),
    ];
    for (what, value, expected) in cases {
        assert_eq!(rendered(value), expected, "{what}");
    }
    // The top level is sorted, and nothing else is.
    assert_eq!(
        stored_filter::compared(&Some(doc! { "b": 1, "a": 2 })).unwrap(),
        r#"{"a":{"$numberInt":"2"},"b":{"$numberInt":"1"}}"#
    );
}

// -- The stored forms -----------------------------------------------------------

/// A filter written by a build before ADR-182 is in the relaxed form, which reads
/// a small `Int64` back as an `Int32`. Every member reads a stored form the same
/// way, and a rewrite stores the canonical form of the already-lossy value, so
/// members that converged agree: the digest of the meta read from the relaxed
/// text equals that of the meta read back from its own rewrite.
#[test]
fn a_relaxed_stored_filter_and_its_canonical_rewrite_hash_alike() {
    let relaxed = serde_json::json!({
        "id": CollectionId::derive("shop", "orders"),
        "db": "shop",
        "name": "orders",
        "created": Hlc::new(10, 0),
        "indexes": [{
            "id": IndexMeta::derive_id("f"),
            "name": "f",
            "fields": [{ "path": "n", "descending": false }],
            "partial_filter": { "n": { "$gte": 5 }, "s": "x" },
        }],
    });
    let from_relaxed: CollectionMeta = serde_json::from_value(relaxed).unwrap();
    let rewritten = serde_json::to_string(&from_relaxed).unwrap();
    assert!(rewritten.contains("$numberInt"), "premise: the rewrite is canonical: {rewritten}");
    let from_canonical: CollectionMeta = serde_json::from_str(&rewritten).unwrap();
    assert_eq!(from_relaxed.indexes[0].partial_filter, from_canonical.indexes[0].partial_filter);
    assert_eq!(definition_digest(&from_relaxed), definition_digest(&from_canonical));
}
