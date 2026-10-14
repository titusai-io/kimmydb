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
    DefinitionKey, definition_digest, definition_list, index_hash, vector_hash,
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

/// One change to a definition and the engine's own judgement of it. The
/// independent pairs above almost never differ in only one part, so a part left
/// out of the hash could go unseen for thousands of cases; here every pair is a
/// definition and the same definition with one part changed.
fn one_change() -> impl Strategy<Value = (IndexMeta, IndexMeta)> {
    (definitions(), 0usize..7).prop_map(|(a, which)| {
        let mut b = a.clone();
        match which {
            0 => b.unique = !b.unique,
            1 => {
                b.enforcement = match b.enforcement {
                    Enforcement::Local => Enforcement::Coordinated,
                    Enforcement::Coordinated => Enforcement::Local,
                }
            }
            2 => b.fields[0].descending = !b.fields[0].descending,
            3 => {
                b.expire_after_secs = match b.expire_after_secs {
                    None => Some(60),
                    Some(_) => None,
                }
            }
            4 => b.fields.reverse(),
            5 => b.fields[0].path.push('z'),
            _ => b.multikey = !b.multikey,
        }
        (a, b)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn one_change_is_seen_by_the_hash_iff_the_engine_sees_it(pair in one_change()) {
        let (a, b) = pair;
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
    let keys: Vec<&DefinitionKey> = list.iter().map(|(key, _)| key).collect();
    let mut by_id = ["zeta", "alpha"];
    by_id.sort_by_key(|name| (IndexMeta::derive_id(name), *name));
    let (first, second) = (DefinitionKey::index(by_id[0]), DefinitionKey::index(by_id[1]));
    assert_eq!(keys, vec![&first, &second, &DefinitionKey::vector()]);
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

/// Strings in a list split differently: framed by length, so they do not hash alike.
#[test]
fn a_vector_configurations_fields_are_framed_by_length() {
    let mut one = config(4);
    one.fields = vec!["ab".into(), "c".into()];
    let mut other = config(4);
    other.fields = vec!["a".into(), "bc".into()];
    assert_ne!(vector_hash(&one), vector_hash(&other));
}

/// The metric, the order of an index's fields and its direction are each part
/// of the hash.
#[test]
fn a_metric_and_a_direction_are_part_of_the_hash() {
    let mut euclid = config(4);
    euclid.metric = Metric::Euclidean;
    let mut dot = config(4);
    dot.metric = Metric::Dot;
    let hashes = [vector_hash(&config(4)), vector_hash(&euclid), vector_hash(&dot)];
    assert_eq!(hashes.iter().collect::<std::collections::BTreeSet<_>>().len(), 3);
    assert_ne!(
        index_hash(&index("x", &[("a", false)])),
        index_hash(&index("x", &[("a", true)])),
        "ascending against descending"
    );
}

/// Two names can share a derived id (a 32-bit hash). The list orders them by
/// name, so the order the indexes were created in does not change the digest.
#[test]
fn two_names_sharing_a_derived_id_are_ordered_by_name_in_the_list() {
    let mut seen: std::collections::HashMap<u32, String> = std::collections::HashMap::new();
    let (first, second) = (0u32..)
        .find_map(|n| {
            let name = format!("n{n}");
            seen.insert(IndexMeta::derive_id(&name), name.clone()).map(|earlier| (earlier, name))
        })
        .unwrap();
    assert_eq!(IndexMeta::derive_id(&first), IndexMeta::derive_id(&second));
    let a = collection(vec![index(&first, &[("a", false)]), index(&second, &[("b", false)])], None);
    let b = collection(vec![index(&second, &[("b", false)]), index(&first, &[("a", false)])], None);
    assert_eq!(definition_list(&a), definition_list(&b));
    assert_eq!(definition_digest(&a), definition_digest(&b));
}

/// What is hashed and how it is framed is pinned to the byte: a change to either
/// changes one of these and bumps `FORMAT` on purpose, so that two builds under
/// different formats are never equal by accident.
#[test]
fn the_hash_of_a_definition_is_pinned() {
    let full = {
        let mut i = index("full", &[("a", true), ("bc", false)]);
        i.unique = true;
        i.enforcement = Enforcement::Coordinated;
        i.expire_after_secs = Some(60);
        i.partial_filter = Some(doc! { "n": { "$gte": 5_i64 } });
        i
    };
    let with_provider = |provider: ProviderConfig| {
        let mut c = config(4);
        c.provider = provider;
        c
    };
    let mut euclid = config(4);
    euclid.metric = Metric::Euclidean;
    let mut prefixed = config(4);
    prefixed.document_prefix = Some("doc: ".into());
    prefixed.query_prefix = Some("q: ".into());
    prefixed.chunk.max_tokens = Some(512);
    let rows: Vec<(&str, u128)> = vec![
        ("index, one ascending field", index_hash(&index("x", &[("a", false)]))),
        ("index, every part", index_hash(&full)),
        ("vector, byo", vector_hash(&config(4))),
        ("vector, euclidean", vector_hash(&euclid)),
        ("vector, prefixes and tokens", vector_hash(&prefixed)),
        (
            "vector, openai bare",
            vector_hash(&with_provider(ProviderConfig::OpenAi {
                model: "m".into(),
                endpoint: None,
                api_key_env: "K".into(),
                dimensions: None,
            })),
        ),
        (
            "vector, openai full",
            vector_hash(&with_provider(ProviderConfig::OpenAi {
                model: "m".into(),
                endpoint: Some("http://e".into()),
                api_key_env: "K".into(),
                dimensions: Some(3),
            })),
        ),
        (
            "vector, ollama",
            vector_hash(&with_provider(ProviderConfig::Ollama {
                model: "m".into(),
                endpoint: "http://e".into(),
            })),
        ),
        (
            "vector, custom http",
            vector_hash(&with_provider(ProviderConfig::CustomHttp {
                endpoint: "http://e".into(),
                api_key_env: Some("K".into()),
            })),
        ),
        (
            "vector, cohere",
            vector_hash(&with_provider(ProviderConfig::Cohere {
                model: "m".into(),
                endpoint: None,
                api_key_env: "K".into(),
            })),
        ),
        (
            "vector, gemini",
            vector_hash(&with_provider(ProviderConfig::Gemini {
                model: "m".into(),
                endpoint: None,
                api_key_env: "K".into(),
            })),
        ),
        ("vector, local", vector_hash(&with_provider(ProviderConfig::Local { model: "m".into() }))),
        (
            "vector, profile",
            vector_hash(&with_provider(ProviderConfig::Profile { name: "p".into() })),
        ),
        (
            "collection digest, two indexes and a vector",
            definition_digest(&collection(
                vec![index("x", &[("a", false)]), full.clone()],
                Some(config(4)),
            )),
        ),
        ("collection digest, nothing", definition_digest(&collection(vec![], None))),
    ];
    let seen: std::collections::BTreeSet<_> = rows.iter().map(|(_, h)| *h).collect();
    assert_eq!(seen.len(), rows.len(), "every row is its own hash");
    let pinned: Vec<(&str, &str)> = PINNED.to_vec();
    let got: Vec<String> = rows.iter().map(|(_, h)| format!("{h:032x}")).collect();
    for (((label, _), hash), (pinned_label, pinned_hash)) in rows.iter().zip(&got).zip(&pinned) {
        assert_eq!(label, pinned_label);
        assert_eq!(hash, pinned_hash, "{label}: the hash changed; bump FORMAT on purpose");
    }
    assert_eq!(rows.len(), pinned.len());
}

const PINNED: &[(&str, &str)] = &[
    ("index, one ascending field", "6b17b6a32909a39fa9507e29f28d81ff"),
    ("index, every part", "941ef9752b07086ce5c515d6d5397c68"),
    ("vector, byo", "4805202f3b82b1f992d4ea795f6efee2"),
    ("vector, euclidean", "17c73213c716aa6281aec555317635ba"),
    ("vector, prefixes and tokens", "42c315cfa4e9643f4c92553864b245d0"),
    ("vector, openai bare", "f6a6f003f7d8e1cb1d8ed7d1f05d0563"),
    ("vector, openai full", "a10fe6686620cc5100baa9a627f779ec"),
    ("vector, ollama", "1c960c13ffa82267abf7d909dd3f3bb8"),
    ("vector, custom http", "82d9ace0d0ac6d25245e4a51961c37ad"),
    ("vector, cohere", "1cbcf1cd0aa7d93920737975a6132d81"),
    ("vector, gemini", "17e4ddbe15641b7bb64bbb5c8144aefe"),
    ("vector, local", "badab7edd56b9fb6a3714192bfc9e4ce"),
    ("vector, profile", "e3a444fa911037d5b4f74fda21a9a441"),
    ("collection digest, two indexes and a vector", "568dbcf3cd44a4ec9a9215a5a11c9de9"),
    ("collection digest, nothing", "97abaa003ead56bbffb0d7276c8766a7"),
];
