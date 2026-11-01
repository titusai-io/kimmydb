//! The answers the reference (6.0.5) gave to the Q8a-1 probe `q8-probes/stages.js`, section
//! by section, run here over the probe's own documents. An answer the reference leaves
//! unspecified (the order of equal counts) is compared as a set. The `$unionWith` sections
//! that read a second collection are `kimmy-api`'s `exec/union_tests.rs`.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits};

fn stg() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "k": "a", "n": 1, "a": {"x": 1, "y": 2}, "items": [{"sku": 1, "q": 5}, {"sku": 2}, 7]},
        doc! {"_id": 2, "k": "b", "n": 2, "a": {"x": 3, "y": 4}},
        doc! {"_id": 3, "k": "a", "n": 3},
        doc! {"_id": 4, "k": "c", "n": 4},
        doc! {"_id": 5, "k": "a", "n": 5},
        doc! {"_id": 6, "n": 6},
        doc! {"_id": 7, "k": Bson::Null, "n": 7},
    ]
}

fn lv() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "level": 1, "title": "t1",
        "cc": {"level": 5, "num": 12},
        "ship": [{"level": 3, "city": "A"}, {"level": 5, "city": "B"}, 9],
        "meta": {"level": 2, "tags": ["x", {"level": 5, "z": 1}]}},
        doc! {"_id": 2, "level": 5, "title": "secret"},
    ]
}

fn run(pipeline: Vec<Document>, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&pipeline)?;
    stages.iter().try_fold(input, |docs, stage| aggregate::apply(stage, docs, &Limits::default()))
}

fn ok(pipeline: Vec<Document>, input: Vec<Document>) -> Vec<Document> {
    run(pipeline, input).unwrap_or_else(|e| panic!("refused: {e}"))
}

fn refused(pipeline: Vec<Document>, input: Vec<Document>) {
    assert!(run(pipeline, input).is_err());
}

fn id_i64(d: &Document) -> i64 {
    match d.get("_id") {
        Some(Bson::Int32(n)) => i64::from(*n),
        Some(Bson::Int64(n)) => *n,
        other => panic!("not an integer _id: {other:?}"),
    }
}

// --- $sortByCount -----------------------------------------------------------

#[test]
fn reference_sort_by_count_sections() {
    // "sortByCount": a 3, null 2, then b and c once each in an order the reference leaves open.
    let out = ok(vec![doc! {"$sortByCount": "$k"}], stg());
    assert_eq!(out[0], doc! {"_id": "a", "count": 3});
    assert_eq!(out[1], doc! {"_id": Bson::Null, "count": 2});
    let mut tail: Vec<String> =
        out[2..].iter().map(|d| d.get_str("_id").unwrap().to_string()).collect();
    tail.sort();
    assert_eq!(tail, ["b", "c"]);
    assert!(out[2..].iter().all(|d| d.get("count") == Some(&Bson::Int32(1))));
    // "of an expression": odd n four, even three.
    let out = ok(vec![doc! {"$sortByCount": {"$mod": ["$n", 2]}}], stg());
    assert_eq!(out.len(), 2);
    assert_eq!((id_i64(&out[0]), out[0].get("count")), (1, Some(&Bson::Int32(4))));
    assert_eq!((id_i64(&out[1]), out[1].get("count")), (0, Some(&Bson::Int32(3))));
    // "of an object": refused.
    refused(vec![doc! {"$sortByCount": {"who": "$k"}}], stg());
    // "of $$ROOT": two groups of one.
    let out =
        ok(vec![doc! {"$match": {"_id": {"$lte": 2}}}, doc! {"$sortByCount": "$$ROOT"}], stg());
    assert_eq!(out.len(), 2);
    assert!(
        out.iter()
            .all(|d| d.get("count") == Some(&Bson::Int32(1)) && d.get_document("_id").is_ok())
    );
    // The bad operands, "{}" and an empty input.
    for bad in [
        Bson::Int32(1),
        Bson::Boolean(true),
        Bson::Null,
        Bson::Array(vec!["$k".into()]),
        Bson::String("k".into()),
        Bson::String("$".into()),
        Bson::Document(doc! {}),
    ] {
        refused(vec![doc! {"$sortByCount": bad}], stg());
    }
    assert!(ok(vec![doc! {"$match": {"_id": 999}}, doc! {"$sortByCount": "$k"}], stg()).is_empty());
}

// --- $replaceWith -----------------------------------------------------------

#[test]
fn reference_replace_with_sections() {
    let one = |p: Document| ok(vec![doc! {"$match": {"_id": 1}}, p], stg());
    assert_eq!(one(doc! {"$replaceWith": "$a"}), vec![doc! {"x": 1, "y": 2}]);
    assert_eq!(
        one(doc! {"$replaceWith": {"id": "$_id", "both": {"$concat": ["k", "$k"]}}}),
        vec![doc! {"id": 1, "both": "ka"}]
    );
    assert_eq!(
        ok(
            vec![
                doc! {"$match": {"_id": {"$in": [1, 3]}}},
                doc! {"$replaceWith": {"$mergeObjects": [{"x": 0}, "$a"]}}
            ],
            stg()
        ),
        vec![doc! {"x": 1, "y": 2}, doc! {"x": 0}]
    );
    refused(vec![doc! {"$match": {"_id": 3}}, doc! {"$replaceWith": "$a"}], stg());
    refused(vec![doc! {"$match": {"_id": 3}}, doc! {"$replaceWith": "$n"}], stg());
}

// --- $unset -----------------------------------------------------------------

#[test]
fn reference_unset_sections() {
    let one = |id: i32, p: Document| ok(vec![doc! {"$match": {"_id": id}}, p], stg());
    let without_k = one(1, doc! {"$unset": "k"});
    assert_eq!(
        without_k,
        vec![
            doc! {"_id": 1, "n": 1, "a": {"x": 1, "y": 2}, "items": [{"sku": 1, "q": 5}, {"sku": 2}, 7]}
        ]
    );
    assert_eq!(
        one(1, doc! {"$unset": ["k", "a.x", "items.q"]}),
        vec![doc! {"_id": 1, "n": 1, "a": {"y": 2}, "items": [{"sku": 1}, {"sku": 2}, 7]}]
    );
    assert_eq!(one(2, doc! {"$unset": ["_id", "a", "n"]}), vec![doc! {"k": "b"}]);
    assert_eq!(one(4, doc! {"$unset": ["nope", "a.b"]}), vec![doc! {"_id": 4, "k": "c", "n": 4}]);
    for bad in [
        Bson::Array(vec![]),
        Bson::Array(vec![Bson::Int32(1)]),
        Bson::Int32(1),
        Bson::Null,
        Bson::String(String::new()),
        Bson::Array(vec!["a".into(), "a".into()]),
        Bson::Array(vec!["a".into(), "a.b".into()]),
        Bson::String("$a".into()),
        Bson::String("a..b".into()),
        Bson::Document(doc! {}),
    ] {
        assert!(aggregate::parse(&[doc! {"$unset": bad.clone()}]).is_err(), "{bad:?}");
    }
}

// --- $sample ----------------------------------------------------------------

#[test]
fn reference_sample_sections() {
    let count = |size: Bson| ok(vec![doc! {"$sample": {"size": size}}], stg()).len();
    assert_eq!(count(Bson::Int32(3)), 3);
    assert_eq!(count(Bson::Int32(100)), 7);
    assert_eq!(count(Bson::Double(2.0)), 2);
    // "bad size 0" answers []; "1.5" answers one document.
    assert_eq!(count(Bson::Int32(0)), 0);
    assert_eq!(count(Bson::Double(1.5)), 1);
    for bad in [Bson::Int32(-1), Bson::String("2".into()), Bson::Null, Bson::Document(doc! {})] {
        assert!(aggregate::parse(&[doc! {"$sample": {"size": bad.clone()}}]).is_err(), "{bad:?}");
    }
    assert!(aggregate::parse(&[doc! {"$sample": {}}]).is_err());
    assert!(aggregate::parse(&[doc! {"$sample": {"size": 1, "seed": 3}}]).is_err());
    // "distinct": five different documents.
    let out = ok(
        vec![doc! {"$sample": {"size": 5}}, doc! {"$group": {"_id": "$_id"}}, doc! {"$count": "n"}],
        stg(),
    );
    assert_eq!(out[0].get("n"), Some(&Bson::Int32(5)));
}

// --- $redact ----------------------------------------------------------------

fn prune_at_5() -> Document {
    doc! {"$redact": {"$cond": {"if": {"$eq": ["$level", 5]}, "then": "$$PRUNE", "else": "$$DESCEND"}}}
}

#[test]
fn reference_redact_sections() {
    let pruned = vec![doc! {"_id": 1, "level": 1, "title": "t1",
    "ship": [{"level": 3, "city": "A"}, 9],
    "meta": {"level": 2, "tags": ["x"]}}];
    assert_eq!(ok(vec![prune_at_5()], lv()), pruned);
    let array_form = doc! {"$redact": {"$cond": [{"$eq": ["$level", 5]}, "$$PRUNE", "$$DESCEND"]}};
    assert_eq!(ok(vec![array_form], lv()), pruned);
    // "keep": document 1 kept whole, 2 pruned.
    let keep = doc! {"$redact": {"$cond": [{"$eq": ["$level", 5]}, "$$PRUNE", "$$KEEP"]}};
    assert_eq!(ok(vec![keep], lv()), vec![lv().remove(0)]);
    // The bare variables and their string spellings.
    let n = |v: &str| ok(vec![doc! {"$redact": v}], lv()).len();
    assert_eq!((n("$$KEEP"), n("$$PRUNE"), n("$$DESCEND")), (2, 0, 2));
    assert_eq!((n("keep"), n("prune"), n("descend")), (2, 0, 2));
    // "ROOT at depth".
    let at_depth = doc! {"$redact": {"$cond": [
    {"$or": [{"$eq": [{"$type": "$level"}, "missing"]}, {"$lte": ["$level", "$$ROOT.level"]}]},
    "$$DESCEND", "$$PRUNE"]}};
    assert_eq!(
        ok(vec![at_depth], lv()),
        vec![
            doc! {"_id": 1, "level": 1, "title": "t1", "ship": [9]},
            doc! {"_id": 2, "level": 5, "title": "secret"},
        ]
    );
    // "set ops": every level lacks tags, so the top is pruned.
    let set_ops = doc! {"$redact": {"$cond": {
    "if": {"$gt": [{"$size": {"$setIntersection": [{"$ifNull": ["$tags", []]}, ["x"]]}}, 0]},
    "then": "$$DESCEND", "else": "$$PRUNE"}}};
    assert!(ok(vec![set_ops], lv()).is_empty());
    // Bad answers, at run time.
    for bad in [Bson::Int32(1), Bson::Null, Bson::String("other".into()), Bson::Boolean(true)] {
        refused(vec![doc! {"$redact": bad}], lv());
    }
    // Outside $redact: "Use of undefined variable: PRUNE" there, refused here, as is $$CLUSTER_TIME.
    let err = aggregate::parse(&[doc! {"$project": {"x": "$$PRUNE"}}]).unwrap_err().to_string();
    assert!(err.contains("Use of undefined variable: PRUNE"), "{err}");
    assert!(aggregate::parse(&[doc! {"$project": {"x": "$$CLUSTER_TIME"}}]).is_err());
}

// --- $unionWith, the parse sections ------------------------------------------

#[test]
fn reference_union_with_parse_sections() {
    for (label, bad) in [
        ("no coll", doc! {"pipeline": []}),
        ("unknown field", doc! {"coll": "stg_other", "extra": 1}),
        ("pipeline not array", doc! {"coll": "stg_other", "pipeline": {}}),
        ("out inside", doc! {"coll": "stg_other", "pipeline": [{"$out": "zz"}]}),
        ("merge inside", doc! {"coll": "stg_other", "pipeline": [{"$merge": {"into": "zz"}}]}),
    ] {
        assert!(aggregate::parse(&[doc! {"$unionWith": bad}]).is_err(), "{label}");
    }
    assert!(aggregate::parse(&[doc! {"$unionWith": 1}]).is_err(), "number");
    // "facet inside" is accepted.
    assert!(
        aggregate::parse(&[
            doc! {"$unionWith": {"coll": "stg_other", "pipeline": [{"$facet": {"f": []}}]}}
        ])
        .is_ok()
    );
}
