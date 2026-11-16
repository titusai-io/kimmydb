//! The effects of an upsert that matched nothing (`plan_upsert`): the layers of the creation, as
//! written, and the creation through the writer's own entries.

use bson::{Bson, Document, doc};
use kimmy_core::effects::{CellTag, UpsertEffects};
use kimmy_core::merge_state::{JoinOptions, LossKind, Path, Value, Writer};

use crate::update::{self, Update};
use crate::update_effects::{UpsertPlan, plan_upsert};
use crate::update_effects_tests::{NOW, bytes, filt, np, p, parse, render, v, wid};

fn plan(filter: Document, u: Document) -> (UpsertEffects, Document) {
    match plan_upsert(&parse(u.clone()), &filt(filter.clone()), NOW)
        .unwrap_or_else(|e| panic!("{filter} {u}: refused: {e}"))
    {
        UpsertPlan::Effects { effects, post_image } => (effects, post_image),
        UpsertPlan::Replace { .. } => panic!("a replacement"),
    }
}

fn refused(filter: Document, u: Document) -> String {
    match plan_upsert(&parse(u), &filt(filter), NOW) {
        Err(e) => e.to_string(),
        Ok(p) => panic!("not refused: {p:?}"),
    }
}

/// The path of every field of `post`, in the order of the document: a depth-first walk that names a
/// document-valued field before its members. It is the order `creation_list` asks for: it holds
/// the path of every cell of the creation, the filter's cells included.
fn post_image_order(post: &Document) -> Vec<Path> {
    fn walk(d: &Document, at: &mut Vec<String>, out: &mut Vec<Path>) {
        for (name, value) in d {
            at.push(name.clone());
            out.push(Path::new(at.iter().cloned()).expect("a path of the post-image"));
            if let Bson::Document(inner) = value {
                walk(inner, at, out);
            }
            at.pop();
        }
    }
    let mut out = Vec::new();
    walk(post, &mut Vec::new(), &mut out);
    out
}

/// Create the document through the writer's typed creation entry, [`UpsertEffects::apply`]: the
/// cells (the increments' typed operands among them) in the order of the post-image `post`.
fn create(fx: &UpsertEffects, post: &Document) -> Writer {
    create_as(fx, post, 5)
}

/// [`create`] by the writer numbered `who`: another member making the same creation.
fn create_as(fx: &UpsertEffects, post: &Document, who: u8) -> Writer {
    let mut w = Writer::new(wid(who), JoinOptions::default());
    fx.apply(&mut w, &post_image_order(post), 0).expect("the creation writes");
    w
}

/// The writer renders the inserted document: the same fields in the same order, the same bytes.
fn creates(filter: Document, u: Document) -> (UpsertEffects, Document) {
    let (fx, post) = plan(filter, u);
    let got = render(&create(&fx, &post));
    assert_eq!(got, post, "the writer creates another document");
    let (mut a, mut b) = (Vec::new(), Vec::new());
    got.to_writer(&mut a).unwrap();
    post.to_writer(&mut b).unwrap();
    assert_eq!(a, b, "same fields, other order or types: {got} / {post}");
    (fx, post)
}

#[test]
fn set_only_set_on_insert_only_and_the_filter_layer() {
    let (fx, post) = creates(doc! {"k": 5}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(5))]);
    assert_eq!(fx.set, vec![(p("a"), v(1))]);
    assert!(fx.set_on_insert.is_empty() && fx.inc.is_empty());
    assert_eq!(post, doc! {"k": 5, "a": 1});

    let (fx, _) = creates(doc! {}, doc! {"$setOnInsert": {"s": 1}});
    assert_eq!(fx.set_on_insert, vec![(p("s"), v(1))]);
    assert!(fx.set.is_empty() && fx.from_filter.is_empty());

    let (fx, post) = creates(doc! {"k": 1}, doc! {"$set": {}});
    assert!(fx.set.is_empty());
    assert_eq!(post, doc! {"k": 1});
}

#[test]
fn a_set_overrides_the_filter_and_dotted_leaves_merge() {
    let (_, post) = creates(doc! {"a": 1}, doc! {"$set": {"a": 2}});
    assert_eq!(post, doc! {"a": 2});
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$set": {"a.c": 2}});
    assert_eq!(fx.from_filter, vec![(np(&["a", "b"]), v(1))]);
    assert_eq!(fx.set, vec![(np(&["a", "c"]), v(2))]);
    assert_eq!(post, doc! {"a": {"b": 1, "c": 2}});
    // a whole-document filter value and a field set below it
    let (_, post) = creates(doc! {"a": {"x": 1}}, doc! {"$set": {"a.y": 2}});
    assert_eq!(post, doc! {"a": {"x": 1, "y": 2}});
}

/// The creation values of the register operators, listed in name order like the document the
/// reference inserts (q7s2.out u8: arr, b, d, hi, lo, m, st).
#[test]
fn the_creation_values_of_the_register_operators_go_in_set() {
    let (fx, post) = creates(
        doc! {},
        doc! {
            "$mul": {"m": 3}, "$min": {"lo": 2}, "$max": {"hi": 7}, "$bit": {"b": {"or": 6}},
            "$push": {"arr": 1}, "$addToSet": {"set": "x"}, "$currentDate": {"d": true},
        },
    );
    assert!(fx.inc.is_empty() && fx.set_on_insert.is_empty());
    let want = vec![
        (p("m"), v(0)),
        (p("lo"), v(2)),
        (p("hi"), v(7)),
        (p("b"), v(6)),
        (p("arr"), v(vec![1])),
        (p("set"), v(vec!["x"])),
        (p("d"), Value::Date(NOW)),
    ];
    let mut got = fx.set.clone();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    let mut want_sorted = want.clone();
    want_sorted.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, want_sorted);
    // listed in the order of the inserted document
    let order: Vec<&str> = post.keys().map(String::as_str).collect();
    let listed: Vec<String> = fx.set.iter().map(|(q, _)| q.name().to_string()).collect();
    assert_eq!(listed, order);
    assert_eq!(listed, ["arr", "b", "d", "hi", "lo", "m", "set"]);
}

#[test]
fn inc_is_the_operand_and_several_incs_are_allowed() {
    let (fx, post) =
        creates(doc! {"k": 1}, doc! {"$inc": {"n": 2i64, "q.m": 3i64}, "$set": {"a": 1}});
    assert_eq!(fx.inc, vec![(p("n"), v(2i64)), (np(&["q", "m"]), v(3i64))]);
    assert_eq!(post.get("n"), Some(&Bson::Int64(2)));
    // typed operands as written
    let (fx, _) = plan(doc! {}, doc! {"$inc": {"a": 1, "b": 1.5, "c": 2i64}});
    assert_eq!(fx.inc, vec![(p("a"), v(1)), (p("b"), v(1.5)), (p("c"), v(2i64))]);
}

#[test]
fn an_unset_removes_the_filter_field_and_a_rename_moves_it() {
    let (fx, post) = creates(doc! {"a": 1, "b": 2}, doc! {"$unset": {"a": ""}});
    assert_eq!(fx.set, vec![(p("a"), Value::Unset)]);
    assert_eq!(post, doc! {"b": 2});
    let (fx, post) = creates(doc! {"a": 1}, doc! {"$rename": {"a": "b"}});
    assert_eq!(fx.set, vec![(p("a"), Value::Unset), (p("b"), v(1))]);
    assert_eq!(post, doc! {"b": 1});
}

/// The new fields of an upsert are in name order, whatever order the update names them in and
/// whichever operator makes them, after the filter's fields (q7s2.out u1-u4, u7). The layers
/// keep the order of the inserted document inside each list.
#[test]
fn new_fields_are_sorted_like_the_reference() {
    // u4: `$set` zz,aa and `$setOnInsert` s2,s1 give aa, s1, s2, zz
    let (fx, post) =
        plan(doc! {}, doc! {"$set": {"zz": 1, "aa": 2}, "$setOnInsert": {"s2": 3, "s1": 4}});
    let keys: Vec<&str> = post.keys().map(String::as_str).collect();
    assert_eq!(keys, ["aa", "s1", "s2", "zz"]);
    assert_eq!(fx.set, vec![(p("aa"), v(2)), (p("zz"), v(1))]);
    assert_eq!(fx.set_on_insert, vec![(p("s1"), v(4)), (p("s2"), v(3))]);
    // u1: the filter fields k, j (stored j, k), then every new field sorted across the operators
    let (fx, post) = plan(
        doc! {"k": 1, "j": 2},
        doc! {"$set": {"b": 1, "a": 2}, "$setOnInsert": {"c": 1, "z": 2}, "$inc": {"n": 1, "m": 1}},
    );
    let keys: Vec<&str> = post.keys().map(String::as_str).collect();
    assert_eq!(keys, ["j", "k", "a", "b", "c", "m", "n", "z"]);
    assert_eq!(fx.from_filter, vec![(p("k"), v(1)), (p("j"), v(2))]);
    assert_eq!(fx.set, vec![(p("a"), v(2)), (p("b"), v(1))]);
    assert_eq!(fx.set_on_insert, vec![(p("c"), v(1)), (p("z"), v(2))]);
    // u3: an `$inc` written first changes nothing about the order
    let (_, post) =
        plan(doc! {"k": 1}, doc! {"$inc": {"n": 1}, "$set": {"b": 1}, "$setOnInsert": {"c": 1}});
    let keys: Vec<&str> = post.keys().map(String::as_str).collect();
    assert_eq!(keys, ["k", "b", "c", "n"]);
    // u7: a filter field that is also `$set` keeps its place
    let (_, post) = plan(doc! {"a": 1, "b": 2}, doc! {"$set": {"a": 9, "c": 3}});
    assert_eq!(bytes(&post), bytes(&doc! {"a": 9, "b": 2, "c": 3}));
}

/// The seed of an upsert by filter shape, and the fields the update adds after it
/// (q7s2d.out 3 f1-f7: the levels a dotted key makes are sorted, a document given whole keeps its
/// own order, the new fields follow in name order).
#[test]
fn the_seed_by_filter_shape_and_the_new_fields_after_it() {
    let shapes: Vec<(&str, Document, Document, Document)> = vec![
        (
            "f1 {a.c, a.b}",
            doc! {"a.c": 1, "a.b": 2},
            doc! {"z": 1},
            doc! {"a": {"b": 2, "c": 1}, "z": 1},
        ),
        (
            "f2 {a.b, a.c}",
            doc! {"a.b": 2, "a.c": 1},
            doc! {"z": 1},
            doc! {"a": {"b": 2, "c": 1}, "z": 1},
        ),
        (
            "f3 {a: {c, b}} keeps the order of the value",
            doc! {"a": {"c": 1, "b": 2}},
            doc! {"z": 1},
            doc! {"a": {"c": 1, "b": 2}, "z": 1},
        ),
        (
            "f4 {a.c, a.b, b}",
            doc! {"a.c": 1, "a.b": 2, "b": 3},
            doc! {"z": 1},
            doc! {"a": {"b": 2, "c": 1}, "b": 3, "z": 1},
        ),
        (
            "f5 {x.y.z2, x.y.z1}",
            doc! {"x.y.z2": 1, "x.y.z1": 2},
            doc! {"z": 1},
            doc! {"x": {"y": {"z1": 2, "z2": 1}}, "z": 1},
        ),
        (
            "f6 {a.c, b, a.b}",
            doc! {"a.c": 1, "b": 3, "a.b": 2},
            doc! {"z": 1},
            doc! {"a": {"b": 2, "c": 1}, "b": 3, "z": 1},
        ),
    ];
    for (label, filter, set, want) in shapes {
        let (_, post) = plan(filter.clone(), doc! {"$set": set});
        assert_eq!(bytes(&post), bytes(&want), "{label}");
    }
    // the same filters with `$set {zz, aa}`: the new fields are sorted after the seed
    let (_, post) = plan(doc! {"a.c": 1, "a.b": 2, "b": 3}, doc! {"$set": {"zz": 1, "aa": 2}});
    assert_eq!(
        bytes(&post),
        bytes(&doc! {"a": {"b": 2, "c": 1}, "b": 3, "aa": 2, "zz": 1}),
        "f4 with zz, aa"
    );
    let (_, post) = plan(doc! {"a": {"c": 1, "b": 2}}, doc! {"$set": {"zz": 1, "aa": 2}});
    assert_eq!(bytes(&post), bytes(&doc! {"a": {"c": 1, "b": 2}, "aa": 2, "zz": 1}), "f3 zz, aa");
}

/// A multi-field filter with an operator body and a pipeline body (q7s2e.out 2a-2d, 2g, 2h, 2j):
/// the seed is in name order, a restated filter field keeps its place, the fields the update adds
/// follow, appended after the seed.
#[test]
fn an_upsert_with_a_multi_field_filter_by_body() {
    let seeded = |extra: Document| {
        let mut d = doc! {"aa": 2, "k": 5, "m": {"p": 4, "q": 3}, "zz": 1};
        d.extend(extra);
        d
    };
    // filter written in another order than it is stored in (q7s2e.out 2g)
    let filter = || doc! {"k": 5, "m.p": 4, "aa": 2, "m.q": 3, "zz": 1};
    // 2c: operator `$set z`
    let (_, post) = plan(filter(), doc! {"$set": {"z": 1}});
    assert_eq!(bytes(&post), bytes(&seeded(doc! {"z": 1})), "2c");
    // 2d: operator `$set zz, aa` restates two filter fields with the same values
    let (_, post) = plan(filter(), doc! {"$set": {"zz": 1, "aa": 2}});
    assert_eq!(bytes(&post), bytes(&seeded(doc! {})), "2d");
    let pipeline = |stage: Document| {
        let stages = vec![Bson::Document(stage)];
        let update = update::parse_pipeline(&stages, &[]).unwrap();
        match plan_upsert(&update, &filt(filter()), NOW).unwrap() {
            UpsertPlan::Effects { post_image, .. } => post_image,
            UpsertPlan::Replace { .. } => panic!("a replacement"),
        }
    };
    // 2a, 2g: pipeline `$set z`
    assert_eq!(bytes(&pipeline(doc! {"$set": {"z": 1}})), bytes(&seeded(doc! {"z": 1})), "2a");
    // 2b: pipeline `$set zz, aa`
    assert_eq!(bytes(&pipeline(doc! {"$set": {"zz": 1, "aa": 2}})), bytes(&seeded(doc! {})), "2b");
    // 2h: a pipeline that restates a filter field with another value and adds one
    assert_eq!(
        bytes(&pipeline(doc! {"$set": {"aa": 9, "nn": 1}})),
        bytes(&doc! {"aa": 9, "k": 5, "m": {"p": 4, "q": 3}, "zz": 1, "nn": 1}),
        "2h"
    );
    // 2j: a nested seed split by a top-level field
    let stages = vec![Bson::Document(doc! {"$set": {"z": 1}})];
    let update = update::parse_pipeline(&stages, &[]).unwrap();
    let UpsertPlan::Effects { post_image, .. } =
        plan_upsert(&update, &filt(doc! {"a.c": 1, "b": 3, "a.b": 2}), NOW).unwrap()
    else {
        panic!("effects")
    };
    assert_eq!(bytes(&post_image), bytes(&doc! {"a": {"b": 2, "c": 1}, "b": 3, "z": 1}), "2j");
}

/// The seed forms of the shared function (`kimmy-query/src/seed.rs`): a single-value `$in`, a
/// single-value `$all` and a one-branch `$or` pin `a` to 5, so `from_filter` has the leaf
/// `a = 5`, a creation cell; a `$in` of two values pins nothing. The SEED is recorded (q7s2g.out
/// g4.2 `$in` of one, g4.3 `$in` of two, g4.4 `$all` of one, g4.5 `$or` of one; g4.1-g4.12 in
/// all). That `from_filter` follows the shared function is by construction, so the effects-layer
/// side of these forms is UNRECORDED.
#[test]
fn the_new_seed_forms_give_from_filter_a_creation_leaf() {
    for (label, filter) in [
        ("g4.2 $in of one", doc! {"a": {"$in": [5]}}),
        ("g4.4 $all of one", doc! {"a": {"$all": [5]}}),
        ("g4.5 $or of one", doc! {"$or": [{"a": 5}]}),
    ] {
        let (fx, post) = plan(filter, doc! {"$set": {"z": 1}});
        assert_eq!(fx.from_filter, vec![(p("a"), v(5))], "{label}");
        assert_eq!(post, doc! {"a": 5, "z": 1}, "{label}");
        let cells = fx.creation_cells().unwrap();
        let a = cells.iter().find(|c| c.path == p("a")).expect("a cell");
        assert_eq!((a.tag, &a.value), (CellTag::Creation, &v(5)), "{label}");
    }
    // g4.3: a `$in` of two values pins nothing: no seed, no from_filter
    let (fx, post) = plan(doc! {"a": {"$in": [5, 6]}}, doc! {"$set": {"z": 1}});
    assert!(fx.from_filter.is_empty());
    assert_eq!(post, doc! {"z": 1});
}

#[test]
fn the_filters_id_paths_are_left_out() {
    let (fx, _) = plan(doc! {"_id": 7, "k": 1}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(1))]);
    let (fx, _) = plan(doc! {"_id.x": 7, "k": 1}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(1))]);
}

#[test]
fn a_pipeline_upsert_sets_what_the_stages_changed() {
    let stages = vec![Bson::Document(doc! {"$set": {"t": 2, "k": 9}})];
    let update = update::parse_pipeline(&stages, &[]).unwrap();
    let UpsertPlan::Effects { effects, post_image } =
        plan_upsert(&update, &filt(doc! {"k": 1}), NOW).unwrap()
    else {
        panic!("effects")
    };
    assert_eq!(effects.from_filter, vec![(p("k"), v(1))]);
    assert_eq!(effects.set, vec![(p("k"), v(9)), (p("t"), v(2))]);
    assert_eq!(post_image, doc! {"k": 9, "t": 2});
    assert_eq!(render(&create(&effects, &post_image)), post_image);
}

#[test]
fn a_replacement_upsert_is_not_an_effect_list() {
    let u: Update = update::parse(&doc! {"b": 2}).unwrap();
    assert!(matches!(
        plan_upsert(&u, &filt(doc! {"k": 1}), NOW).unwrap(),
        UpsertPlan::Replace { .. }
    ));
}

#[test]
fn the_refusals_of_the_creation_are_400s_at_plan_time() {
    // an `$inc` ABOVE a filter path, or below a scalar one: the reference refuses them too
    // (q7s2.out f4 line 449, f5 line 454; q7s2b.out 4j line 208 and line 163)
    assert!(!refused(doc! {"a": {"b": 1}}, doc! {"$inc": {"a": 1}}).is_empty());
    assert!(!refused(doc! {"a.b": 1}, doc! {"$inc": {"a": 1}}).is_empty());
    assert!(!refused(doc! {"a.b.c": 1}, doc! {"$inc": {"a.b": 1}}).is_empty());
    assert!(!refused(doc! {"a": 5}, doc! {"$inc": {"a.x": 1}}).is_empty());
    // below a scalar filter value
    assert!(!refused(doc! {"a": 5}, doc! {"$set": {"a.x": 1}}).is_empty());
    // a `$` on an insert is the engine's refusal
    assert!(refused(doc! {"k": 1}, doc! {"$set": {"items.$.x": 1}}).contains("$"));
    // two filter equalities on one path
    assert!(!refused(doc! {"$and": [{"a": 1}, {"a": 2}]}, doc! {"$set": {"z": 1}}).is_empty());
}

#[test]
fn a_filter_leaf_beside_an_inc_is_accepted() {
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$inc": {"a.c": 1i64}});
    assert_eq!(fx.inc, vec![(np(&["a", "c"]), v(1i64))]);
    assert_eq!(post, doc! {"a": {"b": 1, "c": 1i64}});
}

// ---------------------------------------------------------------------------------------------
// Amendment 7: an `$inc` on a filter-written path is a creation-tagged `$setOnInsert` of the
// engine's value (no slot, nothing silent)
// ---------------------------------------------------------------------------------------------

/// The recorded cases: no `inc`, the reference's document (q7s2.out 431-445, q7s2b.out 200-204,
/// q7s2g.out 307-311).
#[test]
fn an_inc_on_a_filter_path_is_a_creation_value_not_a_slot() {
    // q7s2.out f1, line 435: {a: 5} + $inc a 1 creates a: 6
    let (fx, post) = creates(doc! {"a": 5}, doc! {"$inc": {"a": 1}});
    assert_eq!(post, doc! {"a": 6});
    assert!(fx.inc.is_empty() && fx.set.is_empty());
    assert_eq!(fx.from_filter, vec![(p("a"), v(5))]);
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(6))]);
    // q7s2b.out 4i, line 204: {"a.b": 1} + $inc a.b 1 creates {a: {b: 2}}
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$inc": {"a.b": 1}});
    assert_eq!(post, doc! {"a": {"b": 2}});
    assert!(fx.inc.is_empty());
    assert_eq!(fx.set_on_insert, vec![(np(&["a", "b"]), v(2))]);
    // q7s2.out f2, line 440: {a: {b: 1}} + $inc a.c 1 creates {a: {b: 1, c: 1}}
    let (fx, post) = creates(doc! {"a": {"b": 1}}, doc! {"$inc": {"a.c": 1}});
    assert_eq!(post, doc! {"a": {"b": 1, "c": 1}});
    assert!(fx.inc.is_empty());
    assert_eq!(fx.set_on_insert, vec![(np(&["a", "c"]), v(1))]);
    // an empty Document filter value counts as a Document: {a: {}} + $inc a.c
    let (fx, post) = creates(doc! {"a": {}}, doc! {"$inc": {"a.c": 1}});
    assert_eq!(post, doc! {"a": {"c": 1}});
    assert!(fx.inc.is_empty());
    // q7s2.out f3, line 445: {"a.b": 1} + $inc a.c is NOT routed: it stays a counter slot
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$inc": {"a.c": 1}});
    assert_eq!(post, doc! {"a": {"b": 1, "c": 1}});
    assert_eq!(fx.inc, vec![(np(&["a", "c"]), v(1))]);
    assert!(fx.set_on_insert.is_empty());
    // q7s2g.out g3i, lines 307-311: filter {9: 2, 10: 1} + $inc 9, 10, a + $set 5, z: the two
    // filter paths are creation values (3 and 2), `a` (not in the filter) stays an `$inc`
    let (fx, post) = plan(
        doc! {"9": 2, "10": 1},
        doc! {"$inc": {"9": 1, "10": 1, "a": 1}, "$set": {"5": 1, "z": 1}},
    );
    assert_eq!(post.get("9"), Some(&Bson::Int32(3)));
    assert_eq!(post.get("10"), Some(&Bson::Int32(2)));
    let mut routed = fx.set_on_insert.clone();
    routed.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(routed, vec![(p("10"), v(2)), (p("9"), v(3))]);
    assert_eq!(fx.inc, vec![(p("a"), v(1))]);
    assert_eq!(fx.set.len(), 2);
}

/// The creation value is the engine's post-image, whatever the type of the operand and of the
/// filter's value (amendment 7, item 4). UNRECORDED against the reference except the Int32 cases
/// above: these follow the engine's typing (q7s2.out d1-d8 for the same sums on a stored value).
#[test]
fn the_creation_value_of_a_routed_inc_is_typed_by_the_engine() {
    // a Double result is a register, not a counter
    let (fx, post) = creates(doc! {"a": 5}, doc! {"$inc": {"a": 1.5}});
    assert_eq!(post, doc! {"a": 6.5});
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(6.5))]);
    assert!(fx.inc.is_empty());
    // an Int64 operand widens the Int32 filter value
    let (fx, _) = creates(doc! {"a": 5}, doc! {"$inc": {"a": 2i64}});
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(7i64))]);
    // a Decimal128 operand
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    let (fx, _) = creates(doc! {"a": 5}, doc! {"$inc": {"a": dec("1.5")}});
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(dec("6.5")))]);
    // a Double filter value
    let (fx, _) = creates(doc! {"a": 1.5}, doc! {"$inc": {"a": 1}});
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(2.5))]);
}

/// `$mul` on a filter-named path: the creation value goes in `set` (the register operators of
/// (c)), over the filter's cell. UNRECORDED against the reference: the document is the engine's.
#[test]
fn a_mul_on_a_filter_path_is_a_creation_value_in_set() {
    let (fx, post) = creates(doc! {"a": 5}, doc! {"$mul": {"a": 3}});
    assert_eq!(post, doc! {"a": 15});
    assert_eq!(fx.from_filter, vec![(p("a"), v(5))]);
    assert_eq!(fx.set, vec![(p("a"), v(15))]);
    assert!(fx.inc.is_empty() && fx.set_on_insert.is_empty());
}

fn int_of(d: &Document, name: &str) -> i64 {
    match d.get(name) {
        Some(Bson::Int32(n)) => i64::from(*n),
        Some(Bson::Int64(n)) => *n,
        other => panic!("{name}: {other:?} in {d}"),
    }
}

/// Amendment 7, item 5, first bullet: two identical concurrent creating upserts read the
/// reference's value (6), and leave ONE `clash` record for the lower-stamped equal register (the
/// join has no equal-value exemption): EXPECTED, not a defect. Every join order.
#[test]
fn two_identical_creating_upserts_read_the_reference_value_and_leave_one_clash() {
    let (fx, post) = plan(doc! {"a": 5}, doc! {"$inc": {"a": 1}});
    for (first, second) in [(5u8, 6u8), (6, 5)] {
        let mut x = create_as(&fx, &post, first);
        let y = create_as(&fx, &post, second);
        let fresh = x.receive(&y.state);
        assert_eq!(int_of(&render(&x), "a"), 6, "order {first} then {second}");
        let clashes = fresh.iter().filter(|k| k.kind == LossKind::Clash).count();
        assert_eq!(clashes, 1, "order {first} then {second}: {fresh:?}");
    }
}

/// Item 5, second bullet: `{$inc: {a: 1, n: 1}}` over filter `{a: 5}` gives `a` by last writer
/// (6) and `n` summed (2): no serial order of the reference produces it (its second upsert fails
/// whole). Stated, not hidden; the same class as `$set`/`$mul` beside an `$inc`.
#[test]
fn a_routed_inc_beside_a_counter_inc_gives_a_by_last_writer_and_n_summed() {
    let (fx, post) = plan(doc! {"a": 5}, doc! {"$inc": {"a": 1, "n": 1}});
    assert_eq!(fx.inc, vec![(p("n"), v(1))]);
    assert_eq!(fx.set_on_insert, vec![(p("a"), v(6))]);
    for (first, second) in [(5u8, 6u8), (6, 5)] {
        let mut x = create_as(&fx, &post, first);
        let y = create_as(&fx, &post, second);
        x.receive(&y.state);
        let d = render(&x);
        assert_eq!((int_of(&d, "a"), int_of(&d, "n")), (6, 2), "order {first} then {second}");
    }
}

/// Item 1: the routed value is creation-TAGGED, so an explicit insert racing the upsert-creation
/// is never silent: both members converge and an `insertLost` is reported (no plain computed
/// cell survives beside the insert's fields).
#[test]
fn a_routed_inc_never_survives_silently_beside_an_explicit_insert() {
    let (fx, post) = plan(doc! {"a": 5}, doc! {"$inc": {"a": 1}});
    for order in [0, 1] {
        let mut inserter = Writer::new(wid(7), JoinOptions::default());
        inserter.insert(&[(p("x"), v(1))], 0).expect("the insert writes");
        let mut creator = create_as(&fx, &post, 6);
        let (fresh_a, fresh_b) = if order == 0 {
            (inserter.receive(&creator.state), creator.receive(&inserter.state))
        } else {
            (creator.receive(&inserter.state), inserter.receive(&creator.state))
        };
        assert_eq!(render(&inserter), render(&creator), "the members converge");
        let lost = fresh_a.iter().chain(fresh_b.iter()).any(|k| k.kind == LossKind::InsertLost);
        assert!(lost, "no insertLost: {fresh_a:?} {fresh_b:?} over {}", render(&creator));
    }
}
