//! A differential for ADR-209: an arrayFilters entry written with field
//! conditions must select exactly the elements the same predicate written as
//! `$expr` over `$$e` selects, and an `$expr` over `$$e.f` must answer what a
//! document-level `$expr` over `$f` answers on the element.

use bson::{Bson, Document, doc};
use kimmy_query::{filter, update};
use proptest::prelude::*;

const OPS: [&str; 6] = ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"];

fn field_value() -> impl Strategy<Value = Option<Bson>> {
    prop_oneof![
        Just(None),
        Just(Some(Bson::Null)),
        (0i32..5).prop_map(|n| Some(Bson::Int32(n))),
        Just(Some(Bson::Int64(3))),
        Just(Some(Bson::Double(2.5))),
        Just(Some(Bson::Double(2.0))),
        Just(Some(Bson::Double(f64::NAN))),
        Just(Some(Bson::String("x".into()))),
        Just(Some(Bson::Boolean(true))),
        Just(Some(Bson::Document(doc! {}))),
    ]
}

fn element() -> impl Strategy<Value = Bson> {
    prop_oneof![
        4 => (field_value(), field_value()).prop_map(|(a, b)| {
            let mut d = Document::new();
            if let Some(a) = a { d.insert("a", a); }
            if let Some(b) = b { d.insert("b", b); }
            Bson::Document(d)
        }),
        1 => field_value().prop_map(|v| v.unwrap_or(Bson::Null)),
    ]
}

fn constant() -> impl Strategy<Value = Bson> {
    prop_oneof![(0i32..5).prop_map(Bson::Int32), Just(Bson::Double(2.5))]
}

/// A leaf: (field or None for the element itself, op index, constant).
#[derive(Clone, Debug)]
enum Tree {
    Leaf(Option<&'static str>, usize, Bson, bool),
    And(Vec<Tree>),
    Or(Vec<Tree>),
    Nor(Vec<Tree>),
}

fn tree() -> impl Strategy<Value = Tree> {
    let leaf = (
        prop_oneof![Just(Some("a")), Just(Some("b")), Just(None)],
        0usize..6,
        constant(),
        any::<bool>(),
    )
        .prop_map(|(f, o, c, as_expr)| Tree::Leaf(f, o, c, as_expr));
    leaf.prop_recursive(3, 12, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 1..3).prop_map(Tree::And),
            prop::collection::vec(inner.clone(), 1..3).prop_map(Tree::Or),
            prop::collection::vec(inner, 1..3).prop_map(Tree::Nor),
        ]
    })
}

fn numeric_guard(v: &str) -> Bson {
    // Numbers sort strictly between null and strings, NaN included.
    Bson::Document(doc! { "$and": [ { "$gt": [v, Bson::Null] }, { "$lt": [v, ""] } ] })
}

fn render(t: &Tree, force_fields: bool) -> Document {
    match t {
        Tree::Leaf(f, o, c, as_expr) => {
            let op = OPS[*o];
            if force_fields || !as_expr {
                let key = f.map_or("e".to_string(), |f| format!("e.{f}"));
                doc! { key: { op: c.clone() } }
            } else {
                let v = f.map_or("$$e".to_string(), |f| format!("$$e.{f}"));
                let cmp = Bson::Document(doc! { op: [v.clone(), c.clone()] });
                // Field conditions bracket by type for the four orderings;
                // `$expr` compares across types, so guard to numbers.
                let e = if op == "$eq" || op == "$ne" {
                    cmp
                } else {
                    Bson::Document(doc! { "$and": [ numeric_guard(&v), cmp ] })
                };
                doc! { "$expr": e }
            }
        }
        Tree::And(ts) => {
            doc! { "$and": ts.iter().map(|t| Bson::Document(render(t, force_fields))).collect::<Vec<_>>() }
        }
        Tree::Or(ts) => {
            doc! { "$or": ts.iter().map(|t| Bson::Document(render(t, force_fields))).collect::<Vec<_>>() }
        }
        Tree::Nor(ts) => {
            doc! { "$nor": ts.iter().map(|t| Bson::Document(render(t, force_fields))).collect::<Vec<_>>() }
        }
    }
}

fn selected(entry: &Document, xs: &[Bson]) -> Result<Vec<bool>, String> {
    let upd = doc! { "$set": { "xs.$[e]": "HIT-MARK" } };
    let parsed =
        update::parse_with_filters(&upd, std::slice::from_ref(entry)).map_err(|e| e.to_string())?;
    let mut d = doc! { "_id": 1, "xs": xs.to_vec() };
    update::apply(&parsed, &mut d, 0).map_err(|e| e.to_string())?;
    Ok(d.get_array("xs").unwrap().iter().map(|v| v == &Bson::String("HIT-MARK".into())).collect())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    #[test]
    fn field_conditions_and_expr_select_the_same_elements(
        t in tree(),
        xs in prop::collection::vec(element(), 1..6),
    ) {
        let reference = render(&t, true);
        let mixed = render(&t, false);
        let want = selected(&reference, &xs);
        let got = selected(&mixed, &xs);
        prop_assert_eq!(&want, &got, "\nref {}\nmix {}\nxs {:?}", reference, mixed, xs);
    }

    #[test]
    fn element_expr_matches_document_expr_on_the_element(
        o in 0usize..6,
        lhs in prop_oneof![Just("a"), Just("b")],
        rhs in prop_oneof![Just("a"), Just("b")],
        add in any::<bool>(),
        xs in prop::collection::vec(element(), 1..6),
    ) {
        let op = OPS[o];
        let (el, dl) = if add {
            (Bson::Document(doc!{ "$add": [format!("$$e.{lhs}"), 1] }), Bson::Document(doc!{ "$add": [format!("${lhs}"), 1] }))
        } else {
            (Bson::String(format!("$$e.{lhs}")), Bson::String(format!("${lhs}")))
        };
        let entry = doc! { "$expr": { op: [el, format!("$$e.{rhs}")] } };
        let docf = filter::parse(&doc! { "$expr": { op: [dl, format!("${rhs}")] } }).unwrap();
        let got = selected(&entry, &xs);
        let want: Result<Vec<bool>, String> = xs.iter().map(|x| match x {
            Bson::Document(d) => filter::matches(&docf, d).map_err(|e| e.to_string()),
            // A scalar: `$$e.f` is null; a document-level evaluation of `{}`.
            _ => filter::matches(&docf, &doc!{}).map_err(|e| e.to_string()),
        }).collect();
        match (&want, &got) {
            (Ok(w), Ok(g)) => prop_assert_eq!(w, g, "{:?}", xs),
            (Err(_), Err(_)) => {}
            _ => prop_assert!(false, "want {:?} got {:?} xs {:?}", want, got, xs),
        }
    }
}
