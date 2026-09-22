//! The bounded filter language a partial index may carry.
//!
//! # Why it is bounded rather than general
//!
//! A partial index holds only the documents matching its filter, so the
//! planner may use it **only for a query provably contained by that filter**.
//! Get that wrong and results silently lose documents — the same failure this
//! codebase already met with multikey, and the one it is least able to notice.
//!
//! General implication between filters is not decidable, so a general partial
//! filter forces a best-effort containment check whose mistakes are silent.
//! This module makes the problem small instead: the language is restricted to
//! shapes where containment is a **decision**, never a guess.
//!
//! ```text
//! allowed          {field: {$exists: true}}
//!                  {field: <literal>}                       equality
//!                  {field: {$gt|$gte|$lt|$lte: <literal>}}
//!                  any conjunction of the above
//!
//! refused          $or $ne $nin $regex $not $elemMatch $in
//!                  $exists: false
//! ```
//!
//! The refusal lands at **index creation**, where an operator is present to
//! read it and can choose a different index — rather than at query time, where
//! the only symptom would be a plan that quietly stopped applying.
//!
//! # Why `$exists: false` is refused
//!
//! It would be sound to index on, but nothing could ever *use* it: a query
//! implies "this field is absent" only by saying so, and a query that says so
//! is asking for the documents an ordinary index already answers with its null
//! entries. Allowing it would be a foot-gun with no upside.
//!
//! # Where this lives, and why
//!
//! In `kimmy-core`, beside [`crate::IndexMeta`], because both sides need it and
//! neither may depend on the other: `kimmy-storage` evaluates it on every write
//! to decide whether a document belongs in the index, and `kimmy-query` reasons
//! about containment to decide whether a plan may use it.

use std::cmp::Ordering;

use bson::{Bson, Document};

use crate::cmp::{canonical_cmp, holds_decimal128};
use crate::error::{Error, Result};
use crate::path;

/// What one predicate asserts about one path.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialOp {
    /// The field is present. `$exists: false` is deliberately not expressible.
    Exists,
    Eq(Bson),
    Gt(Bson),
    Gte(Bson),
    Lt(Bson),
    Lte(Bson),
}

impl PartialOp {
    /// Whether this holds on one value, as `find` tests one value: the
    /// predicate `any_element` applies to a value and to each element of an
    /// array value, **without** looking inside the value itself.
    ///
    /// What [`Self::implies`] reasons with. A document matches a query
    /// predicate through some value or element `w`, and that same `w` is
    /// among what the filter predicate is tested on -- but `w`'s own elements
    /// are not, when `w` is itself an element of an array, since `find`
    /// descends one level. So implication is judged on the value alone.
    fn holds_on_value(&self, value: &Bson) -> bool {
        use crate::cmp::same_type_group;
        let compares = |bound: &Bson, accept: &[Ordering]| {
            same_type_group(value, bound) && accept.contains(&canonical_cmp(value, bound))
        };
        match self {
            PartialOp::Exists => true,
            PartialOp::Eq(Bson::Null) => matches!(value, Bson::Null),
            PartialOp::Eq(want) => canonical_cmp(value, want) == Ordering::Equal,
            PartialOp::Gt(bound) => compares(bound, &[Ordering::Greater]),
            PartialOp::Gte(bound) => compares(bound, &[Ordering::Greater, Ordering::Equal]),
            PartialOp::Lt(bound) => compares(bound, &[Ordering::Less]),
            PartialOp::Lte(bound) => compares(bound, &[Ordering::Less, Ordering::Equal]),
        }
    }

    /// Whether the values a path resolves to satisfy this, as `find`
    /// evaluates the same operator: `values` is empty when the path is absent.
    pub fn selects(&self, values: &[&Bson]) -> bool {
        match self {
            PartialOp::Exists => crate::matching::exists(values, true),
            PartialOp::Eq(want) => crate::matching::equals(values, want),
            PartialOp::Gt(bound) => crate::matching::compares(values, bound, &[Ordering::Greater]),
            PartialOp::Gte(bound) => {
                crate::matching::compares(values, bound, &[Ordering::Greater, Ordering::Equal])
            }
            PartialOp::Lt(bound) => crate::matching::compares(values, bound, &[Ordering::Less]),
            PartialOp::Lte(bound) => {
                crate::matching::compares(values, bound, &[Ordering::Less, Ordering::Equal])
            }
        }
    }

    /// Whether satisfying `self` guarantees satisfying `other`, both read as
    /// `find` reads them (ADR-183).
    ///
    /// This is the whole containment question, and it is decidable precisely
    /// because the language is this small. A document satisfies `self`
    /// through some value or array element `w`; the question is whether
    /// every such `w` also satisfies `other`. Two things make that a
    /// question about `find` and not about the canonical order alone:
    ///
    /// - **A bound implies a bound only within one type bracket.** `find`
    ///   compares a value only with a bound of its own bracket, so `{$gt: 5}`
    ///   selects no string and no array. Judged on `canonical_cmp` alone, a
    ///   query `{k: [1, 2]}` or `{k: {$gt: "a"}}` was taken to imply
    ///   `{k: {$gt: 5}}`, because arrays and strings sort above numbers, and
    ///   the planner answered it from an index that holds neither.
    /// - **`{k: null}` matches a missing field**, so it implies nothing about
    ///   presence and no bound: only itself.
    ///
    /// **Unsound for a document value that is a `Decimal128`, knowingly.** This
    /// reasons through a single value `w`, which assumes equality is transitive.
    /// `canonical_cmp` ranks a `Decimal128` equal to every number — the settled
    /// contract in `docs/http-api.md` and `docs/key-encoding.md` — so `Eq(5)` and
    /// `Eq(6)` both hold on one while neither implies the other, and a query can
    /// be judged contained by a filter whose index does not hold the document.
    /// Filed as the finding whose slug ends `-because-equality-with-a-decimal128-is-not-transitive`
    /// (recorded in full in ADR-183); not fixed here, because a fix
    /// trades index use for soundness under a contract that has not been
    /// reopened. The soundness property's corpus therefore holds no
    /// `Decimal128`.
    pub fn implies(&self, other: &PartialOp) -> bool {
        use crate::cmp::same_type_group;
        let bracket = |a: &Bson, b: &Bson| same_type_group(a, b);
        match (self, other) {
            // A null equality is satisfied by absence, which satisfies only
            // another null equality.
            (PartialOp::Eq(Bson::Null), o) => matches!(o, PartialOp::Eq(Bson::Null)),
            // Anything else that holds at all implies presence.
            (_, PartialOp::Exists) => true,
            // A known value implies whatever that value satisfies.
            (PartialOp::Eq(v), o) => o.holds_on_value(v),
            (PartialOp::Exists, _) => false,

            // Lower bounds: the tighter one implies the looser, within one
            // bracket.
            (PartialOp::Gt(a), PartialOp::Gt(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Less
            }
            (PartialOp::Gt(a), PartialOp::Gte(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Less
            }
            (PartialOp::Gte(a), PartialOp::Gte(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Less
            }
            // `>= a` implies `> b` only when a is strictly past b, since a
            // itself satisfies the former and must also satisfy the latter.
            (PartialOp::Gte(a), PartialOp::Gt(b)) => {
                bracket(a, b) && canonical_cmp(a, b) == Ordering::Greater
            }

            // Upper bounds, mirrored.
            (PartialOp::Lt(a), PartialOp::Lt(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Greater
            }
            (PartialOp::Lt(a), PartialOp::Lte(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Greater
            }
            (PartialOp::Lte(a), PartialOp::Lte(b)) => {
                bracket(a, b) && canonical_cmp(a, b) != Ordering::Greater
            }
            (PartialOp::Lte(a), PartialOp::Lt(b)) => {
                bracket(a, b) && canonical_cmp(a, b) == Ordering::Less
            }

            // A bound in one direction says nothing about the other.
            _ => false,
        }
    }
}

/// A conjunction of predicates. Every one must hold.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartialFilter {
    predicates: Vec<(String, PartialOp)>,
}

impl PartialFilter {
    /// Parse a `partialFilterExpression`, refusing anything outside the
    /// language.
    pub fn parse(doc: &Document) -> Result<Self> {
        if doc.is_empty() {
            return Err(Error::InvalidQuery(
                "a partialFilterExpression cannot be empty; leave it off for an ordinary index"
                    .into(),
            ));
        }

        let mut predicates = Vec::with_capacity(doc.len());
        for (path, value) in doc {
            if path.starts_with('$') {
                return Err(Error::UnsupportedOperator(format!(
                    "{path} cannot appear in a partialFilterExpression: only a conjunction of \
                     per-field predicates is allowed, so containment stays decidable"
                )));
            }
            predicates.push((path.clone(), parse_op(path, value)?));
        }
        Ok(Self { predicates })
    }

    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    pub fn predicates(&self) -> impl Iterator<Item = (&str, &PartialOp)> {
        self.predicates.iter().map(|(p, o)| (p.as_str(), o))
    }

    /// Whether `find` with this filter as its expression would return `doc`
    /// -- which is also whether `doc` belongs in the index (ADR-183).
    ///
    /// Evaluated by [`crate::matching`], the same code `kimmy-query`'s filter
    /// evaluates these operators with, so the answer is `find`'s by
    /// construction rather than by a second implementation agreeing with it.
    /// Index maintenance asks it for membership, and TTL expiry asks it again
    /// before it deletes (ADR-181). There used to be a second rule, for
    /// membership, that compared across type brackets, never compared a whole
    /// array, and read a missing field as matching nothing; containment was
    /// judged against the one while `find` answered by the other.
    pub fn selects(&self, doc: &Document) -> bool {
        self.predicates.iter().all(|(field, op)| op.selects(&path::resolve(doc, field)))
    }

    /// Whether a query carrying `query` is provably contained by this filter.
    ///
    /// `query` maps a field to every predicate the query imposes on it, in this
    /// same language. Containment holds when **each** of this filter's
    /// predicates is implied by **some** predicate the query imposes on the
    /// same field. Anything unproven is a `false`, and the caller falls back to
    /// a scan rather than returning a subset.
    pub fn covered_by(&self, query: &[(String, PartialOp)]) -> bool {
        self.predicates.iter().all(|(field, needed)| {
            query.iter().any(|(qfield, qop)| qfield == field && qop.implies(needed))
        })
    }

    /// Back to the document form, for storage and for display.
    pub fn to_document(&self) -> Document {
        let mut out = Document::new();
        for (field, op) in &self.predicates {
            let value = match op {
                PartialOp::Exists => bson::doc! { "$exists": true }.into(),
                PartialOp::Eq(v) => v.clone(),
                PartialOp::Gt(v) => bson::doc! { "$gt": v.clone() }.into(),
                PartialOp::Gte(v) => bson::doc! { "$gte": v.clone() }.into(),
                PartialOp::Lt(v) => bson::doc! { "$lt": v.clone() }.into(),
                PartialOp::Lte(v) => bson::doc! { "$lte": v.clone() }.into(),
            };
            out.insert(field.clone(), value);
        }
        out
    }
}

fn parse_op(path: &str, value: &Bson) -> Result<PartialOp> {
    // A `Decimal128` cannot be a bound or an equality: `canonical_cmp` ranks
    // it equal to every other number, so a filter holding one would select
    // every numeric value and the index would hold documents its definition
    // never named. Checked over the whole value, operator and all, because
    // every shape below compares its operand.
    if holds_decimal128(value) {
        return Err(Error::InvalidQuery(format!(
            "partialFilterExpression for {path:?} holds a Decimal128, which cannot be compared: \
             it has no exact key encoding in this engine and ranks equal to every other number; \
             use a double or a long"
        )));
    }
    let Bson::Document(spec) = value else {
        // A bare value is equality, as it is everywhere else in the filter
        // language.
        return Ok(PartialOp::Eq(value.clone()));
    };

    // A document whose first key is not an operator is a literal sub-document
    // to compare against, not a nested predicate.
    let Some((first, _)) = spec.iter().next() else {
        return Ok(PartialOp::Eq(value.clone()));
    };
    if !first.starts_with('$') {
        return Ok(PartialOp::Eq(value.clone()));
    }

    if spec.len() > 1 {
        return Err(Error::InvalidQuery(format!(
            "partialFilterExpression for {path:?} takes one operator, found {}",
            spec.len()
        )));
    }
    let operand = spec.get(first).expect("key from the same document");

    Ok(match first.as_str() {
        "$exists" => match operand {
            Bson::Boolean(true) => PartialOp::Exists,
            Bson::Boolean(false) => {
                return Err(Error::UnsupportedOperator(format!(
                    "$exists: false cannot appear in a partialFilterExpression for {path:?}: no \
                     query could ever be proven to imply it, so the index would never be used"
                )));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$exists takes a boolean, found {other:?}"
                )));
            }
        },
        "$eq" => PartialOp::Eq(operand.clone()),
        "$gt" => PartialOp::Gt(operand.clone()),
        "$gte" => PartialOp::Gte(operand.clone()),
        "$lt" => PartialOp::Lt(operand.clone()),
        "$lte" => PartialOp::Lte(operand.clone()),
        other => {
            return Err(Error::UnsupportedOperator(format!(
                "{other} cannot appear in a partialFilterExpression for {path:?}. Allowed: \
                 $exists: true, $eq, $gt, $gte, $lt, $lte, and a bare value for equality — the \
                 language is deliberately small so a query's containment is decidable rather \
                 than guessed"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn parse(d: Document) -> Result<PartialFilter> {
        PartialFilter::parse(&d)
    }

    // -- the language boundary -------------------------------------------

    #[test]
    fn the_allowed_shapes_parse() {
        assert!(parse(doc! {"email": {"$exists": true}}).is_ok());
        assert!(parse(doc! {"status": "active"}).is_ok());
        assert!(parse(doc! {"status": {"$eq": "active"}}).is_ok());
        assert!(parse(doc! {"qty": {"$gt": 10}}).is_ok());
        assert!(parse(doc! {"qty": {"$gte": 10}}).is_ok());
        assert!(parse(doc! {"qty": {"$lt": 10}}).is_ok());
        assert!(parse(doc! {"qty": {"$lte": 10}}).is_ok());
        // A conjunction of them.
        assert!(parse(doc! {"status": "active", "qty": {"$gte": 1}}).is_ok());
    }

    #[test]
    fn everything_outside_the_language_is_refused_at_parse() {
        // Each of these would force a containment check that could only be a
        // best effort, and a wrong one loses documents silently.
        for bad in [
            doc! {"$or": [{"a": 1}, {"b": 2}]},
            doc! {"a": {"$ne": 1}},
            doc! {"a": {"$in": [1, 2]}},
            doc! {"a": {"$nin": [1]}},
            doc! {"a": {"$regex": "^x"}},
            doc! {"a": {"$not": {"$gt": 1}}},
            doc! {"a": {"$elemMatch": {"b": 1}}},
            doc! {"a": {"$exists": false}},
            doc! {"a": {"$gt": 1, "$lt": 5}},
        ] {
            assert!(parse(bad.clone()).is_err(), "should have been refused: {bad:?}");
        }
    }

    #[test]
    fn an_empty_expression_is_refused() {
        // Silently meaning "everything" would make an ordinary index look
        // partial in the metadata.
        assert!(parse(doc! {}).is_err());
    }

    #[test]
    fn a_literal_subdocument_is_equality_not_a_nested_predicate() {
        let f = parse(doc! {"addr": {"city": "London"}}).unwrap();
        assert!(f.selects(&doc! {"addr": {"city": "London"}}));
        assert!(!f.selects(&doc! {"addr": {"city": "Paris"}}));
    }

    // -- membership -------------------------------------------------------

    #[test]
    fn exists_selects_only_documents_carrying_the_field() {
        // The motivating case: unique only where the field is present.
        let f = parse(doc! {"email": {"$exists": true}}).unwrap();
        assert!(f.selects(&doc! {"_id": 1, "email": "a@b.c"}));
        assert!(!f.selects(&doc! {"_id": 2}));
        // Explicitly null is *present*, and Mongo agrees.
        assert!(f.selects(&doc! {"_id": 3, "email": Bson::Null}));
    }

    #[test]
    fn a_missing_field_satisfies_no_comparison() {
        // If absence were treated as null it would sort below everything and
        // pull every incomplete document into a `$lt` index.
        let f = parse(doc! {"qty": {"$lt": 5}}).unwrap();
        assert!(f.selects(&doc! {"qty": 1}));
        assert!(!f.selects(&doc! {"other": 1}));
    }

    #[test]
    fn a_conjunction_needs_every_predicate() {
        let f = parse(doc! {"status": "active", "qty": {"$gte": 10}}).unwrap();
        assert!(f.selects(&doc! {"status": "active", "qty": 10}));
        assert!(!f.selects(&doc! {"status": "active", "qty": 9}));
        assert!(!f.selects(&doc! {"status": "done", "qty": 10}));
    }

    #[test]
    fn an_array_matches_when_any_element_does() {
        let f = parse(doc! {"tags": "urgent"}).unwrap();
        assert!(f.selects(&doc! {"tags": ["slow", "urgent"]}));
        assert!(!f.selects(&doc! {"tags": ["slow"]}));
    }

    #[test]
    fn a_dotted_path_reaches_into_a_subdocument() {
        let f = parse(doc! {"user.active": true}).unwrap();
        assert!(f.selects(&doc! {"user": {"active": true}}));
        assert!(!f.selects(&doc! {"user": {"active": false}}));
        assert!(!f.selects(&doc! {"user": {}}));
    }

    #[test]
    fn membership_equates_numbers_across_types_as_find_does() {
        let f = parse(doc! {"n": 5}).unwrap();
        assert!(f.selects(&doc! {"n": 5.0}), "5 and 5.0 share an index entry");
    }

    // Where membership used to differ from `find`, and no longer can
    // (ADR-183): the one rule is `find`'s.

    #[test]
    fn a_whole_array_equal_to_the_operand_is_a_member() {
        let f = parse(doc! {"k": [1, 2]}).unwrap();
        assert!(f.selects(&doc! {"k": [1, 2]}), "the whole array");
        assert!(f.selects(&doc! {"k": [[1, 2]]}), "an element equal to it");
        assert!(!f.selects(&doc! {"k": [2, 1]}));
    }

    #[test]
    fn an_empty_array_is_present() {
        let f = parse(doc! {"k": {"$exists": true}}).unwrap();
        assert!(f.selects(&doc! {"k": []}));
    }

    #[test]
    fn a_bound_selects_only_its_own_type_bracket() {
        let f = parse(doc! {"size": {"$gt": 5}}).unwrap();
        assert!(f.selects(&doc! {"size": 10}));
        for other in [Bson::String("large".into()), doc! {"w": 1}.into(), true.into(), Bson::Null] {
            assert!(!f.selects(&doc! {"size": other.clone()}), "{other:?} is not a number");
        }
    }

    #[test]
    fn a_null_equality_selects_a_missing_field() {
        let f = parse(doc! {"deleted": null}).unwrap();
        assert!(f.selects(&doc! {"deleted": null}));
        assert!(f.selects(&doc! {"other": 1}), "absent reads as null, as in find");
    }

    // -- containment ------------------------------------------------------

    fn q(field: &str, op: PartialOp) -> Vec<(String, PartialOp)> {
        vec![(field.to_string(), op)]
    }

    #[test]
    fn an_equality_query_covers_an_exists_index() {
        // The everyday case: a sparse-style index answering a normal lookup.
        let f = parse(doc! {"email": {"$exists": true}}).unwrap();
        assert!(f.covered_by(&q("email", PartialOp::Eq("a@b.c".into()))));
        assert!(f.covered_by(&q("email", PartialOp::Gt(Bson::Int32(0)))));
    }

    #[test]
    fn a_query_on_another_field_covers_nothing() {
        let f = parse(doc! {"email": {"$exists": true}}).unwrap();
        assert!(!f.covered_by(&q("name", PartialOp::Eq("ada".into()))));
        assert!(!f.covered_by(&[]));
    }

    #[test]
    fn a_tighter_bound_covers_a_looser_one() {
        let f = parse(doc! {"qty": {"$gte": 10}}).unwrap();
        assert!(f.covered_by(&q("qty", PartialOp::Gte(Bson::Int32(50)))));
        assert!(f.covered_by(&q("qty", PartialOp::Gt(Bson::Int32(10)))));
        assert!(f.covered_by(&q("qty", PartialOp::Eq(Bson::Int32(10)))));
        // Looser, or the wrong direction: not proven, so not used.
        assert!(!f.covered_by(&q("qty", PartialOp::Gte(Bson::Int32(5)))));
        assert!(!f.covered_by(&q("qty", PartialOp::Gt(Bson::Int32(5)))));
        assert!(!f.covered_by(&q("qty", PartialOp::Lt(Bson::Int32(50)))));
        assert!(!f.covered_by(&q("qty", PartialOp::Eq(Bson::Int32(9)))));
    }

    #[test]
    fn the_strictness_of_a_bound_is_respected_at_its_edge() {
        // `>= 10` does not imply `> 10`: the document holding exactly 10
        // satisfies the query and would be missing from the index.
        let strict = parse(doc! {"qty": {"$gt": 10}}).unwrap();
        assert!(!strict.covered_by(&q("qty", PartialOp::Gte(Bson::Int32(10)))));
        assert!(strict.covered_by(&q("qty", PartialOp::Gt(Bson::Int32(10)))));
        assert!(strict.covered_by(&q("qty", PartialOp::Gte(Bson::Int32(11)))));

        let loose = parse(doc! {"qty": {"$gte": 10}}).unwrap();
        assert!(loose.covered_by(&q("qty", PartialOp::Gt(Bson::Int32(10)))));
        assert!(loose.covered_by(&q("qty", PartialOp::Gte(Bson::Int32(10)))));
    }

    #[test]
    fn upper_bounds_mirror_lower_ones() {
        let f = parse(doc! {"qty": {"$lte": 10}}).unwrap();
        assert!(f.covered_by(&q("qty", PartialOp::Lte(Bson::Int32(5)))));
        assert!(f.covered_by(&q("qty", PartialOp::Lt(Bson::Int32(10)))));
        assert!(!f.covered_by(&q("qty", PartialOp::Lte(Bson::Int32(50)))));

        let strict = parse(doc! {"qty": {"$lt": 10}}).unwrap();
        assert!(!strict.covered_by(&q("qty", PartialOp::Lte(Bson::Int32(10)))));
        assert!(strict.covered_by(&q("qty", PartialOp::Lt(Bson::Int32(10)))));
    }

    #[test]
    fn every_predicate_of_a_conjunction_must_be_covered() {
        let f = parse(doc! {"status": "active", "qty": {"$gte": 10}}).unwrap();
        let both = vec![
            ("status".to_string(), PartialOp::Eq("active".into())),
            ("qty".to_string(), PartialOp::Gte(Bson::Int32(20))),
        ];
        assert!(f.covered_by(&both));
        // Only one of the two: not proven.
        assert!(!f.covered_by(&q("status", PartialOp::Eq("active".into()))));
    }

    #[test]
    fn an_equality_on_the_wrong_value_does_not_cover() {
        let f = parse(doc! {"status": "active"}).unwrap();
        assert!(f.covered_by(&q("status", PartialOp::Eq("active".into()))));
        assert!(!f.covered_by(&q("status", PartialOp::Eq("done".into()))));
    }

    /// Values of every type bracket a filter can compare, as a field holds
    /// them: scalars, arrays whole and nested, documents, and null.
    fn corpus() -> Vec<Bson> {
        vec![
            Bson::Null,
            Bson::Int32(5),
            Bson::Int32(6),
            Bson::Int64(5),
            Bson::Double(7.5),
            Bson::Int32(-1),
            "x".into(),
            "".into(),
            "a".into(),
            Bson::Array(vec![]),
            Bson::Array(vec![1.into(), 2.into()]),
            Bson::Array(vec![Bson::Array(vec![1.into(), 2.into()])]),
            Bson::Array(vec![5.into()]),
            Bson::Array(vec![1.into(), "x".into()]),
            Bson::Array(vec![Bson::Null]),
            doc! {"a": 1}.into(),
            Bson::Array(vec![doc! {"a": 1}.into()]),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![1, 2],
            }),
            true.into(),
            Bson::DateTime(bson::DateTime::from_millis(1_000)),
        ]
    }

    fn ops() -> Vec<PartialOp> {
        let mut ops = vec![PartialOp::Exists];
        for v in corpus() {
            ops.push(PartialOp::Eq(v.clone()));
            ops.push(PartialOp::Gt(v.clone()));
            ops.push(PartialOp::Gte(v.clone()));
            ops.push(PartialOp::Lt(v.clone()));
            ops.push(PartialOp::Lte(v));
        }
        ops
    }

    #[test]
    fn implication_is_sound_for_what_find_selects() {
        // The property the whole thing rests on: if a query predicate implies
        // the index predicate, every document the query matches is one the
        // index holds. Over every type bracket, because the defect it guards
        // against lived between brackets: an all-integer corpus could never
        // have failed.
        let mut docs = vec![doc! {}];
        docs.extend(corpus().into_iter().map(|v| doc! {"k": v}));
        let mut implied = 0usize;
        for qop in ops() {
            for iop in ops() {
                if !qop.implies(&iop) {
                    continue;
                }
                implied += 1;
                for d in &docs {
                    let values = path::resolve(d, "k");
                    assert!(
                        !qop.selects(&values) || iop.selects(&values),
                        "{qop:?} claims to imply {iop:?}, but {d:?} matches the query and \
                         is not in the index -- this is the silent document loss"
                    );
                }
            }
        }
        assert!(implied > 600, "premise: the corpus exercises implication (692 pairs): {implied}");
    }

    // -- round-tripping ---------------------------------------------------

    #[test]
    fn a_filter_round_trips_through_its_document_form() {
        for original in [
            doc! {"email": {"$exists": true}},
            doc! {"status": "active"},
            doc! {"qty": {"$gte": 10}},
            doc! {"status": "active", "qty": {"$lt": 100}},
        ] {
            let parsed = PartialFilter::parse(&original).unwrap();
            let reparsed = PartialFilter::parse(&parsed.to_document()).unwrap();
            assert_eq!(parsed, reparsed, "lost fidelity: {original:?}");
        }
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    #[test]
    fn a_decimal128_cannot_be_a_bound_or_an_equality() {
        // A partial filter compares on every write; a Decimal128 operand
        // would select every numeric value. Refused at index creation, where
        // an operator is there to read it, whatever shape carries it.
        let d = Bson::Decimal128("1.5".parse().unwrap());
        for filter in [
            doc! { "v": d.clone() },
            doc! { "v": { "$gt": d.clone() } },
            doc! { "v": { "$eq": { "n": d.clone() } } },
            doc! { "ok": { "$exists": true }, "v": { "$lte": d.clone() } },
        ] {
            let Err(err) = PartialFilter::parse(&filter) else {
                panic!("{filter} should be refused")
            };
            let msg = err.to_string();
            assert!(msg.contains("Decimal128") && msg.contains("\"v\""), "{filter}: {msg}");
        }
        assert!(PartialFilter::parse(&doc! { "v": { "$gt": 1.5 } }).is_ok());
    }
}
