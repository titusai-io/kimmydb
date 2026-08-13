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

use crate::cmp::canonical_cmp;
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
    /// Whether a document's value at the path satisfies this.
    ///
    /// `value` is `None` for an absent field. Absence satisfies nothing here —
    /// not even a comparison — because a missing field would otherwise be
    /// indexed as null and compare below everything, quietly pulling every
    /// incomplete document into a `$lt` index.
    fn holds(&self, value: Option<&Bson>) -> bool {
        let Some(value) = value else {
            return false;
        };
        match self {
            PartialOp::Exists => true,
            PartialOp::Eq(want) => canonical_cmp(value, want) == Ordering::Equal,
            PartialOp::Gt(bound) => canonical_cmp(value, bound) == Ordering::Greater,
            PartialOp::Gte(bound) => canonical_cmp(value, bound) != Ordering::Less,
            PartialOp::Lt(bound) => canonical_cmp(value, bound) == Ordering::Less,
            PartialOp::Lte(bound) => canonical_cmp(value, bound) != Ordering::Greater,
        }
    }

    /// Whether satisfying `self` guarantees satisfying `other`.
    ///
    /// This is the whole containment question, and it is decidable precisely
    /// because the language is this small. Every arm is a fact about the total
    /// order `canonical_cmp` defines, not a heuristic.
    pub fn implies(&self, other: &PartialOp) -> bool {
        match (self, other) {
            // Anything that can hold at all implies presence.
            (_, PartialOp::Exists) => true,
            // A known value implies whatever that value satisfies.
            (PartialOp::Eq(v), o) => o.holds(Some(v)),
            (PartialOp::Exists, _) => false,

            // Lower bounds: the tighter one implies the looser.
            (PartialOp::Gt(a), PartialOp::Gt(b)) => canonical_cmp(a, b) != Ordering::Less,
            (PartialOp::Gt(a), PartialOp::Gte(b)) => canonical_cmp(a, b) != Ordering::Less,
            (PartialOp::Gte(a), PartialOp::Gte(b)) => canonical_cmp(a, b) != Ordering::Less,
            // `>= a` implies `> b` only when a is strictly past b, since a
            // itself satisfies the former and must also satisfy the latter.
            (PartialOp::Gte(a), PartialOp::Gt(b)) => canonical_cmp(a, b) == Ordering::Greater,

            // Upper bounds, mirrored.
            (PartialOp::Lt(a), PartialOp::Lt(b)) => canonical_cmp(a, b) != Ordering::Greater,
            (PartialOp::Lt(a), PartialOp::Lte(b)) => canonical_cmp(a, b) != Ordering::Greater,
            (PartialOp::Lte(a), PartialOp::Lte(b)) => canonical_cmp(a, b) != Ordering::Greater,
            (PartialOp::Lte(a), PartialOp::Lt(b)) => canonical_cmp(a, b) == Ordering::Less,

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

    /// Whether this document belongs in the index.
    ///
    /// A path that fans out through an array satisfies the predicate if **any**
    /// of its values does, matching how the filter layer treats arrays — an
    /// index over `tags` with a partial filter on `tags` must hold a document
    /// whose array contains a qualifying element.
    pub fn matches(&self, doc: &Document) -> bool {
        self.predicates.iter().all(|(field, op)| {
            let resolved = path::resolve(doc, field);
            if resolved.is_empty() {
                return op.holds(None);
            }
            resolved.iter().any(|value| match value {
                Bson::Array(items) => items.iter().any(|item| op.holds(Some(item))),
                other => op.holds(Some(other)),
            })
        })
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
        assert!(f.matches(&doc! {"addr": {"city": "London"}}));
        assert!(!f.matches(&doc! {"addr": {"city": "Paris"}}));
    }

    // -- membership -------------------------------------------------------

    #[test]
    fn exists_selects_only_documents_carrying_the_field() {
        // The motivating case: unique only where the field is present.
        let f = parse(doc! {"email": {"$exists": true}}).unwrap();
        assert!(f.matches(&doc! {"_id": 1, "email": "a@b.c"}));
        assert!(!f.matches(&doc! {"_id": 2}));
        // Explicitly null is *present*, and Mongo agrees.
        assert!(f.matches(&doc! {"_id": 3, "email": Bson::Null}));
    }

    #[test]
    fn a_missing_field_satisfies_no_comparison() {
        // If absence were treated as null it would sort below everything and
        // pull every incomplete document into a `$lt` index.
        let f = parse(doc! {"qty": {"$lt": 5}}).unwrap();
        assert!(f.matches(&doc! {"qty": 1}));
        assert!(!f.matches(&doc! {"other": 1}));
    }

    #[test]
    fn a_conjunction_needs_every_predicate() {
        let f = parse(doc! {"status": "active", "qty": {"$gte": 10}}).unwrap();
        assert!(f.matches(&doc! {"status": "active", "qty": 10}));
        assert!(!f.matches(&doc! {"status": "active", "qty": 9}));
        assert!(!f.matches(&doc! {"status": "done", "qty": 10}));
    }

    #[test]
    fn an_array_matches_when_any_element_does() {
        let f = parse(doc! {"tags": "urgent"}).unwrap();
        assert!(f.matches(&doc! {"tags": ["slow", "urgent"]}));
        assert!(!f.matches(&doc! {"tags": ["slow"]}));
    }

    #[test]
    fn a_dotted_path_reaches_into_a_subdocument() {
        let f = parse(doc! {"user.active": true}).unwrap();
        assert!(f.matches(&doc! {"user": {"active": true}}));
        assert!(!f.matches(&doc! {"user": {"active": false}}));
        assert!(!f.matches(&doc! {"user": {}}));
    }

    #[test]
    fn membership_compares_across_types_the_way_indexes_do() {
        let f = parse(doc! {"n": 5}).unwrap();
        assert!(f.matches(&doc! {"n": 5.0}), "5 and 5.0 share an index entry");
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

    #[test]
    fn implication_agrees_with_membership() {
        // The property the whole thing rests on: if a query predicate implies
        // the index predicate, then every document the query can match is one
        // the index actually holds.
        let values: Vec<Bson> = (0..20).map(Bson::Int32).collect();
        let index_ops = [
            PartialOp::Gt(Bson::Int32(10)),
            PartialOp::Gte(Bson::Int32(10)),
            PartialOp::Lt(Bson::Int32(10)),
            PartialOp::Lte(Bson::Int32(10)),
            PartialOp::Exists,
        ];
        let query_ops = [
            PartialOp::Gt(Bson::Int32(9)),
            PartialOp::Gt(Bson::Int32(10)),
            PartialOp::Gte(Bson::Int32(10)),
            PartialOp::Gte(Bson::Int32(11)),
            PartialOp::Lt(Bson::Int32(10)),
            PartialOp::Lte(Bson::Int32(10)),
            PartialOp::Eq(Bson::Int32(10)),
        ];

        for iop in &index_ops {
            for qop in &query_ops {
                if !qop.implies(iop) {
                    continue;
                }
                for v in &values {
                    if qop.holds(Some(v)) {
                        assert!(
                            iop.holds(Some(v)),
                            "{qop:?} claims to imply {iop:?}, but {v:?} matches the query and \
                             is not in the index — this is the silent document loss"
                        );
                    }
                }
            }
        }
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
