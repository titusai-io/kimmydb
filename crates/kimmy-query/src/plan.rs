//! Rule-based index selection.
//!
//! No cost model. Extract the equality and range predicates the filter imposes,
//! find the index whose leading fields those predicates cover best, and turn
//! that into a key range. If nothing covers anything, fall back to a collection
//! scan.
//!
//! # The safety rule
//!
//! An index answers **"which documents might match"**. Only the filter decides
//! membership, so the caller must re-apply the full filter to every candidate.
//!
//! It follows that a computed range may be **too wide but never too narrow**. A
//! wide range costs time; a narrow one silently drops matching documents. Every
//! decision below resolves that way — unusable operators are ignored rather
//! than guessed at, and anything uncertain widens the range.

use std::collections::HashMap;

use bson::Bson;
use kimmy_core::{IndexMeta, keyenc};

use crate::filter::{Condition, Filter};

/// A chosen access path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexPlan {
    pub index_id: u32,
    pub index_name: String,
    /// Inclusive lower bound on the encoded index key.
    pub lower: Vec<u8>,
    /// Inclusive upper bound on the encoded index key.
    pub upper: Vec<u8>,
    /// How many index fields the filter constrained. Reported by `explain`.
    pub fields_used: usize,
}

/// Greater than the first byte of any encoded component.
///
/// Type tags occupy `0x01..=0xF0`, and a descending component inverts them into
/// `0x0F..=0xFE`, so `0xFF` exceeds every possible continuation. Appending it
/// makes an upper bound cover every key sharing the prefix, including those
/// with further components after it.
const ABOVE_ANY_CONTINUATION: u8 = 0xFF;

/// What the filter constrains one field to.
#[derive(Default, Clone, Debug)]
struct Bounds {
    eq: Option<Bson>,
    lower: Option<Bson>,
    upper: Option<Bson>,
}

/// Pick an index for this filter, or `None` to scan.
pub fn choose(filter: &Filter, indexes: &[IndexMeta]) -> Option<IndexPlan> {
    if indexes.is_empty() {
        return None;
    }
    let predicates = extract(filter);
    if predicates.is_empty() {
        return None;
    }

    let mut best: Option<IndexPlan> = None;
    for index in indexes {
        if let Some(plan) = plan_for(index, &predicates)
            && best.as_ref().is_none_or(|b| plan.fields_used > b.fields_used)
        {
            best = Some(plan);
        }
    }
    best
}

/// Collect the predicates that must hold for *every* matching document.
///
/// Only conjunctions contribute. A disjunction's branches need not all hold, so
/// narrowing on one of them would drop documents the other branch matches —
/// they are skipped entirely rather than approximated.
fn extract(filter: &Filter) -> HashMap<String, Bounds> {
    let mut out = HashMap::new();
    collect(filter, &mut out);
    out
}

fn collect(filter: &Filter, out: &mut HashMap<String, Bounds>) {
    match filter {
        Filter::And(branches) => {
            for branch in branches {
                collect(branch, out);
            }
        }
        Filter::Field { path, conditions } => {
            let slot = out.entry(path.clone()).or_default();
            for condition in conditions {
                match condition {
                    // `$eq: null` is usable: a missing field is indexed as
                    // null, so its entry exists and will be found.
                    Condition::Eq(v) => slot.eq.get_or_insert(v.clone()),
                    Condition::Gt(v) | Condition::Gte(v) => slot.lower.get_or_insert(v.clone()),
                    Condition::Lt(v) | Condition::Lte(v) => slot.upper.get_or_insert(v.clone()),
                    // Everything else — $ne, $in, $nin, $not, $exists, $regex,
                    // $size, $all, $elemMatch — either cannot narrow a range or
                    // needs care to do so safely. Ignoring them only costs
                    // selectivity, since the filter is re-applied anyway.
                    _ => continue,
                };
            }
        }
        // A disjunction constrains nothing that must universally hold.
        Filter::Or(_) | Filter::Nor(_) | Filter::AlwaysTrue => {}
    }
}

/// Build a plan for one index, if the predicates cover its leading fields.
fn plan_for(index: &IndexMeta, predicates: &HashMap<String, Bounds>) -> Option<IndexPlan> {
    // How many leading fields have an equality predicate.
    let mut prefix: Vec<(Bson, bool)> = Vec::new();
    for field in &index.fields {
        match predicates.get(&field.path).and_then(|b| b.eq.clone()) {
            Some(value) => prefix.push((value, field.descending)),
            None => break,
        }
    }

    // The field just past the equality prefix may carry a range.
    let mut lower = prefix.clone();
    let mut upper = prefix.clone();
    let mut used = prefix.len();

    if let Some(field) = index.fields.get(prefix.len())
        && let Some(bounds) = predicates.get(&field.path)
        && (bounds.lower.is_some() || bounds.upper.is_some())
    {
        // A descending field inverts the encoding, which swaps which end of the
        // range each bound belongs to. Rather than risk getting that backwards
        // — the failure mode is a range that is too *narrow* — the range is
        // dropped and only the equality prefix is used. Wider, always correct.
        if !field.descending {
            if let Some(v) = &bounds.lower {
                lower.push((v.clone(), false));
            }
            if let Some(v) = &bounds.upper {
                upper.push((v.clone(), false));
            }
            used += 1;
        }
    }

    if used == 0 {
        return None;
    }

    let mut lower_bytes = keyenc::encode_compound_ordered(&lower).ok()?;
    let mut upper_bytes = keyenc::encode_compound_ordered(&upper).ok()?;
    // An unbounded lower end starts at the prefix itself; an unbounded upper
    // end must reach past every continuation of it.
    if lower_bytes.is_empty() {
        lower_bytes = Vec::new();
    }
    upper_bytes.push(ABOVE_ANY_CONTINUATION);

    Some(IndexPlan {
        index_id: index.id,
        index_name: index.name.clone(),
        lower: lower_bytes,
        upper: upper_bytes,
        fields_used: used,
    })
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::IndexField;

    use super::*;

    fn index(id: u32, fields: Vec<IndexField>) -> IndexMeta {
        IndexMeta {
            id,
            name: IndexMeta::default_name(&fields),
            fields,
            unique: false,
            enforcement: Default::default(),
        }
    }

    fn plan(query: bson::Document, indexes: &[IndexMeta]) -> Option<IndexPlan> {
        choose(&crate::filter::parse(&query).unwrap(), indexes)
    }

    #[test]
    fn an_equality_predicate_selects_a_matching_index() {
        let idx = [index(0, vec![IndexField::ascending("qty")])];
        let p = plan(doc! { "qty": 5 }, &idx).expect("should use the index");
        assert_eq!(p.index_id, 0);
        assert_eq!(p.fields_used, 1);
    }

    #[test]
    fn no_index_is_chosen_when_nothing_matches() {
        let idx = [index(0, vec![IndexField::ascending("qty")])];
        assert!(plan(doc! { "other": 5 }, &idx).is_none());
        assert!(plan(doc! {}, &idx).is_none());
        assert!(plan(doc! { "qty": 5 }, &[]).is_none());
    }

    #[test]
    fn the_index_covering_most_fields_wins() {
        let idx = [
            index(0, vec![IndexField::ascending("a")]),
            index(1, vec![IndexField::ascending("a"), IndexField::ascending("b")]),
        ];
        let p = plan(doc! { "a": 1, "b": 2 }, &idx).unwrap();
        assert_eq!(p.index_id, 1);
        assert_eq!(p.fields_used, 2);
    }

    #[test]
    fn a_compound_index_needs_its_leading_field() {
        // Constraining only the second field cannot narrow the key range.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::ascending("b")])];
        assert!(plan(doc! { "b": 2 }, &idx).is_none());
        assert!(plan(doc! { "a": 1 }, &idx).is_some());
    }

    #[test]
    fn a_range_on_the_field_after_the_prefix_is_used() {
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::ascending("n")])];
        let p = plan(doc! { "a": 1, "n": { "$gt": 5, "$lt": 10 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 2);
        assert!(p.lower < p.upper);
    }

    #[test]
    fn a_bare_range_uses_the_leading_field() {
        let idx = [index(0, vec![IndexField::ascending("n")])];
        let p = plan(doc! { "n": { "$gte": 5 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 1);
    }

    #[test]
    fn a_disjunction_alone_selects_no_index() {
        // Narrowing on one branch would drop documents the other branch
        // matches.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        assert!(plan(doc! { "$or": [ { "a": 1 }, { "b": 2 } ] }, &idx).is_none());
    }

    #[test]
    fn a_conjunction_containing_a_disjunction_still_uses_the_conjunct() {
        // `a == 1` must hold for every match, so narrowing on it is safe.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let p = plan(doc! { "a": 1, "$or": [ { "b": 1 }, { "c": 2 } ] }, &idx).unwrap();
        assert_eq!(p.fields_used, 1);
    }

    #[test]
    fn operators_that_cannot_narrow_are_ignored() {
        let idx = [index(0, vec![IndexField::ascending("a")])];
        for query in [
            doc! { "a": { "$ne": 1 } },
            doc! { "a": { "$exists": true } },
            doc! { "a": { "$regex": "^x" } },
            doc! { "a": { "$in": [1, 2] } },
            doc! { "a": { "$size": 2 } },
        ] {
            assert!(plan(query.clone(), &idx).is_none(), "{query:?} must not narrow");
        }
    }

    #[test]
    fn a_descending_range_falls_back_to_the_equality_prefix() {
        // Getting the bound swap backwards would produce a range that is too
        // narrow, so the range is dropped rather than risked.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::descending("n")])];
        let p = plan(doc! { "a": 1, "n": { "$gt": 5 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 1, "only the equality prefix should be used");
    }

    #[test]
    fn a_descending_equality_field_is_still_usable() {
        let idx = [index(0, vec![IndexField::descending("a")])];
        assert_eq!(plan(doc! { "a": 1 }, &idx).unwrap().fields_used, 1);
    }

    #[test]
    fn eq_null_is_usable_because_missing_fields_index_as_null() {
        let idx = [index(0, vec![IndexField::ascending("a")])];
        assert!(plan(doc! { "a": Bson::Null }, &idx).is_some());
    }

    #[test]
    fn the_upper_bound_reaches_past_longer_keys() {
        // A one-field equality on a two-field index must still find documents
        // whose key carries a second component.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::ascending("b")])];
        let p = plan(doc! { "a": 1 }, &idx).unwrap();

        let longer = keyenc::encode_compound_ordered(&[
            (Bson::Int32(1), false),
            (Bson::String("zzz".into()), false),
        ])
        .unwrap();
        assert!(longer >= p.lower && longer <= p.upper, "a longer key must fall in range");
    }

    #[test]
    fn equal_values_across_numeric_types_produce_the_same_bounds() {
        let idx = [index(0, vec![IndexField::ascending("n")])];
        let a = plan(doc! { "n": 5i32 }, &idx).unwrap();
        let b = plan(doc! { "n": 5.0 }, &idx).unwrap();
        assert_eq!(a.lower, b.lower);
        assert_eq!(a.upper, b.upper);
    }
}
