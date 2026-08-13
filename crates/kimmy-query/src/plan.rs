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
    /// Inclusive `(lower, upper)` bounds on the encoded index key.
    ///
    /// One entry for a contiguous range — an equality prefix, with or without
    /// a range on the next field. Several for `$in`, which becomes a **union
    /// of equality probes**: one entry per distinct value, in key order. The
    /// executor scans each and unions the candidates, deduplicating by
    /// document key — one document can appear under several probes when an
    /// array holds two of the listed values.
    pub ranges: Vec<(Vec<u8>, Vec<u8>)>,
    /// How many index fields the filter constrained. Reported by `explain`.
    pub fields_used: usize,
    /// Whether both ends of a range were intersected into the bounds.
    ///
    /// Intersecting is only sound while no document contributes several keys —
    /// which the planner concluded from a metadata read that is stale by the
    /// time anything scans. A plan carrying `true` must go through
    /// `index_candidates_unless_multikey`, which re-checks the flag in the
    /// same snapshot as the scan, rather than the plain candidate scan.
    ///
    /// Always `false` for a `$in` union: equality probes select exact keys,
    /// which is sound on a multikey index — nothing is intersected.
    pub both_bounds: bool,
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
    /// The values of a `$in`, when one is present.
    in_values: Option<Vec<Bson>>,
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
                    Condition::Eq(v) => {
                        slot.eq.get_or_insert(v.clone());
                    }
                    Condition::Gt(v) | Condition::Gte(v) => {
                        slot.lower.get_or_insert(v.clone());
                    }
                    Condition::Lt(v) | Condition::Lte(v) => {
                        slot.upper.get_or_insert(v.clone());
                    }
                    // `$in` is a disjunction of equalities on one field, so —
                    // unlike `$or` — every match still satisfies "the field is
                    // one of these", which a union of point probes can answer.
                    Condition::In(values) => {
                        slot.in_values.get_or_insert(values.clone());
                    }
                    // Everything else — $ne, $nin, $not, $exists, $regex,
                    // $size, $all, $elemMatch — either cannot narrow a range or
                    // needs care to do so safely. Ignoring them only costs
                    // selectivity, since the filter is re-applied anyway.
                    _ => {}
                }
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

    // The field just past the equality prefix may carry a `$in` or a range.
    //
    // `$in` is checked first: a set of equality probes is tighter than a range
    // when both constrain the same field, and — being equalities — the probes
    // are sound on a multikey index, which a two-sided range is not.
    if let Some(field) = index.fields.get(prefix.len())
        && let Some(bounds) = predicates.get(&field.path)
        && let Some(values) = &bounds.in_values
    {
        // One probe per distinct value. Deduplicated on the *encoded* key, so
        // `[5, 5.0]` — one value in two numeric spellings — probes once, the
        // same way the index itself collapses them.
        let mut seen = std::collections::BTreeSet::new();
        let mut ranges = Vec::new();
        for value in values {
            let mut components = prefix.clone();
            components.push((value.clone(), field.descending));
            let encoded = keyenc::encode_compound_ordered(&components).ok()?;
            if seen.insert(encoded.clone()) {
                let mut upper = encoded.clone();
                upper.push(ABOVE_ANY_CONTINUATION);
                ranges.push((encoded, upper));
            }
        }
        // Key order, so the union scans the index monotonically. An empty
        // `$in` yields an empty union — zero probes, zero candidates — which
        // is the right answer to a filter nothing can match.
        ranges.sort();
        return Some(IndexPlan {
            index_id: index.id,
            index_name: index.name.clone(),
            ranges,
            fields_used: prefix.len() + 1,
            both_bounds: false,
        });
    }

    let mut lower = prefix.clone();
    let mut upper = prefix.clone();
    let mut used = prefix.len();
    let mut both_bounds = false;

    if let Some(field) = index.fields.get(prefix.len())
        && let Some(bounds) = predicates.get(&field.path)
        && (bounds.lower.is_some() || bounds.upper.is_some())
    {
        // Whether BOTH ends of the range may be used hangs on one fact
        // about the data, not the query: does any document contribute more
        // than one key?
        //
        // A field can hold an array — a *multikey* index — and Mongo
        // semantics let **different elements** satisfy each end of a range.
        // `{a: [2, 0]}` matches `{$gte: 1, $lte: 1}` because 2 satisfies
        // the lower bound and 0 the upper, even though neither satisfies
        // both. Intersecting the bounds into a single key range would
        // exclude that document entirely — a range that is too narrow, and
        // therefore silently wrong.
        //
        // So: a multikey index uses one bound, keeping the range a superset
        // the recheck trims. An index that has never seen an array — the
        // write path tracks this, see `IndexMeta::multikey` — has exactly
        // one key per document per field, and a scalar satisfies both
        // bounds iff that key lies in the intersection. Both bounds, and
        // the scan stops where the range does.
        //
        // A descending field inverts its encoded bytes, which reverses
        // order: the value-space lower bound caps the key-space *top*, and
        // the upper bound its bottom. Getting this swap backwards produces
        // a range that is too narrow — which is why, until it had its own
        // property test, the range was dropped here rather than risked.
        let desc = field.descending;
        match (&bounds.lower, &bounds.upper) {
            (Some(lo), Some(hi)) if !index.multikey => {
                let (key_low, key_high) = if desc { (hi, lo) } else { (lo, hi) };
                lower.push((key_low.clone(), desc));
                upper.push((key_high.clone(), desc));
                both_bounds = true;
            }
            // One usable end — or a multikey index, where only one may be
            // used. The value-space lower bound is preferred, landing on
            // whichever key-space end the encoding sends it to.
            (Some(v), _) => match desc {
                true => upper.push((v.clone(), true)),
                false => lower.push((v.clone(), false)),
            },
            (None, Some(v)) => match desc {
                true => lower.push((v.clone(), true)),
                false => upper.push((v.clone(), false)),
            },
            (None, None) => unreachable!("checked above"),
        }
        used += 1;
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
        ranges: vec![(lower_bytes, upper_bytes)],
        fields_used: used,
        both_bounds,
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
            multikey: false,
            expire_after_secs: None,
        }
    }

    /// The same index, but one that has seen an array.
    fn multikey(id: u32, fields: Vec<IndexField>) -> IndexMeta {
        IndexMeta { multikey: true, ..index(id, fields) }
    }

    /// The single contiguous range of a non-union plan.
    fn bounds(p: &IndexPlan) -> (&Vec<u8>, &Vec<u8>) {
        assert_eq!(p.ranges.len(), 1, "expected one contiguous range, got {:?}", p.ranges);
        (&p.ranges[0].0, &p.ranges[0].1)
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
    fn a_tie_between_indexes_goes_to_the_first_listed() {
        // Found by mutation testing: `>` → `>=` in `choose` survived the
        // suite. Either winner answers the query correctly, which is why
        // nothing caught it — but the choice decides what `explain` names and
        // must not depend on which comparison operator someone typed. First
        // listed wins, and index order in metadata is stable.
        let idx = [
            index(7, vec![IndexField::ascending("a")]),
            index(9, vec![IndexField::ascending("a")]),
        ];
        let p = plan(doc! { "a": 1 }, &idx).unwrap();
        assert_eq!(p.index_id, 7, "a tie must go to the first listed index");
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
        let (lower, upper) = bounds(&p);
        assert!(lower < upper);
    }

    #[test]
    fn a_bare_range_uses_the_leading_field() {
        let idx = [index(0, vec![IndexField::ascending("n")])];
        let p = plan(doc! { "n": { "$gte": 5 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 1);
    }

    #[test]
    fn a_multikey_index_uses_only_one_end_of_a_range() {
        // Different elements of an array may satisfy each bound — `{a: [2, 0]}`
        // matches `{$gte: 1, $lte: 1}`. Intersecting both bounds would exclude
        // it, so once an index has seen an array, only the lower bound narrows
        // the range and the upper end stays open.
        let idx = [multikey(0, vec![IndexField::ascending("a")])];
        let two_sided = plan(doc! { "a": { "$gte": 1, "$lte": 1 } }, &idx).unwrap();
        let lower_only = plan(doc! { "a": { "$gte": 1 } }, &idx).unwrap();
        assert_eq!(
            two_sided, lower_only,
            "a two-sided range over a multikey index must not narrow further than its lower \
             bound alone"
        );
        assert!(!two_sided.both_bounds, "a multikey plan must not claim both bounds");

        // With no lower bound, the upper one is used instead.
        let upper_only = plan(doc! { "a": { "$lte": 1 } }, &idx).unwrap();
        assert_ne!(bounds(&upper_only).1, bounds(&lower_only).1);
    }

    #[test]
    fn an_index_that_has_never_seen_an_array_uses_both_ends() {
        // The selectivity the multikey flag exists to buy back: a scalar
        // satisfies both bounds iff its single key lies in the intersection,
        // so the scan can stop where the range does.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let two_sided = plan(doc! { "a": { "$gte": 1, "$lte": 5 } }, &idx).unwrap();
        let lower_only = plan(doc! { "a": { "$gte": 1 } }, &idx).unwrap();

        assert_eq!(bounds(&two_sided).0, bounds(&lower_only).0, "the lower bound is shared");
        assert!(
            bounds(&two_sided).1 < bounds(&lower_only).1,
            "the upper bound must actually close the range"
        );
        assert!(two_sided.both_bounds, "the plan must say it intersected, so the scan is checked");
        // One-sided ranges did not intersect anything and need no check.
        assert!(!lower_only.both_bounds);
        assert!(!plan(doc! { "a": { "$lte": 5 } }, &idx).unwrap().both_bounds);
    }

    #[test]
    fn both_bounds_apply_after_an_equality_prefix() {
        // The compound shape: `{a: 1, n: {$gte: 5, $lte: 9}}` over an index on
        // (a, n) must close the range on n, not fall back to scanning all of
        // `a == 1`.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::ascending("n")])];
        let p = plan(doc! { "a": 1, "n": { "$gte": 5, "$lte": 9 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 2);
        assert!(p.both_bounds);

        let wide = plan(doc! { "a": 1, "n": { "$gte": 5 } }, &idx).unwrap();
        assert!(bounds(&p).1 < bounds(&wide).1, "the range on n must be closed");
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
        // `$in` is absent from this list now: it narrows, as a union of
        // point probes.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        for query in [
            doc! { "a": { "$ne": 1 } },
            doc! { "a": { "$exists": true } },
            doc! { "a": { "$regex": "^x" } },
            doc! { "a": { "$size": 2 } },
        ] {
            assert!(plan(query.clone(), &idx).is_none(), "{query:?} must not narrow");
        }
    }

    #[test]
    fn a_descending_range_swaps_which_end_each_bound_narrows() {
        // Descending encoding inverts bytes, reversing order — so the
        // value-space lower bound must cap the key-space *top*. The proof is
        // in the encoded keys: with `n >= 5`, the key for n=7 must fall
        // inside the plan's range and the key for n=3 outside it.
        let idx = [index(0, vec![IndexField::descending("n")])];
        let key_of = |n: i32| keyenc::encode_compound_ordered(&[(Bson::Int32(n), true)]).unwrap();

        let p = plan(doc! { "n": { "$gte": 5 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 1, "the range must be planned, not dropped");
        let (lower, upper) = bounds(&p);
        let in_range = |k: &Vec<u8>| k >= lower && k <= upper;
        assert!(in_range(&key_of(7)), "7 satisfies n >= 5");
        assert!(in_range(&key_of(5)), "the bound itself is inclusive");
        assert!(!in_range(&key_of(3)), "3 does not satisfy n >= 5");

        // And the mirror: `n <= 5` keeps 3, excludes 7.
        let p = plan(doc! { "n": { "$lte": 5 } }, &idx).unwrap();
        let (lower, upper) = bounds(&p);
        let in_range = |k: &Vec<u8>| k >= lower && k <= upper;
        assert!(in_range(&key_of(3)));
        assert!(!in_range(&key_of(7)));
    }

    #[test]
    fn a_two_sided_descending_range_uses_both_ends() {
        let idx = [index(0, vec![IndexField::descending("n")])];
        let key_of = |n: i32| keyenc::encode_compound_ordered(&[(Bson::Int32(n), true)]).unwrap();

        let p = plan(doc! { "n": { "$gte": 5, "$lte": 9 } }, &idx).unwrap();
        assert!(p.both_bounds, "a scalar-only descending index must intersect");
        let (lower, upper) = bounds(&p);
        let in_range = |k: &Vec<u8>| k >= lower && k <= upper;
        for n in 5..=9 {
            assert!(in_range(&key_of(n)), "{n} is inside [5, 9]");
        }
        assert!(!in_range(&key_of(4)), "below the range");
        assert!(!in_range(&key_of(10)), "above the range");
    }

    #[test]
    fn a_multikey_descending_index_still_uses_only_one_end() {
        // The multikey rule is about the data, not the direction: different
        // array elements can satisfy each bound regardless of how the key is
        // encoded.
        let idx = [multikey(0, vec![IndexField::descending("n")])];
        let two_sided = plan(doc! { "n": { "$gte": 5, "$lte": 9 } }, &idx).unwrap();
        assert!(!two_sided.both_bounds);
        let lower_only = plan(doc! { "n": { "$gte": 5 } }, &idx).unwrap();
        assert_eq!(
            two_sided, lower_only,
            "multikey must not narrow past the value-space lower bound"
        );
    }

    #[test]
    fn a_descending_range_after_an_equality_prefix_is_planned() {
        // The compound shape the old fallback dropped: `{a: 1, n: {$gt: 5}}`
        // over (a asc, n desc) used only the equality prefix.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::descending("n")])];
        let p = plan(doc! { "a": 1, "n": { "$gt": 5 } }, &idx).unwrap();
        assert_eq!(p.fields_used, 2, "the descending range must be used");
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
        let (lower, upper) = bounds(&p);
        assert!(longer >= *lower && longer <= *upper, "a longer key must fall in range");
    }

    #[test]
    fn equal_values_across_numeric_types_produce_the_same_bounds() {
        let idx = [index(0, vec![IndexField::ascending("n")])];
        let a = plan(doc! { "n": 5i32 }, &idx).unwrap();
        let b = plan(doc! { "n": 5.0 }, &idx).unwrap();
        assert_eq!(a.ranges, b.ranges);
    }

    #[test]
    fn a_dollar_in_becomes_a_union_of_point_probes() {
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let p = plan(doc! { "a": { "$in": [3, 7, 5] } }, &idx).expect("$in should be planned");
        assert_eq!(p.ranges.len(), 3, "one probe per value");
        assert_eq!(p.fields_used, 1);
        assert!(!p.both_bounds, "equality probes intersect nothing");
        // In key order, so the union scans the index monotonically.
        let probe_lowers: Vec<_> = p.ranges.iter().map(|(lo, _)| lo.clone()).collect();
        let mut sorted = probe_lowers.clone();
        sorted.sort();
        assert_eq!(probe_lowers, sorted);

        // Each probe must cover exactly its value: the key for 5 falls in one
        // probe's range and the key for 4 in none.
        let key_of = |n: i32| keyenc::encode_compound_ordered(&[(Bson::Int32(n), false)]).unwrap();
        let hit = |k: &Vec<u8>| p.ranges.iter().any(|(lo, hi)| k >= lo && k <= hi);
        assert!(hit(&key_of(5)));
        assert!(!hit(&key_of(4)));
    }

    #[test]
    fn duplicate_in_values_probe_once() {
        // `[5, 5.0]` is one value in two numeric spellings, and the index
        // collapses them to one key — so the plan must too, or the union
        // would scan the same range twice.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let p = plan(doc! { "a": { "$in": [5, 5.0, 5i64] } }, &idx).unwrap();
        assert_eq!(p.ranges.len(), 1, "one distinct value, one probe");
    }

    #[test]
    fn an_empty_in_list_plans_an_empty_union() {
        // `{a: {$in: []}}` matches nothing; zero probes answer it without
        // touching a single document.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let p = plan(doc! { "a": { "$in": [] } }, &idx).expect("still a plan");
        assert!(p.ranges.is_empty());
    }

    #[test]
    fn a_dollar_in_after_an_equality_prefix_probes_within_it() {
        // `{a: 1, b: {$in: [2, 3]}}` over (a, b): each probe carries the
        // prefix, so the union stays inside a == 1.
        let idx = [index(0, vec![IndexField::ascending("a"), IndexField::ascending("b")])];
        let p = plan(doc! { "a": 1, "b": { "$in": [2, 3] } }, &idx).unwrap();
        assert_eq!(p.ranges.len(), 2);
        assert_eq!(p.fields_used, 2);

        let key = |a: i32, b: i32| {
            keyenc::encode_compound_ordered(&[(Bson::Int32(a), false), (Bson::Int32(b), false)])
                .unwrap()
        };
        let hit = |k: &Vec<u8>| p.ranges.iter().any(|(lo, hi)| k >= lo && k <= hi);
        assert!(hit(&key(1, 2)));
        assert!(hit(&key(1, 3)));
        assert!(!hit(&key(2, 2)), "a different prefix value must fall outside every probe");
        assert!(!hit(&key(1, 4)), "a value not listed must fall outside every probe");
    }

    #[test]
    fn an_equality_on_the_same_field_beats_its_in() {
        // `{a: 5, a: {$in: [1, 2]}}` cannot be written literally in BSON, but
        // `$eq` alongside `$in` can. The equality is consumed by the prefix,
        // and the plan must be the single-point one, not a union.
        let idx = [index(0, vec![IndexField::ascending("a")])];
        let p = plan(doc! { "a": { "$eq": 5, "$in": [1, 2, 5] } }, &idx).unwrap();
        assert_eq!(p.ranges.len(), 1, "the equality prefix answers this alone");
    }
}
