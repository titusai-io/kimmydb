//! Vector and hybrid search.
//!
//! What is in this module is the **exact** path: every stored vector is scored
//! and the best `k` are kept. That is O(n) per query but has no recall loss and
//! no index to keep consistent, which makes it both the path small collections
//! take and the oracle the approximate one is measured against.
//!
//! Choosing between the two lives in `crate::cache`, not here — this module is
//! deliberately unaware that an index exists. See `docs/vectors.md`.

use std::collections::HashMap;

use kimmy_core::{DocId, Metric, VectorRecord, similarity};
use kimmy_storage::{CollectionMeta, Engine};

use crate::error::Result;

/// One search result.
#[derive(Clone, Debug, PartialEq)]
pub struct Hit {
    /// The source document, not the chunk.
    pub id: DocId,
    /// Higher is better, whatever the metric.
    pub score: f32,
    /// Which chunk matched, and its text — so a caller can show *why*.
    pub chunk: u32,
    pub text: String,
}

/// Search options shared by both entry points.
#[derive(Clone, Debug)]
pub struct SearchOptions {
    pub k: usize,
    pub metric: Metric,
    /// Consider at most this many chunks per document.
    ///
    /// Without a cap, one long document can occupy every result slot with its
    /// own chunks and crowd out every other document.
    pub per_document: usize,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self { k: 10, metric: Metric::Cosine, per_document: 1 }
    }
}

/// Exact k-nearest-neighbour search over a collection's vectors.
///
/// `allowed` optionally restricts results to a set of document ids — the caller
/// computes it by running an ordinary filter first, so vector search composes
/// with the normal query language.
pub fn vector_search(
    engine: &Engine,
    shadow: &CollectionMeta,
    query: &[f32],
    options: &SearchOptions,
    allowed: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<Hit>> {
    let mut scored: Vec<Hit> = Vec::new();

    engine.for_each_vector(shadow, |record: VectorRecord| {
        // A dimension mismatch means this vector was written under a different
        // model. Scoring it would produce a meaningless number rather than an
        // error, so it is skipped.
        if record.vector.len() != query.len() {
            return Ok(true);
        }
        if let Some(allowed) = allowed
            && !allowed.contains(&record.source.to_string())
        {
            return Ok(true);
        }
        scored.push(Hit {
            score: similarity(query, &record.vector, options.metric),
            id: record.source,
            chunk: record.chunk,
            text: record.text,
        });
        Ok(true)
    })?;

    Ok(rank_hits(scored, options))
}

/// Keep the best hits, capped per document, then truncated to `k`.
pub(crate) fn rank_hits(mut hits: Vec<Hit>, options: &SearchOptions) -> Vec<Hit> {
    // Descending by score. `total_cmp` rather than `partial_cmp`: a NaN would
    // otherwise make the comparator inconsistent and the sort order undefined.
    hits.sort_by(|a, b| b.score.total_cmp(&a.score));

    let mut per_doc: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(options.k);
    for hit in hits {
        let seen = per_doc.entry(hit.id.to_string()).or_insert(0);
        if *seen >= options.per_document {
            continue;
        }
        *seen += 1;
        out.push(hit);
        if out.len() >= options.k {
            break;
        }
    }
    out
}

/// Combine two rankings by Reciprocal Rank Fusion.
///
/// RRF scores by *position* rather than raw score, which is what makes it
/// usable here: a cosine similarity and a keyword score are not on the same
/// scale, and normalizing them against each other requires assumptions about
/// their distributions that do not hold in general.
///
/// score(d) = Σ 1 / (k + rank(d))
///
/// This is [`weighted_reciprocal_rank_fusion`] with every weight at 1.
pub fn reciprocal_rank_fusion(rankings: &[Vec<Hit>], limit: usize) -> Vec<Hit> {
    let equal: Vec<(&[Hit], f32)> = rankings.iter().map(|r| (r.as_slice(), 1.0)).collect();
    weighted_reciprocal_rank_fusion(&equal, limit)
}

/// Reciprocal Rank Fusion with a weight per ranking.
///
/// score(d) = Σ w_i / (k + rank_i(d))
///
/// The weights say how much authority each ranking's *position* carries, which
/// is the one thing plain RRF fixes by fiat: it treats a rank of 3 in a
/// near-random ordering as exactly as informative as a rank of 3 in a good
/// one. Only the ratio between weights matters to the resulting order, since
/// scores are compared with each other and never with a threshold.
///
/// A ranking weighted zero contributes nothing and is skipped outright rather
/// than admitting its documents at score zero — so `(dense, 1), (lexical, 0)`
/// is the dense ranking, not the dense ranking followed by lexical stragglers.
/// Rejecting negative weights is the caller's job; here they would simply
/// subtract.
pub fn weighted_reciprocal_rank_fusion(rankings: &[(&[Hit], f32)], limit: usize) -> Vec<Hit> {
    /// Dampens the contribution of low-ranked results. 60 is the value from
    /// the original RRF paper and the common default.
    const RRF_K: f32 = 60.0;

    let mut fused: HashMap<String, (Hit, f32)> = HashMap::new();
    for (ranking, weight) in rankings {
        if *weight == 0.0 {
            continue;
        }
        for (position, hit) in ranking.iter().enumerate() {
            let contribution = weight / (RRF_K + position as f32 + 1.0);
            fused
                .entry(hit.id.to_string())
                .and_modify(|(_, score)| *score += contribution)
                .or_insert_with(|| (hit.clone(), contribution));
        }
    }
    let mut out: Vec<Hit> = fused.into_values().map(|(hit, score)| Hit { score, ..hit }).collect();
    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(limit);
    out
}

/// Rank documents by how well their stored chunk text matches a query string.
///
/// A deliberately simple term-overlap score, not BM25. It exists so hybrid
/// search has a lexical signal to fuse; RRF only uses the *ordering*, so the
/// absolute values do not need to be principled.
///
/// `min_overlap` is how many *distinct* query terms a chunk must contain to be
/// ranked at all. At `1` any chunk sharing a word with the query is a
/// candidate, which on short documents makes almost everything a candidate and
/// the ordering among them close to random. Raising it keeps only the chunks
/// that agree with the query on several words — what an exact-term match
/// actually looks like — and leaves the rest to the dense half. A query with
/// fewer distinct terms than `min_overlap` is held to its own count, so a
/// one-word query is never gated to nothing; `0` is treated as `1`.
pub fn keyword_search(
    engine: &Engine,
    shadow: &CollectionMeta,
    query: &str,
    options: &SearchOptions,
    min_overlap: usize,
) -> Result<Vec<Hit>> {
    let terms: Vec<String> = tokenize(query);
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    let mut distinct: Vec<&str> = terms.iter().map(String::as_str).collect();
    distinct.sort_unstable();
    distinct.dedup();
    let required = min_overlap.clamp(1, distinct.len());

    let mut scored = Vec::new();
    engine.for_each_vector(shadow, |record: VectorRecord| {
        let haystack = tokenize(&record.text);
        if haystack.is_empty() {
            return Ok(true);
        }
        let overlap = distinct.iter().filter(|t| haystack.iter().any(|h| h == *t)).count();
        if overlap < required {
            return Ok(true);
        }
        // The score counts query terms as written — a repeated term counts
        // twice — which is what it has always done; the gate above is the
        // only place distinctness matters.
        let matched = terms.iter().filter(|t| haystack.contains(t)).count();
        // Normalized by length so a long chunk does not win on size alone.
        let score = matched as f32 / haystack.len() as f32;
        scored.push(Hit { score, id: record.source, chunk: record.chunk, text: record.text });
        Ok(true)
    })?;

    Ok(rank_hits(scored, options))
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use kimmy_core::{ChunkConfig, ProviderConfig, VectorConfig};

    use super::*;

    fn config() -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo,
            dim: 2,
            metric: Metric::Cosine,
            document_prefix: None,
            query_prefix: None,
            chunk: ChunkConfig::default(),
        }
    }

    /// An engine with vectors already stored, so search can be tested without
    /// running the worker.
    fn setup(
        vectors: &[(i64, u32, [f32; 2], &str)],
    ) -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config()).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();

        let mut by_source: HashMap<i64, Vec<VectorRecord>> = HashMap::new();
        for (source, chunk, vector, text) in vectors {
            by_source.entry(*source).or_default().push(VectorRecord {
                source: DocId::Int64(*source),
                chunk: *chunk,
                source_hlc: kimmy_core::Hlc::new(1, 0),
                vector: vector.to_vec(),
                text: (*text).to_string(),
            });
        }
        for (source, records) in by_source {
            engine.put_vectors(&shadow, &DocId::Int64(source), &records).unwrap();
        }
        (engine, shadow, dir)
    }

    fn ids(hits: &[Hit]) -> Vec<i64> {
        hits.iter()
            .map(|h| match h.id {
                DocId::Int64(n) => n,
                _ => panic!("unexpected id"),
            })
            .collect()
    }

    #[test]
    fn nearest_vectors_rank_first() {
        let (engine, shadow, _dir) = setup(&[
            (1, 0, [1.0, 0.0], "east"),
            (2, 0, [0.0, 1.0], "north"),
            (3, 0, [-1.0, 0.0], "west"),
        ]);

        let hits =
            vector_search(&engine, &shadow, &[1.0, 0.0], &SearchOptions::default(), None).unwrap();
        assert_eq!(ids(&hits), vec![1, 2, 3], "closest first, opposite last");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn k_limits_the_result_count() {
        let (engine, shadow, _dir) =
            setup(&[(1, 0, [1.0, 0.0], "a"), (2, 0, [0.9, 0.1], "b"), (3, 0, [0.0, 1.0], "c")]);
        let options = SearchOptions { k: 2, ..Default::default() };
        assert_eq!(vector_search(&engine, &shadow, &[1.0, 0.0], &options, None).unwrap().len(), 2);
    }

    #[test]
    fn one_document_cannot_monopolize_the_results() {
        // A long document produces many chunks; without a per-document cap it
        // would fill every slot and crowd out everything else.
        let (engine, shadow, _dir) = setup(&[
            (1, 0, [1.0, 0.0], "a"),
            (1, 1, [0.99, 0.01], "b"),
            (1, 2, [0.98, 0.02], "c"),
            (2, 0, [0.9, 0.1], "d"),
        ]);

        let options = SearchOptions { k: 3, per_document: 1, ..Default::default() };
        let hits = vector_search(&engine, &shadow, &[1.0, 0.0], &options, None).unwrap();
        assert_eq!(ids(&hits), vec![1, 2], "each document should appear once");

        // Raising the cap lets more chunks of the same document through.
        let options = SearchOptions { k: 3, per_document: 3, ..Default::default() };
        let hits = vector_search(&engine, &shadow, &[1.0, 0.0], &options, None).unwrap();
        assert_eq!(hits.len(), 3);
        assert_eq!(ids(&hits)[0], 1);
    }

    #[test]
    fn a_filter_restricts_the_candidate_set() {
        // This is what lets vector search compose with the ordinary query
        // language: the filter runs first and its ids are passed in.
        let (engine, shadow, _dir) = setup(&[(1, 0, [1.0, 0.0], "a"), (2, 0, [0.9, 0.1], "b")]);

        let allowed: HashSet<String> = ["2".to_string()].into_iter().collect();
        let hits =
            vector_search(&engine, &shadow, &[1.0, 0.0], &SearchOptions::default(), Some(&allowed))
                .unwrap();
        assert_eq!(ids(&hits), vec![2], "the closer document was filtered out");
    }

    #[test]
    fn vectors_of_the_wrong_width_are_skipped_not_scored() {
        // They were written under a different model; scoring them would give a
        // meaningless number rather than an error.
        let (engine, shadow, _dir) = setup(&[(1, 0, [1.0, 0.0], "right")]);
        let hits =
            vector_search(&engine, &shadow, &[1.0, 0.0, 0.0], &SearchOptions::default(), None)
                .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn searching_an_empty_collection_returns_nothing() {
        let (engine, shadow, _dir) = setup(&[]);
        assert!(
            vector_search(&engine, &shadow, &[1.0, 0.0], &SearchOptions::default(), None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn hits_report_which_chunk_matched() {
        // Showing a user *why* something matched needs the chunk text.
        let (engine, shadow, _dir) =
            setup(&[(1, 0, [0.0, 1.0], "far"), (1, 1, [1.0, 0.0], "near")]);
        let hits =
            vector_search(&engine, &shadow, &[1.0, 0.0], &SearchOptions::default(), None).unwrap();
        assert_eq!(hits[0].chunk, 1);
        assert_eq!(hits[0].text, "near");
    }

    // -----------------------------------------------------------------------
    // Keyword and hybrid
    // -----------------------------------------------------------------------

    #[test]
    fn keyword_search_matches_terms_case_insensitively() {
        let (engine, shadow, _dir) = setup(&[
            (1, 0, [1.0, 0.0], "The Quick Brown Fox"),
            (2, 0, [0.0, 1.0], "a slow green turtle"),
        ]);
        let hits =
            keyword_search(&engine, &shadow, "quick fox", &SearchOptions::default(), 1).unwrap();
        assert_eq!(ids(&hits), vec![1]);
    }

    #[test]
    fn keyword_search_ignores_punctuation_and_empty_queries() {
        let (engine, shadow, _dir) = setup(&[(1, 0, [1.0, 0.0], "hello, world!")]);
        assert_eq!(
            ids(&keyword_search(&engine, &shadow, "world", &SearchOptions::default(), 1).unwrap()),
            vec![1]
        );
        assert!(
            keyword_search(&engine, &shadow, "   ", &SearchOptions::default(), 1)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn fusion_rewards_documents_ranked_well_by_both() {
        // Doc 2 is second in each ranking; doc 1 is first in one and absent
        // from the other. Appearing in both should win.
        let hit = |id: i64, score: f32| Hit {
            id: DocId::Int64(id),
            score,
            chunk: 0,
            text: String::new(),
        };
        let vector = vec![hit(1, 0.9), hit(2, 0.8)];
        let keyword = vec![hit(3, 0.9), hit(2, 0.8)];

        let fused = reciprocal_rank_fusion(&[vector, keyword], 10);
        assert_eq!(ids(&fused)[0], 2, "the consistently-ranked document should lead");
    }

    #[test]
    fn fusion_ignores_incomparable_raw_scores() {
        // A cosine similarity and a keyword score are on different scales.
        // RRF uses position only, so a tiny keyword score still contributes.
        let hit = |id: i64, score: f32| Hit {
            id: DocId::Int64(id),
            score,
            chunk: 0,
            text: String::new(),
        };
        let vector = vec![hit(1, 0.99)];
        let keyword = vec![hit(2, 0.0001)];

        let fused = reciprocal_rank_fusion(&[vector, keyword], 10);
        assert_eq!(fused.len(), 2);
        // Both were rank 1 in their own list, so they tie rather than the
        // larger raw score dominating.
        assert!((fused[0].score - fused[1].score).abs() < 1e-6);
    }

    #[test]
    fn fusion_truncates_to_the_limit() {
        let hit = |id: i64| Hit { id: DocId::Int64(id), score: 1.0, chunk: 0, text: String::new() };
        let ranking: Vec<Hit> = (1..=10).map(hit).collect();
        assert_eq!(reciprocal_rank_fusion(&[ranking], 3).len(), 3);
    }

    #[test]
    fn keyword_search_min_overlap_requires_that_many_distinct_terms() {
        // Doc 1 shares one term with the query, doc 2 shares two. At the
        // default gate both are candidates; at two, only the second is. On a
        // corpus of short documents this is the difference between a lexical
        // ranking that is noise and one that means "this really matches".
        let (engine, shadow, _dir) =
            setup(&[(1, 0, [1.0, 0.0], "the red apple"), (2, 0, [0.0, 1.0], "red and blue paint")]);
        let options = SearchOptions::default();
        let mut loose = ids(&keyword_search(&engine, &shadow, "red blue", &options, 1).unwrap());
        loose.sort_unstable();
        assert_eq!(loose, vec![1, 2]);

        let strict = keyword_search(&engine, &shadow, "red blue", &options, 2).unwrap();
        assert_eq!(ids(&strict), vec![2], "one shared term is no longer enough");
    }

    #[test]
    fn keyword_search_min_overlap_counts_distinct_terms_and_is_capped_by_the_query() {
        let (engine, shadow, _dir) = setup(&[(1, 0, [1.0, 0.0], "red red red")]);
        let options = SearchOptions::default();

        // "red red" has one distinct term. A gate of 2 is held to that one, so
        // the single-word query is not gated to nothing...
        let capped = keyword_search(&engine, &shadow, "red red", &options, 2).unwrap();
        assert_eq!(ids(&capped), vec![1]);
        // ...and a chunk repeating one query term does not count it twice.
        assert!(
            keyword_search(&engine, &shadow, "red blue", &options, 2).unwrap().is_empty(),
            "three occurrences of one term are one distinct term"
        );
        // Zero is not a meaningful gate and behaves as one.
        let zero = keyword_search(&engine, &shadow, "red blue", &options, 0).unwrap();
        assert_eq!(ids(&zero), vec![1]);
    }

    #[test]
    fn weighted_fusion_lets_the_dense_rank_outvote_the_lexical_one() {
        // Doc 1: first by the dense half, twentieth by the lexical. Doc 2:
        // tenth by the dense half, third by the lexical. Under equal weights
        // the lexical half decides it; under 0.7/0.3 the dense half does.
        //
        //   equal:   doc 1 = 1/61 + 1/80 = 0.02889   doc 2 = 1/70 + 1/63 = 0.03016
        //   0.7/0.3: doc 1 = 0.7/61 + 0.3/80 = 0.01523   doc 2 = 0.7/70 + 0.3/63 = 0.01476
        let hit = |id: i64| Hit { id: DocId::Int64(id), score: 0.0, chunk: 0, text: String::new() };
        let filler = |from: i64, n: i64| -> Vec<Hit> { (from..from + n).map(hit).collect() };

        let mut dense = vec![hit(1)];
        dense.extend(filler(100, 8));
        dense.push(hit(2));
        assert_eq!(dense.len(), 10);

        let mut lexical = filler(200, 2);
        lexical.push(hit(2));
        lexical.extend(filler(300, 16));
        lexical.push(hit(1));
        assert_eq!(lexical.len(), 20);

        let equal = weighted_reciprocal_rank_fusion(&[(&dense, 1.0), (&lexical, 1.0)], 2);
        assert_eq!(ids(&equal), vec![2, 1], "plain RRF: the lexical rank carries the day");
        // And plain RRF is exactly the equal-weight case, not merely close.
        let plain = reciprocal_rank_fusion(&[dense.clone(), lexical.clone()], 2);
        assert_eq!(plain, equal);

        let tilted = weighted_reciprocal_rank_fusion(&[(&dense, 0.7), (&lexical, 0.3)], 2);
        assert_eq!(ids(&tilted), vec![1, 2], "0.7/0.3: the dense rank does");

        // Only the ratio matters to the order.
        let scaled = weighted_reciprocal_rank_fusion(&[(&dense, 7.0), (&lexical, 3.0)], 2);
        assert_eq!(ids(&scaled), ids(&tilted));
    }

    #[test]
    fn a_zero_weight_switches_a_half_off_rather_than_admitting_it_at_zero() {
        let hit = |id: i64| Hit { id: DocId::Int64(id), score: 0.0, chunk: 0, text: String::new() };
        let dense = vec![hit(1), hit(2)];
        let lexical = vec![hit(3), hit(2)];

        let fused = weighted_reciprocal_rank_fusion(&[(&dense, 1.0), (&lexical, 0.0)], 10);
        assert_eq!(ids(&fused), vec![1, 2], "doc 3 was only ever lexical evidence");
        assert!((fused[0].score - 1.0 / 61.0).abs() < 1e-6, "and doc 2 got no lexical share");
    }

    #[test]
    fn ranking_tolerates_a_nan_score() {
        // total_cmp keeps the sort well-defined; partial_cmp would make the
        // comparator inconsistent and the order undefined.
        let hits = vec![
            Hit { id: DocId::Int64(1), score: f32::NAN, chunk: 0, text: String::new() },
            Hit { id: DocId::Int64(2), score: 0.5, chunk: 0, text: String::new() },
        ];
        let out = rank_hits(hits, &SearchOptions::default());
        assert_eq!(out.len(), 2, "a NaN must not drop results or panic");
    }
}
