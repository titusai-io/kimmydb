//! Stored vectors.
//!
//! One record per *chunk* of a source document, not per document — a long
//! document becomes several vectors, each searchable on its own, all pointing
//! back at the same `_id`.
//!
//! Each record carries the HLC of the document version it was derived from.
//! That single field does most of the work in the pipeline:
//!
//! - **Staleness is detectable.** If a document's current stamp is newer than
//!   its vectors', they need re-embedding — no separate dirty flag to keep in
//!   sync with the data.
//! - **Re-embedding is idempotent.** Embedding the same version twice writes
//!   the same records.
//! - **Out-of-order work is safe.** A worker that processes an older version
//!   after a newer one can tell, and decline to overwrite.

use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;
use crate::ids::DocId;

/// One embedded chunk.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorRecord {
    /// The document this chunk came from.
    pub source: DocId,
    /// Which chunk of that document, in split order.
    pub chunk: u32,
    /// The version of the source document this was derived from.
    ///
    /// Compared against the document's current stamp to detect staleness.
    pub source_hlc: Hlc,
    /// The embedding itself.
    pub vector: Vec<f32>,
    /// The text that produced it, kept for hybrid search and for showing a
    /// user *why* a chunk matched.
    pub text: String,
}

impl VectorRecord {
    /// The `_id` under which this chunk is stored.
    ///
    /// Derived from the source id and chunk number so that re-embedding a
    /// document overwrites its chunks in place rather than accumulating
    /// duplicates.
    pub fn id(source: &DocId, chunk: u32) -> DocId {
        DocId::String(format!("{source}#{chunk}"))
    }

    /// Split a stored chunk id back into its source id and chunk number.
    ///
    /// The source part is returned as a string: the original may have been an
    /// ObjectId or an integer, and the shadow collection does not need to
    /// reconstruct its type — only to group chunks by document.
    pub fn parse_id(id: &DocId) -> Option<(String, u32)> {
        let DocId::String(s) = id else {
            return None;
        };
        // Split from the right: a string `_id` may itself contain '#'.
        let (source, chunk) = s.rsplit_once('#')?;
        Some((source.to_string(), chunk.parse().ok()?))
    }

    /// Whether this record was derived from an older version than `current`.
    pub fn is_stale(&self, current: Hlc) -> bool {
        self.source_hlc < current
    }
}

/// Similarity between two vectors under a metric.
///
/// Returns a **score where higher is better**, so callers can rank uniformly
/// without knowing whether the underlying metric is a distance or a similarity.
/// Euclidean distance is negated for that reason.
pub fn similarity(a: &[f32], b: &[f32], metric: crate::vector_meta::Metric) -> f32 {
    use crate::vector_meta::Metric;
    match metric {
        Metric::Cosine => cosine(a, b),
        Metric::Dot => dot(a, b),
        Metric::Euclidean => -euclidean(a, b),
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let denominator = norm(a) * norm(b);
    // A zero vector has no direction, so no angle is defined. Zero is the
    // neutral answer — neither similar nor opposite — and avoids a NaN
    // propagating into the ranking.
    if denominator == 0.0 {
        return 0.0;
    }
    dot(a, b) / denominator
}

fn norm(v: &[f32]) -> f32 {
    dot(v, v).sqrt()
}

fn euclidean(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_meta::Metric;

    fn record(chunk: u32, hlc_ms: u64) -> VectorRecord {
        VectorRecord {
            source: DocId::Int64(7),
            chunk,
            source_hlc: Hlc::new(hlc_ms, 0),
            vector: vec![1.0, 0.0],
            text: "hello".into(),
        }
    }

    #[test]
    fn chunk_ids_are_derived_and_reversible() {
        let source = DocId::Int64(7);
        let id = VectorRecord::id(&source, 3);
        assert_eq!(VectorRecord::parse_id(&id), Some(("7".to_string(), 3)));
    }

    #[test]
    fn re_embedding_overwrites_rather_than_accumulating() {
        // The id depends only on the source and chunk number, so writing the
        // same chunk twice replaces it instead of adding a duplicate.
        let source = DocId::Int64(7);
        assert_eq!(VectorRecord::id(&source, 0), VectorRecord::id(&source, 0));
        assert_ne!(VectorRecord::id(&source, 0), VectorRecord::id(&source, 1));
    }

    #[test]
    fn a_source_id_containing_a_hash_still_parses() {
        // Splitting from the left would truncate the id at the wrong '#'.
        let source = DocId::String("order#42".into());
        let id = VectorRecord::id(&source, 2);
        assert_eq!(VectorRecord::parse_id(&id), Some(("order#42".to_string(), 2)));
    }

    #[test]
    fn malformed_chunk_ids_are_rejected() {
        assert_eq!(VectorRecord::parse_id(&DocId::String("nohash".into())), None);
        assert_eq!(VectorRecord::parse_id(&DocId::String("x#notanumber".into())), None);
        assert_eq!(VectorRecord::parse_id(&DocId::Int64(1)), None);
    }

    #[test]
    fn staleness_compares_against_the_document_version() {
        let r = record(0, 100);
        assert!(r.is_stale(Hlc::new(200, 0)), "a newer document makes vectors stale");
        assert!(!r.is_stale(Hlc::new(100, 0)), "the same version is not stale");
        assert!(!r.is_stale(Hlc::new(50, 0)), "an older version must not mark it stale");
    }

    #[test]
    fn records_round_trip_through_json() {
        let r = record(1, 5);
        let text = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<VectorRecord>(&text).unwrap(), r);
    }

    // -----------------------------------------------------------------------
    // Similarity
    // -----------------------------------------------------------------------

    #[test]
    fn cosine_ranks_by_angle_not_magnitude() {
        let query = [1.0, 0.0];
        // Same direction, very different length — cosine should call them equal.
        assert!((cosine(&query, &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!((cosine(&query, &[100.0, 0.0]) - 1.0).abs() < 1e-6);
        // Orthogonal, then opposite.
        assert!(cosine(&query, &[0.0, 1.0]).abs() < 1e-6);
        assert!((cosine(&query, &[-1.0, 0.0]) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn a_zero_vector_scores_neutral_rather_than_nan() {
        // A NaN here would poison the ranking comparison silently.
        let score = similarity(&[0.0, 0.0], &[1.0, 0.0], Metric::Cosine);
        assert!(score.is_finite(), "got {score}");
        assert_eq!(score, 0.0);
    }

    #[test]
    fn every_metric_scores_higher_for_a_closer_match() {
        // The ranking layer must not need to know which metrics are distances.
        let query = [1.0, 0.0];
        let near = [0.9, 0.1];
        let far = [-1.0, 0.0];

        for metric in [Metric::Cosine, Metric::Dot, Metric::Euclidean] {
            let near_score = similarity(&query, &near, metric);
            let far_score = similarity(&query, &far, metric);
            assert!(
                near_score > far_score,
                "{metric:?}: near {near_score} should outrank far {far_score}"
            );
        }
    }

    #[test]
    fn euclidean_is_negated_so_higher_is_better() {
        let identical = similarity(&[1.0, 2.0], &[1.0, 2.0], Metric::Euclidean);
        assert_eq!(identical, 0.0, "an exact match is the maximum euclidean score");
        assert!(similarity(&[0.0, 0.0], &[3.0, 4.0], Metric::Euclidean) < 0.0);
    }
}
