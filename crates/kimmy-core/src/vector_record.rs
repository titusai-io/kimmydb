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
//!
//! A record also carries the fingerprint of the configuration that made it
//! (ADR-203), because the document's version cannot see a configuration
//! change: configurations do not touch documents. With it, vectors made under
//! an earlier model, width, field list, chunking or prefix read as stale on
//! every member, not only on the one that ran the reindex.

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
    /// The fingerprint ([`crate::VectorConfig::fingerprint`]) of the
    /// configuration this was made under, compared against the collection's
    /// current one to detect a configuration change (ADR-203).
    ///
    /// `None` on a record written before 0.43.0, by an older member, or by a
    /// client into a collection the server embeds; such a record is judged by
    /// its version alone, as every record was before ([`Unstamped`]). A
    /// record a client stores into a `byo` collection carries the `byo`
    /// configuration's, so a later switch to a server-side provider reads it
    /// as made under another configuration.
    ///
    /// Additive on disk: absent when `None`, so such a record encodes exactly
    /// as before, and a build that does not know the key ignores it. Stored as
    /// the fingerprint's bits in a signed 64-bit integer, since BSON refuses a
    /// `u64` above `i64::MAX` and half of all fingerprints are.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "fingerprint_bits")]
    pub config: Option<u64>,
}

/// How a record that carries no configuration fingerprint is judged; see
/// [`VectorRecord::is_stale`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unstamped {
    /// By its source version alone, as every record was judged before records
    /// carried a fingerprint. What a record written by an older release gets
    /// wherever nothing says the configuration changed since: re-embedding
    /// every such record on each ownership move would cost a provider call per
    /// document, for vectors that are almost always current.
    ByVersion,
    /// As stale: the caller knows the configuration changed since this member
    /// last completed a scan, and a record that cannot say which configuration
    /// made it may be from before the change.
    Stale,
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

    /// Whether this record needs making again: it was derived from an older
    /// version than `current`, or made under a configuration other than the
    /// one fingerprinted `config`. A record with no fingerprint is judged as
    /// `unstamped` says.
    pub fn is_stale(&self, current: Hlc, config: u64, unstamped: Unstamped) -> bool {
        self.source_hlc < current || self.made_under_another(config, unstamped)
    }

    /// Whether this record was made under a configuration other than the one
    /// fingerprinted `config`, whatever its version. A record with no
    /// fingerprint is judged as `unstamped` says.
    pub fn made_under_another(&self, config: u64, unstamped: Unstamped) -> bool {
        match self.config {
            Some(made_under) => made_under != config,
            None => unstamped == Unstamped::Stale,
        }
    }
}

/// The record's fingerprint as the bits of an `i64`, the widest integer BSON
/// stores, and back.
mod fingerprint_bits {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
        value.map(|bits| bits as i64).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        Ok(Option::<i64>::deserialize(deserializer)?.map(|bits| bits as u64))
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
            config: None,
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

    /// Above `i64::MAX`, as half of all fingerprints are: the value BSON would
    /// refuse as a `u64`.
    const HIGH: u64 = 0xdead_beef_0bad_c0de;

    #[test]
    fn staleness_compares_against_the_document_version() {
        let current = 7;
        for config in [None, Some(current)] {
            for unstamped in [Unstamped::ByVersion, Unstamped::Stale] {
                let r = VectorRecord { config, ..record(0, 100) };
                let stale = |hlc_ms| r.is_stale(Hlc::new(hlc_ms, 0), current, unstamped);
                assert!(stale(200), "a newer document makes vectors stale");
                if config.is_some() || unstamped == Unstamped::ByVersion {
                    assert!(!stale(100), "the same version is not stale");
                    assert!(!stale(50), "an older version must not mark it stale");
                }
            }
        }
    }

    /// The configuration a record says made it (ADR-203): another one makes it
    /// stale whatever its version, and the current one leaves the version to
    /// decide, even when records with no fingerprint are to be read as stale.
    #[test]
    fn staleness_compares_the_configuration_a_record_carries() {
        let at = Hlc::new(100, 0);
        let under = |config| VectorRecord { config: Some(config), ..record(0, 100) };
        for unstamped in [Unstamped::ByVersion, Unstamped::Stale] {
            assert!(under(HIGH).is_stale(at, 7, unstamped), "made under another configuration");
            assert!(!under(HIGH).is_stale(at, HIGH, unstamped), "made under this one");
        }
    }

    /// A record with no fingerprint (an older release's, an older member's, or
    /// a client's in a collection the server embeds) is judged by its version
    /// unless the caller knows the configuration changed. Reading it as stale
    /// everywhere would re-embed everything older releases wrote.
    #[test]
    fn a_record_without_a_fingerprint_is_judged_as_the_caller_says() {
        let r = record(0, 100);
        assert!(!r.is_stale(Hlc::new(100, 0), HIGH, Unstamped::ByVersion));
        assert!(r.is_stale(Hlc::new(100, 0), HIGH, Unstamped::Stale));
    }

    #[test]
    fn records_round_trip_through_json() {
        let r = record(1, 5);
        let text = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<VectorRecord>(&text).unwrap(), r);
        let stamped = VectorRecord { config: Some(HIGH), ..record(1, 5) };
        let text = serde_json::to_string(&stamped).unwrap();
        assert_eq!(serde_json::from_str::<VectorRecord>(&text).unwrap(), stamped);
    }

    // -----------------------------------------------------------------------
    // The stored format across the 0.43.0 boundary
    // -----------------------------------------------------------------------

    /// `VectorRecord` as 0.42.0 declared it, copied here so the tests below
    /// decode with the shape an older build has. Records are stored, and
    /// replicated, snapshotted and backed up, as the BSON documents of an
    /// ordinary collection, and every build reads them with serde and without
    /// `deny_unknown_fields`.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct RecordBefore043 {
        source: DocId,
        chunk: u32,
        source_hlc: Hlc,
        vector: Vec<f32>,
        text: String,
    }

    fn before_043() -> RecordBefore043 {
        let r = record(3, 42);
        RecordBefore043 {
            source: r.source,
            chunk: r.chunk,
            source_hlc: r.source_hlc,
            vector: r.vector,
            text: r.text,
        }
    }

    /// An older build reads a record carrying a fingerprint, from the stored
    /// document and from its bytes as a replication frame or snapshot page
    /// carries them, and drops the key: so a member rolled back to 0.42.0 reads
    /// every record 0.43.0 wrote. A fingerprint above `i64::MAX` encodes at
    /// all, which as a plain `u64` BSON would refuse.
    #[test]
    fn an_older_build_reads_a_record_carrying_a_fingerprint() {
        let stamped = VectorRecord { config: Some(HIGH), ..record(3, 42) };
        let doc = bson::serialize_to_document(&stamped).unwrap();
        assert!(doc.contains_key("config"), "the fingerprint is stored: {doc:?}");
        let old: RecordBefore043 = bson::deserialize_from_document(doc.clone()).unwrap();
        assert_eq!(old, before_043());
        let bytes = bson::serialize_to_vec(&stamped).unwrap();
        let old: RecordBefore043 = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(old, before_043());
        // And it comes back whole.
        assert_eq!(bson::deserialize_from_document::<VectorRecord>(doc).unwrap(), stamped);
    }

    /// This build reads a record an older build wrote, as having no
    /// fingerprint; and a record with none encodes exactly as an older build
    /// encoded it, so nothing about such a record changes on disk or the wire.
    #[test]
    fn this_build_reads_a_record_an_older_build_wrote() {
        let doc = bson::serialize_to_document(&before_043()).unwrap();
        let read: VectorRecord = bson::deserialize_from_document(doc.clone()).unwrap();
        assert_eq!(read, record(3, 42));
        assert_eq!(read.config, None);
        let bytes = bson::serialize_to_vec(&before_043()).unwrap();
        assert_eq!(bson::deserialize_from_slice::<VectorRecord>(&bytes).unwrap(), record(3, 42));
        assert_eq!(bson::serialize_to_document(&record(3, 42)).unwrap(), doc, "no key when none");
        assert_eq!(bson::serialize_to_vec(&record(3, 42)).unwrap(), bytes);
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
