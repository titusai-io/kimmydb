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
//!
//! A chunk is stored under an id built from its source and its number
//! ([`VectorRecord::id`]). The source part is an injective encoding of the
//! `_id` ([`ChunkSource`], ADR-239), not its display text: until 0.51.0 it was
//! `DocId`'s `Display`, under which every binary `_id` of one length, and a
//! String and the number or ObjectId it prints as, shared one chunk set.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;
use crate::ids::DocId;

/// The chunk-id format this build writes (ADR-239): 2, the injective encoding
/// of [`ChunkSource`]. 0.50.0 and every build before it wrote the `_id`'s
/// display text, which is format 1 and is never named. A member says it in its
/// facts block, and a store records that the recheck the change owes was
/// queued under it.
pub const CHUNK_ID_FORMAT: u64 = 2;

/// What a chunk id names as its source: the `_id` value as the document key
/// identifies it, and a binary's subtype (ADR-239).
///
/// The document key folds a binary's subtype (until 0.53.0), so two sources
/// that differ only by subtype are one document. Their chunk ids differ all
/// the same, and the document's run ([`VectorRecord::run_prefix`]) holds both.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChunkSource {
    ObjectId([u8; 12]),
    String(String),
    /// Int32 and Int64, which `DocId` normalises to one.
    Int64(i64),
    /// The canonical 19 bytes of a `Decimal128` `_id` ([`Decimal128Id::to_bytes`],
    /// ADR-238): equal values have equal bytes, so one decimal is one source.
    Decimal128([u8; 19]),
    /// Any length. `DocId::Uuid(u)` is subtype 4 over `u`'s bytes.
    Binary {
        subtype: u8,
        bytes: Vec<u8>,
    },
}

impl From<&DocId> for ChunkSource {
    /// Total over `DocId`. A folded `DocId::Binary` carries no subtype and is
    /// the generic one, 0; [`ChunkSource::of`] takes the subtype from the
    /// stored body where there is one.
    fn from(id: &DocId) -> Self {
        // No wildcard, here or in the other matches over `DocId` below: a new
        // kind of `_id` must name its chunk source, and the compiler asks.
        match id {
            DocId::ObjectId(oid) => Self::ObjectId(oid.bytes()),
            DocId::String(s) => Self::String(s.clone()),
            DocId::Int64(i) => Self::Int64(*i),
            DocId::Binary(bytes) => Self::Binary { subtype: 0x00, bytes: bytes.clone() },
            DocId::Uuid(u) => Self::Binary { subtype: 0x04, bytes: u.as_bytes().to_vec() },
            DocId::Decimal128(d) => Self::Decimal128(d.to_bytes()),
        }
    }
}

impl ChunkSource {
    /// The chunk source of the document keyed `id` whose stored body carries
    /// `body_id` as its `_id`: the body's binary subtype when the body's `_id`
    /// is a binary of the same bytes, and [`ChunkSource::from`] otherwise.
    ///
    /// The key folds the subtype and the body keeps it, so only a writer that
    /// holds the body can name it; the embedding worker does, in `prepare`.
    pub fn of(id: &DocId, body_id: Option<&bson::Bson>) -> Self {
        if let Some(bson::Bson::Binary(stored)) = body_id {
            let source =
                Self::Binary { subtype: u8::from(stored.subtype), bytes: stored.bytes.clone() };
            if source.is_of(id) {
                return source;
            }
        }
        Self::from(id)
    }

    /// The document this source is a chunk source of, as the key identifies
    /// it: a binary's subtype folded away. `None` for a `Decimal128` whose
    /// bytes are not a canonical id (a chunk id is parsed from stored text).
    pub fn document(&self) -> Option<DocId> {
        match self {
            Self::ObjectId(bytes) => Some(DocId::ObjectId(bson::oid::ObjectId::from_bytes(*bytes))),
            Self::String(s) => Some(DocId::String(s.clone())),
            Self::Int64(i) => Some(DocId::Int64(*i)),
            Self::Binary { bytes, .. } => Some(DocId::Binary(bytes.clone())),
            Self::Decimal128(bytes) => {
                crate::ids::Decimal128Id::from_bytes(bytes).map(DocId::Decimal128)
            }
        }
    }

    /// Whether this is a source of the document keyed `id`: the same value,
    /// or for a binary the same bytes under any subtype, since the key folds
    /// the subtype.
    pub fn is_of(&self, id: &DocId) -> bool {
        match id {
            DocId::ObjectId(oid) => *self == Self::ObjectId(oid.bytes()),
            DocId::String(s) => matches!(self, Self::String(own) if own == s),
            DocId::Int64(i) => *self == Self::Int64(*i),
            DocId::Binary(b) => matches!(self, Self::Binary { bytes, .. } if bytes == b),
            DocId::Uuid(u) => {
                matches!(self, Self::Binary { bytes, .. } if bytes.as_slice() == u.as_bytes())
            }
            DocId::Decimal128(d) => *self == Self::Decimal128(d.to_bytes()),
        }
    }

    /// The source part of a chunk id; see [`VectorRecord::id`] for the
    /// grammar.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        match self {
            Self::ObjectId(bytes) => push_hex(&mut out, bytes),
            Self::Int64(i) => {
                let _ = write!(out, "{i}");
            }
            Self::String(s) if is_reserved(s) => {
                out.push_str("~s:");
                out.push_str(s);
            }
            Self::String(s) => out.push_str(s),
            Self::Decimal128(bytes) => {
                out.push_str("~d:");
                push_hex(&mut out, bytes);
            }
            Self::Binary { subtype, bytes } => {
                out.push_str("~b:");
                push_hex(&mut out, bytes);
                out.push('.');
                push_hex(&mut out, &[*subtype]);
            }
        }
        out
    }

    /// The source part of a chunk id back, strictly: `None` for anything
    /// [`Self::encode`] does not write, so every source this accepts encodes
    /// to the text it was read from.
    fn decode(text: &str) -> Option<Self> {
        if let Some(tagged) = text.strip_prefix('~') {
            if let Some(s) = tagged.strip_prefix("s:") {
                // Only a String that needs the escape is written with it.
                return is_reserved(s).then(|| Self::String(s.to_string()));
            }
            if let Some(binary) = tagged.strip_prefix("b:") {
                let (bytes, subtype) = binary.rsplit_once('.')?;
                let [subtype] = decode_hex(subtype)?[..] else { return None };
                return Some(Self::Binary { subtype, bytes: decode_hex(bytes)? });
            }
            if let Some(decimal) = tagged.strip_prefix("d:") {
                return Some(Self::Decimal128(decode_hex(decimal)?.try_into().ok()?));
            }
            return None;
        }
        if text.len() == 24
            && let Some(bytes) = decode_hex(text)
        {
            return Some(Self::ObjectId(bytes.try_into().ok()?));
        }
        if is_canonical_i64(text) {
            return text.parse().ok().map(Self::Int64);
        }
        Some(Self::String(text.to_string()))
    }
}

/// Whether a String source is written escaped, `~s:` and itself: it begins
/// with `~`, which every tagged form does, or the parse would read it as an
/// ObjectId (24 lowercase hex digits) or an Int64 (an `i64`'s canonical
/// decimal). Every other String keeps its 0.50 chunk id, which is itself.
fn is_reserved(s: &str) -> bool {
    s.starts_with('~') || (s.len() == 24 && decode_hex(s).is_some()) || is_canonical_i64(s)
}

/// An `i64` written as `i64` writes it: an optional `-`, no `+`, no leading
/// zero, never `-0`, in range.
fn is_canonical_i64(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    is_canonical_digits(digits) && s != "-0" && s.parse::<i64>().is_ok()
}

/// A chunk number written as `u32` writes it: no sign, no leading zero.
fn parse_chunk(s: &str) -> Option<u32> {
    if !is_canonical_digits(s) {
        return None;
    }
    s.parse().ok()
}

/// Decimal digits with no sign and no leading zero, as an integer writes them:
/// checked without writing the number back, since the graph's key pre-filter
/// parses every candidate it is offered.
fn is_canonical_digits(s: &str) -> bool {
    match s.as_bytes() {
        [] => false,
        [b'0'] => true,
        [first, rest @ ..] => (b'1'..=b'9').contains(first) && rest.iter().all(u8::is_ascii_digit),
    }
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
}

/// Lowercase hex of an even length, and nothing else.
fn decode_hex(text: &str) -> Option<Vec<u8>> {
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let text = text.as_bytes();
    if !text.len().is_multiple_of(2) {
        return None;
    }
    text.chunks(2).map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?)).collect()
}

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
    /// The `_id` under which this chunk is stored (ADR-239).
    ///
    /// Derived from the source and chunk number so that re-embedding a
    /// document overwrites its chunks in place rather than accumulating
    /// duplicates. A `DocId::String` of this grammar, which is exact:
    ///
    /// ```text
    /// chunk-id        = source "#" chunk
    /// chunk           = "0" / ( %x31-39 *DIGIT )       ; a u32
    /// source          = object-id / int64 / plain-string / tagged
    /// object-id       = 24LHEX                         ; its 0.50 form
    /// int64           = [ "-" ] ( "0" / %x31-39 *DIGIT ) ; i64 range, never "-0": its 0.50 form
    /// plain-string    = a String that is not reserved  ; itself: its 0.50 form
    /// tagged          = "~s:" reserved-string
    ///                 / "~b:" *( 2LHEX ) "." 2LHEX     ; the bytes, then the subtype
    ///                 / "~d:" 38LHEX                   ; a Decimal128's canonical 19 bytes
    /// LHEX            = 0-9 a-f, lowercase only
    /// reserved-string = begins with "~", or is 24 LHEX, or is an i64's canonical decimal
    /// ```
    ///
    /// Injective over chunk sources, and the 0.50 id for ObjectId, Int64 and
    /// every String that is not reserved, so their chunks did not move.
    pub fn id(source: &ChunkSource, chunk: u32) -> DocId {
        DocId::String(format!("{}#{chunk}", source.encode()))
    }

    /// Split a stored chunk id back into its source and chunk number, by the
    /// grammar of [`Self::id`] and nothing looser: every id this accepts is
    /// the one [`Self::id`] writes for what it returns, so "this chunk is well
    /// keyed" is a comparison of the two.
    pub fn parse_id(id: &DocId) -> Option<(ChunkSource, u32)> {
        let DocId::String(s) = id else {
            return None;
        };
        Self::parse_key(s)
    }

    /// [`Self::parse_id`] of a String key, which the graph holds as text.
    pub fn parse_key(key: &str) -> Option<(ChunkSource, u32)> {
        // Split from the right: a string `_id` may itself contain '#'.
        let (source, chunk) = key.rsplit_once('#')?;
        Some((ChunkSource::decode(source)?, parse_chunk(chunk)?))
    }

    /// The prefix every chunk id of the document keyed `id` begins with, and
    /// which a scan of its run starts from. For a binary it stops before the
    /// subtype, `~b:` and the bytes and `.`, since the key folds the subtype
    /// (until 0.53.0): the run of the bytes under every subtype. The prefix
    /// is not proof of ownership (a String may contain `#`), so a reader parses
    /// each key and asks [`ChunkSource::is_of`].
    pub fn run_prefix(id: &DocId) -> String {
        let binary = |bytes: &[u8]| {
            let mut out = String::from("~b:");
            push_hex(&mut out, bytes);
            out.push('.');
            out
        };
        match id {
            DocId::Binary(bytes) => binary(bytes),
            DocId::Uuid(u) => binary(u.as_bytes()),
            DocId::ObjectId(_) | DocId::String(_) | DocId::Int64(_) | DocId::Decimal128(_) => {
                format!("{}#", ChunkSource::from(id).encode())
            }
        }
    }

    /// The prefix of the chunk ids a 0.50 build wrote for the document keyed
    /// `id`, frozen, for the kinds whose id changed: a binary, whose 0.50 id
    /// was its length (`bin(16 bytes)#`; a `DocId::Uuid`, its hyphenated text),
    /// and a reserved String, whose 0.50 id was its raw text. `None` for every
    /// other kind, whose id did not change.
    ///
    /// What sits under it is read, never written: it may hold another
    /// document's chunks, so a reader keeps only those whose body names `id`.
    pub fn legacy_prefix(id: &DocId) -> Option<String> {
        match id {
            DocId::Binary(bytes) => Some(format!("bin({} bytes)#", bytes.len())),
            DocId::Uuid(u) => Some(format!("{u}#")),
            DocId::String(s) if is_reserved(s) => Some(format!("{s}#")),
            // A Decimal128 `_id` did not exist before 0.51.0, so no 0.50 chunk is its.
            DocId::String(_) | DocId::ObjectId(_) | DocId::Int64(_) | DocId::Decimal128(_) => None,
        }
    }

    /// The chunk number of `id` when it is a key at the legacy location
    /// `prefix` ([`Self::legacy_prefix`]): the prefix, then a chunk number,
    /// and nothing else.
    pub fn legacy_chunk(id: &DocId, prefix: &str) -> Option<u32> {
        let DocId::String(s) = id else {
            return None;
        };
        parse_chunk(s.strip_prefix(prefix)?)
    }

    /// The document `id` as search tells documents apart: the encoding of
    /// its source, which is injective, where `Display` is not. For ObjectId,
    /// Int64 and every String that is not reserved it is their `Display`, so
    /// the order ties are broken in did not move for them.
    pub fn source_key(id: &DocId) -> String {
        ChunkSource::from(id).encode()
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
        let source = ChunkSource::from(&DocId::Int64(7));
        let id = VectorRecord::id(&source, 3);
        assert_eq!(id, DocId::String("7#3".into()));
        assert_eq!(VectorRecord::parse_id(&id), Some((ChunkSource::Int64(7), 3)));
    }

    #[test]
    fn re_embedding_overwrites_rather_than_accumulating() {
        // The id depends only on the source and chunk number, so writing the
        // same chunk twice replaces it instead of adding a duplicate.
        let source = ChunkSource::from(&DocId::Int64(7));
        assert_eq!(VectorRecord::id(&source, 0), VectorRecord::id(&source, 0));
        assert_ne!(VectorRecord::id(&source, 0), VectorRecord::id(&source, 1));
    }

    #[test]
    fn a_source_id_containing_a_hash_still_parses() {
        // Splitting from the left would truncate the id at the wrong '#'.
        let source = ChunkSource::String("order#42".into());
        let id = VectorRecord::id(&source, 2);
        assert_eq!(VectorRecord::parse_id(&id), Some((source, 2)));
    }

    #[test]
    fn malformed_chunk_ids_are_rejected() {
        assert_eq!(VectorRecord::parse_id(&DocId::String("nohash".into())), None);
        assert_eq!(VectorRecord::parse_id(&DocId::String("x#notanumber".into())), None);
        assert_eq!(VectorRecord::parse_id(&DocId::Int64(1)), None);
    }

    // -----------------------------------------------------------------------
    // The chunk-id encoding (ADR-239)
    // -----------------------------------------------------------------------

    /// `DocId`'s `Display` as 0.50.0 wrote it, frozen: what every chunk id was
    /// built from until 0.51.0.
    fn display_050(id: &DocId) -> String {
        match id {
            DocId::ObjectId(oid) => oid.to_hex(),
            DocId::String(s) => s.clone(),
            DocId::Int64(i) => i.to_string(),
            DocId::Binary(b) => format!("bin({} bytes)", b.len()),
            DocId::Uuid(u) => u.to_string(),
            // No Decimal128 `_id` existed in 0.50.0; this is its `Display`, which
            // is the twin a String can be made to collide with.
            DocId::Decimal128(d) => d.to_string(),
        }
    }

    /// The 0.50.0 chunk id, frozen.
    fn id_050(source: &DocId, chunk: u32) -> DocId {
        DocId::String(format!("{}#{chunk}", display_050(source)))
    }

    fn text(id: &DocId) -> &str {
        match id {
            DocId::String(s) => s,
            other => panic!("a chunk id is a String, not {other:?}"),
        }
    }

    /// An `_id` as a client or a test writes it, with what kimmydb keys it as.
    #[derive(Clone, Debug)]
    enum Value {
        Bson(bson::Bson),
        /// A `DocId::Uuid`, which only the engine's API builds.
        Uuid(uuid::Uuid),
        /// A `Decimal128` `_id`'s bytes, any 19 of them: the encoding is tested
        /// over bytes that are not a canonical id too; `Value::Bson` carries the
        /// real ones.
        Decimal([u8; 19]),
    }

    /// Two values are one source exactly when kimmydb keys them as one
    /// document and, for a binary, they carry one subtype. Computed from the
    /// document key, independently of the encoding under test.
    #[derive(Debug, PartialEq, Eq)]
    enum Identity {
        Key(Vec<u8>, Option<u8>),
        Decimal([u8; 19]),
    }

    impl Value {
        fn identity(&self) -> Identity {
            match self {
                Value::Bson(b) => {
                    let id = DocId::try_from_bson(b).unwrap();
                    let subtype = match b {
                        bson::Bson::Binary(bin) => Some(u8::from(bin.subtype)),
                        _ => None,
                    };
                    Identity::Key(crate::keyenc::encode(&id.to_bson()).unwrap(), subtype)
                }
                // As the binary subtype 4 it is: the engine keys a `DocId::Uuid`
                // under subtype 4, but nothing outside its API builds one, and
                // every other route folds a binary's subtype in the key (until
                // 0.53.0, which keys every binary by its subtype).
                Value::Uuid(u) => {
                    let folded = DocId::Binary(u.as_bytes().to_vec()).to_bson();
                    Identity::Key(crate::keyenc::encode(&folded).unwrap(), Some(4))
                }
                Value::Decimal(bytes) => Identity::Decimal(*bytes),
            }
        }

        fn source(&self) -> ChunkSource {
            match self {
                Value::Bson(b) => ChunkSource::of(&DocId::try_from_bson(b).unwrap(), Some(b)),
                Value::Uuid(u) => ChunkSource::from(&DocId::Uuid(*u)),
                Value::Decimal(bytes) => ChunkSource::Decimal128(*bytes),
            }
        }
    }

    fn binary(subtype: u8, bytes: Vec<u8>) -> bson::Bson {
        bson::Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::from(subtype),
            bytes,
        })
    }

    mod encoding {
        use proptest::prelude::*;

        use super::*;

        /// Strings weighted to the shapes the escape exists for: 24 hex digits,
        /// canonical and non-canonical decimals, a leading `~`, `#`, and the
        /// other kinds' encodings.
        fn any_string() -> impl Strategy<Value = String> {
            prop_oneof![
                "[0-9a-fA-F~#:.bsd-]{0,6}",
                "[0-9a-f]{24}",
                "[0-9A-F]{24}",
                "[0-9a-f]{23,25}",
                any::<i64>().prop_map(|i| i.to_string()),
                any::<i32>().prop_map(|i| format!("+{i}")),
                "0[0-9]{1,3}",
                Just("-0".to_string()),
                Just("9223372036854775808".to_string()),
                Just("-9223372036854775809".to_string()),
                "~[sbd]?:?[0-9a-f.]{0,8}",
                "[a-z0-9]{0,3}#[0-9]{0,3}",
                "bin\\([0-9]{1,2} bytes\\)",
                Just(String::new()),
            ]
        }

        fn any_value() -> impl Strategy<Value = Value> {
            prop_oneof![
                any::<[u8; 12]>().prop_map(|b| Value::Bson(bson::Bson::ObjectId(
                    bson::oid::ObjectId::from_bytes(b)
                ))),
                any_string().prop_map(|s| Value::Bson(bson::Bson::String(s))),
                prop_oneof![any::<i32>(), Just(i32::MIN), Just(i32::MAX), Just(0)]
                    .prop_map(|i| Value::Bson(bson::Bson::Int32(i))),
                prop_oneof![any::<i64>(), Just(i64::MIN), Just(i64::MAX), Just(-1), Just(0)]
                    .prop_map(|i| Value::Bson(bson::Bson::Int64(i))),
                (any::<u8>(), prop::collection::vec(any::<u8>(), 0..=40))
                    .prop_map(|(st, b)| Value::Bson(binary(st, b))),
                // Short binaries under the common subtypes, so equal bytes
                // under two subtypes come up.
                (
                    prop_oneof![Just(0u8), Just(4u8), Just(3u8), Just(0x80u8)],
                    prop::collection::vec(0u8..2, 0..=2)
                )
                    .prop_map(|(st, b)| Value::Bson(binary(st, b))),
                any::<[u8; 16]>().prop_map(|b| Value::Uuid(uuid::Uuid::from_bytes(b))),
                any::<[u8; 19]>().prop_map(Value::Decimal),
                // Real decimals, which `DocId` keys by value: an integral one in
                // range is an `Int64` (and one source with it), the rest are
                // `DocId::Decimal128`.
                (any::<u64>(), -40i32..40).prop_map(|(c, e)| Value::Bson(bson::Bson::Decimal128(
                    format!("{c}E{e}").parse().unwrap()
                ))),
            ]
        }

        /// A value and a second one often made from it: its 0.50 display, its
        /// encoding, the same bytes under another subtype, the same number in
        /// another width — the twins the old ids collided on.
        fn any_pair() -> impl Strategy<Value = (Value, Value)> {
            (any_value(), any_value(), 0u8..10, any::<u8>()).prop_map(|(a, other, how, st)| {
                let b = match (how, &a) {
                    (0, Value::Bson(b)) => Value::Bson(bson::Bson::String(display_050(
                        &DocId::try_from_bson(b).unwrap(),
                    ))),
                    (1, _) => Value::Bson(bson::Bson::String(a.source().encode())),
                    (2, _) => Value::Bson(bson::Bson::String(format!("{}#0", a.source().encode()))),
                    (3, Value::Bson(bson::Bson::Binary(bin))) => {
                        Value::Bson(binary(st, bin.bytes.clone()))
                    }
                    (3, Value::Uuid(u)) => Value::Bson(binary(st, u.as_bytes().to_vec())),
                    (4, Value::Bson(bson::Bson::Int32(i))) => {
                        Value::Bson(bson::Bson::Int64(i64::from(*i)))
                    }
                    (4, Value::Bson(bson::Bson::Int64(i))) => {
                        Value::Bson(bson::Bson::String(format!("~s:{i}")))
                    }
                    (5, _) => a.clone(),
                    _ => other,
                };
                (a, b)
            })
        }

        fn any_chunk() -> impl Strategy<Value = u32> {
            prop_oneof![Just(0u32), Just(1), Just(10), Just(u32::MAX), any::<u32>()]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(4000))]

            /// Test 1, injectivity: two values kimmydb keys as different
            /// documents, or that differ only by a binary's subtype, never
            /// share a chunk id at any pair of chunk numbers; values it keys as
            /// one document (Int32 5 and Int64 5) share one source.
            #[test]
            fn chunk_ids_are_injective_over_sources(
                (a, b) in any_pair(), n in any_chunk(), m in any_chunk()
            ) {
                let same = a.identity() == b.identity();
                prop_assert_eq!(a.source() == b.source(), same, "{:?} / {:?}", a, b);
                let (ia, ib) = (VectorRecord::id(&a.source(), n), VectorRecord::id(&b.source(), m));
                prop_assert_eq!(ia == ib, same && n == m, "{:?}#{} / {:?}#{}: {:?}", a, n, b, m, ia);
            }

            /// A source names the document its key identifies, a binary's
            /// subtype folded: what the graph's key pre-filter looks up.
            #[test]
            fn a_source_names_the_document_its_key_identifies(a in any_value()) {
                let source = a.source();
                let expected = match &a {
                    Value::Bson(b) => Some(DocId::try_from_bson(b).unwrap()),
                    Value::Uuid(u) => Some(DocId::Binary(u.as_bytes().to_vec())),
                    Value::Decimal(_) => None,
                };
                prop_assert_eq!(source.document(), expected.clone());
                if let Some(document) = expected {
                    prop_assert!(source.is_of(&document));
                }
            }

            /// The canonical-number checks are what writing the number back
            /// and comparing would say, without the allocation.
            #[test]
            fn canonical_numbers_are_what_writing_them_back_says(
                t in prop_oneof!["[-+0-9]{0,21}", any::<i64>().prop_map(|i| i.to_string())]
            ) {
                prop_assert_eq!(
                    is_canonical_i64(&t),
                    t.parse::<i64>().is_ok_and(|n| n.to_string() == t)
                );
                prop_assert_eq!(
                    parse_chunk(&t),
                    t.parse::<u32>().ok().filter(|n| n.to_string() == t)
                );
            }

            /// Test 2, the round trip.
            #[test]
            fn every_chunk_id_parses_back_to_its_source(a in any_value(), n in any_chunk()) {
                let id = VectorRecord::id(&a.source(), n);
                prop_assert_eq!(VectorRecord::parse_id(&id), Some((a.source(), n)));
            }

            /// Test 3, strictness: whatever the parse accepts encodes to itself.
            #[test]
            fn every_accepted_id_re_encodes_to_itself(t in "[0-9a-fA-F~#:.bsdx+-]{0,30}") {
                let id = DocId::String(t);
                if let Some((source, chunk)) = VectorRecord::parse_id(&id) {
                    prop_assert_eq!(VectorRecord::id(&source, chunk), id);
                }
            }

            /// And over every value's id, each changed by one character.
            #[test]
            fn a_near_miss_is_refused_or_re_encodes_to_itself(
                a in any_value(), n in any_chunk(), at in any::<prop::sample::Index>(),
                with in prop::sample::select(vec!['0', '9', 'a', 'f', 'A', '~', '#', '.', ':', '-', '+'])
            ) {
                let mut t: Vec<char> = text(&VectorRecord::id(&a.source(), n)).chars().collect();
                let i = at.index(t.len());
                t[i] = with;
                let id = DocId::String(t.into_iter().collect());
                if let Some((source, chunk)) = VectorRecord::parse_id(&id) {
                    prop_assert_eq!(VectorRecord::id(&source, chunk), id);
                }
            }

            /// Test 4, the 0.50 forms: an ObjectId's, an Int64's and every
            /// String's that is not reserved are the ids 0.50.0 wrote.
            #[test]
            fn the_050_id_is_kept_where_it_was_unambiguous(a in any_value(), n in any_chunk()) {
                let Value::Bson(b) = &a else { return Ok(()) };
                let id = DocId::try_from_bson(b).unwrap();
                let kept = match &id {
                    DocId::ObjectId(_) | DocId::Int64(_) => true,
                    DocId::String(s) => !is_reserved(s),
                    DocId::Binary(_) | DocId::Uuid(_) => false,
                    // There was no such id in 0.50.0, so there is no 0.50 form to keep.
                    DocId::Decimal128(_) => return Ok(()),
                };
                let now = VectorRecord::id(&ChunkSource::from(&id), n);
                prop_assert_eq!(now == id_050(&id, n), kept, "{:?}: {:?}", id, now);
                // And the legacy prefix is the 0.50 form exactly where it moved.
                let legacy = VectorRecord::legacy_prefix(&id);
                prop_assert_eq!(legacy.clone(), (!kept).then(|| format!("{}#", display_050(&id))));
                if let Some(prefix) = legacy {
                    prop_assert_eq!(VectorRecord::legacy_chunk(&id_050(&id, n), &prefix), Some(n));
                }
            }

            /// Test 5, the folded binary run: an id begins with the run prefix
            /// of bytes `b` exactly when it is a chunk of `b`, under any
            /// subtype.
            #[test]
            fn the_binary_run_is_prefix_closed(
                bytes in prop::collection::vec(0u8..3, 0..=4),
                (other, _) in any_pair(),
                cut in 0usize..5, st in any::<u8>(), n in any_chunk(),
            ) {
                let prefix = VectorRecord::run_prefix(&DocId::Binary(bytes.clone()));
                let mut near = bytes.clone();
                near.truncate(cut);
                near.push(st % 3);
                for candidate in [
                    other.source(),
                    ChunkSource::Binary { subtype: st, bytes: bytes.clone() },
                    ChunkSource::Binary { subtype: st, bytes: near },
                    ChunkSource::String(format!("{prefix}00")),
                    ChunkSource::String(prefix.clone()),
                ] {
                    let id = VectorRecord::id(&candidate, n);
                    let of = matches!(&candidate, ChunkSource::Binary { bytes: b, .. } if *b == bytes);
                    prop_assert_eq!(text(&id).starts_with(&prefix), of, "{:?}", candidate);
                    prop_assert_eq!(candidate.is_of(&DocId::Binary(bytes.clone())), of);
                }
            }
        }
    }

    /// A `Decimal128` `_id`'s chunk id is `~d:` and the 38 hex digits of its canonical
    /// bytes (sign, exponent, coefficient), and equal values are one source.
    #[test]
    fn a_decimal128_id_has_a_pinned_chunk_id() {
        let id = |text: &str| DocId::try_from_bson(&bson::Bson::Decimal128(text.parse().unwrap()));
        let one_and_a_half = id("1.5").unwrap();
        let source = ChunkSource::from(&one_and_a_half);
        assert_eq!(source.encode(), "~d:00ffff0000000000000000000000000000000f");
        assert_eq!(source.document(), Some(one_and_a_half.clone()));
        assert!(source.is_of(&one_and_a_half));
        assert_eq!(ChunkSource::from(&id("1.50").unwrap()), source, "equal values, one source");
        assert_ne!(ChunkSource::from(&id("-1.5").unwrap()), source);
        let chunk = VectorRecord::id(&source, 3);
        assert_eq!(VectorRecord::parse_id(&chunk), Some((source.clone(), 3)));
        assert!(text(&chunk).starts_with(&VectorRecord::run_prefix(&one_and_a_half)));
        assert_eq!(VectorRecord::legacy_prefix(&one_and_a_half), None, "0.50 had no decimal ids");
        // An integral decimal in range is the Int64 id of that value: one source with it.
        assert_eq!(ChunkSource::from(&id("5.00").unwrap()), ChunkSource::from(&DocId::Int64(5)));
    }

    /// Test 3's refusals, by name.
    #[test]
    fn the_parse_refuses_what_the_encoding_never_writes() {
        let refused = [
            // Chunk numbers: a leading zero, a sign, past `u32::MAX`, none.
            "7#01",
            "7#+1",
            "7#4294967296",
            "7#",
            "7#-1",
            // `~s:` before a String that needs no escape.
            "~s:plain#0",
            "~s:#0",
            "~s:0a#0",
            // Hex that is odd, uppercase, or not hex.
            "~b:0.00#0",
            "~b:0A.00#0",
            "~b:zz.00#0",
            "~d:00#0",
            "~d:000000000000000000000000000000000000000A#0",
            // A subtype that is not two digits.
            "~b:00.0#0",
            "~b:00.000#0",
            "~b:00#0",
            "~b:00.#0",
            "~b:00.0g#0",
            "~b:0.0.00#0",
            // An unknown tag, and a bare `~`.
            "~x:00#0",
            "~#0",
            "~bin#0",
        ];
        for text in refused {
            assert_eq!(VectorRecord::parse_id(&DocId::String(text.into())), None, "{text}");
        }
        // Their near neighbours that the encoding does write.
        for text in ["7#1", "~s:7#0", "~s:~x#0", "~b:.00#0", "~b:00.04#4294967295", "#0"] {
            let id = DocId::String(text.into());
            let (source, chunk) = VectorRecord::parse_id(&id).unwrap_or_else(|| panic!("{text}"));
            assert_eq!(VectorRecord::id(&source, chunk), id, "{text}");
        }
    }

    /// Test 6: one pinned id per kind, so a later change to the format fails
    /// here, loudly, rather than moving every stored chunk.
    #[test]
    fn chunk_ids_are_pinned_to_exact_strings() {
        let oid = bson::oid::ObjectId::parse_str("65f1a2b3c4d5e6f708192a3b").unwrap();
        let uuid = uuid::Uuid::from_bytes([0xab; 16]);
        let golden: [(ChunkSource, u32, &str); 13] = [
            (ChunkSource::from(&DocId::ObjectId(oid)), 0, "65f1a2b3c4d5e6f708192a3b#0"),
            (ChunkSource::from(&DocId::Int64(7)), 3, "7#3"),
            (ChunkSource::from(&DocId::Int64(i64::MIN)), 0, "-9223372036854775808#0"),
            (ChunkSource::String("report".into()), 12, "report#12"),
            (ChunkSource::String("a#1".into()), 0, "a#1#0"),
            (ChunkSource::String("7".into()), 0, "~s:7#0"),
            (
                ChunkSource::String("65f1a2b3c4d5e6f708192a3b".into()),
                1,
                "~s:65f1a2b3c4d5e6f708192a3b#1",
            ),
            (ChunkSource::String("~x".into()), 0, "~s:~x#0"),
            (ChunkSource::String("007".into()), 0, "007#0"),
            (ChunkSource::from(&DocId::Binary(vec![0, 1, 0xfe])), 2, "~b:0001fe.00#2"),
            (ChunkSource::from(&DocId::Uuid(uuid)), 0, "~b:abababababababababababababababab.04#0"),
            (ChunkSource::Decimal128([0x11; 19]), 0, "~d:11111111111111111111111111111111111111#0"),
            (
                ChunkSource::Binary { subtype: 0x80, bytes: Vec::new() },
                4294967295,
                "~b:.80#4294967295",
            ),
        ];
        for (source, chunk, expected) in golden {
            assert_eq!(VectorRecord::id(&source, chunk), DocId::String(expected.into()));
        }
        assert_eq!(VectorRecord::run_prefix(&DocId::Binary(vec![0, 1])), "~b:0001.");
        assert_eq!(VectorRecord::run_prefix(&DocId::String("7".into())), "~s:7#");
        assert_eq!(VectorRecord::run_prefix(&DocId::Int64(7)), "7#");
        assert_eq!(
            VectorRecord::legacy_prefix(&DocId::Binary(vec![0; 16])).unwrap(),
            "bin(16 bytes)#"
        );
        assert_eq!(VectorRecord::legacy_prefix(&DocId::String("7".into())).unwrap(), "7#");
        assert_eq!(VectorRecord::legacy_prefix(&DocId::String("x".into())), None);
        assert_eq!(VectorRecord::legacy_prefix(&DocId::Int64(7)), None);
        assert_eq!(VectorRecord::source_key(&DocId::String("7".into())), "~s:7");
        assert_eq!(VectorRecord::source_key(&DocId::Int64(7)), "7");
    }

    /// The subtype comes from the stored body's `_id`, and only when that is a
    /// binary of the document's own bytes.
    #[test]
    fn the_subtype_comes_from_the_body_that_names_the_same_bytes() {
        let id = DocId::Binary(vec![1, 2]);
        let of = |body: Option<bson::Bson>| ChunkSource::of(&id, body.as_ref());
        assert_eq!(
            of(Some(binary(4, vec![1, 2]))),
            ChunkSource::Binary { subtype: 4, bytes: vec![1, 2] }
        );
        assert_eq!(of(None), ChunkSource::Binary { subtype: 0, bytes: vec![1, 2] });
        assert_eq!(
            of(Some(binary(4, vec![9]))),
            ChunkSource::Binary { subtype: 0, bytes: vec![1, 2] }
        );
        assert_eq!(
            of(Some(bson::Bson::Int32(1))),
            ChunkSource::Binary { subtype: 0, bytes: vec![1, 2] }
        );
        let int = DocId::Int64(5);
        assert_eq!(ChunkSource::of(&int, Some(&bson::Bson::Int32(5))), ChunkSource::Int64(5));
        assert_eq!(ChunkSource::of(&int, Some(&binary(4, vec![5]))), ChunkSource::Int64(5));
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
