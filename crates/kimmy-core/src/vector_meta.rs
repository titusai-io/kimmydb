//! Per-collection vector configuration.
//!
//! Lives in `kimmy-core` for the same reason as [`crate::index_meta`]: storage
//! writes vector records, the vector crate reads the provider settings, and the
//! API validates them, so the shape belongs where none of them depend on each
//! other.

use serde::{Deserialize, Serialize};

/// Suffix appended to a collection's name to hold its vectors.
///
/// The `__` prefix on the segment is reserved for system objects, so a user
/// cannot create a collection that shadows one of these.
pub const VECTOR_SUFFIX: &str = ".__vectors";

/// The shadow collection name for a source collection.
pub fn shadow_name(collection: &str) -> String {
    format!("{collection}{VECTOR_SUFFIX}")
}

/// Whether a name refers to a shadow collection rather than user data.
pub fn is_shadow(name: &str) -> bool {
    name.ends_with(VECTOR_SUFFIX)
}

/// Auto-embedding settings for one collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    /// Document fields whose text is embedded, in order. Dot paths allowed.
    pub fields: Vec<String>,
    pub provider: ProviderConfig,
    /// Vector width. Pinned at configuration time — changing a model changes
    /// this, and mixing widths in one index is meaningless, so a change
    /// requires an explicit reindex.
    pub dim: usize,
    #[serde(default)]
    pub metric: Metric,
    #[serde(default)]
    pub chunk: ChunkConfig,
}

impl VectorConfig {
    /// Reject configurations that cannot work, at configuration time rather
    /// than on the first write.
    pub fn validate(&self) -> Result<(), String> {
        if self.fields.is_empty() {
            return Err("vector.fields must name at least one field".into());
        }
        if self.dim == 0 {
            return Err("vector.dim must be greater than zero".into());
        }
        // Guards against a typo'd dimension quietly allocating enormous
        // records; no current embedding model exceeds this.
        const MAX_DIM: usize = 16_384;
        if self.dim > MAX_DIM {
            return Err(format!("vector.dim {} exceeds the maximum of {MAX_DIM}", self.dim));
        }
        self.chunk.validate()?;
        self.provider.validate()
    }
}

/// Where embeddings come from.
///
/// Untagged-with-`kind` rather than a bare string, because the remote providers
/// need an endpoint and a model name alongside the choice.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderConfig {
    /// The client supplies vectors; the server never embeds.
    ///
    /// The default, and the only provider that needs nothing external —
    /// no API key, no network, no model download.
    Byo,
    /// OpenAI-compatible `/v1/embeddings`.
    OpenAi {
        model: String,
        #[serde(default)]
        endpoint: Option<String>,
        /// Name of the environment variable holding the key. The key itself is
        /// never stored in collection metadata.
        #[serde(default = "default_openai_key_env")]
        api_key_env: String,
    },
    /// A local or remote Ollama server.
    Ollama { model: String, endpoint: String },
    /// Any endpoint accepting `{"input": [...]}` and returning
    /// `{"embeddings": [[...]]}`.
    CustomHttp {
        endpoint: String,
        #[serde(default)]
        api_key_env: Option<String>,
    },
    /// In-process ONNX inference.
    ///
    /// Requires a build with the `local-embeddings` feature. The default build
    /// stays free of native dependencies, so this is rejected at configuration
    /// time rather than failing later on the first write.
    Local { model: String },
}

fn default_openai_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

impl ProviderConfig {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Byo => "byo",
            Self::OpenAi { .. } => "openai",
            Self::Ollama { .. } => "ollama",
            Self::CustomHttp { .. } => "custom_http",
            Self::Local { .. } => "local",
        }
    }

    /// Whether this provider embeds server-side.
    ///
    /// `byo` does not, which means the embedding worker has nothing to do and
    /// vectors arrive with the document instead.
    pub fn embeds_server_side(&self) -> bool {
        !matches!(self, Self::Byo)
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Byo => Ok(()),
            Self::OpenAi { model, .. } if model.is_empty() => {
                Err("openai provider needs a model".into())
            }
            Self::Ollama { model, endpoint } => {
                if model.is_empty() {
                    return Err("ollama provider needs a model".into());
                }
                check_url(endpoint)
            }
            Self::CustomHttp { endpoint, .. } => check_url(endpoint),
            Self::Local { model } if model.is_empty() => Err("local provider needs a model".into()),
            Self::Local { .. } if !cfg!(feature = "local-embeddings") => {
                Err("the local embedding provider requires a build with the \
                 `local-embeddings` feature; the default build has no ONNX runtime. \
                 Use a remote provider, or the `kimmydb:local` image"
                    .into())
            }
            _ => Ok(()),
        }
    }
}

fn check_url(url: &str) -> Result<(), String> {
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(())
    } else {
        Err(format!("endpoint {url:?} must start with http:// or https://"))
    }
}

/// How vector similarity is measured.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    /// Angle between vectors, ignoring magnitude. The right default for text
    /// embeddings, which are usually normalized anyway.
    #[default]
    Cosine,
    /// Straight-line distance.
    Euclidean,
    /// Raw inner product. Only meaningful for vectors of comparable magnitude.
    Dot,
}

/// How long text is split before embedding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkConfig {
    /// Maximum characters per chunk.
    ///
    /// Characters rather than tokens: a token count depends on the model's
    /// tokenizer, which the storage layer has no business knowing about. This
    /// is a conservative proxy.
    pub max_chars: usize,
    /// Characters repeated between adjacent chunks, so a sentence split across
    /// a boundary still appears whole in one of them.
    pub overlap: usize,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        // ~512 tokens at a typical 4 chars/token, with a sentence of overlap.
        Self { max_chars: 2_000, overlap: 200 }
    }
}

impl ChunkConfig {
    fn validate(&self) -> Result<(), String> {
        if self.max_chars == 0 {
            return Err("vector.chunk.max_chars must be greater than zero".into());
        }
        // Equal would mean each chunk repeats the previous one entirely and the
        // splitter never advances.
        if self.overlap >= self.max_chars {
            return Err("vector.chunk.overlap must be smaller than max_chars".into());
        }
        Ok(())
    }

    /// Split text into overlapping chunks.
    ///
    /// Splits on character boundaries, never inside a UTF-8 sequence.
    pub fn split(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return Vec::new();
        }
        if chars.len() <= self.max_chars {
            return vec![text.to_string()];
        }

        let stride = self.max_chars - self.overlap;
        let mut out = Vec::new();
        let mut start = 0;
        while start < chars.len() {
            let end = (start + self.max_chars).min(chars.len());
            out.push(chars[start..end].iter().collect());
            if end == chars.len() {
                break;
            }
            start += stride;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> VectorConfig {
        VectorConfig {
            fields: vec!["title".into(), "body".into()],
            provider: ProviderConfig::Byo,
            dim: 384,
            metric: Metric::Cosine,
            chunk: ChunkConfig::default(),
        }
    }

    #[test]
    fn shadow_names_are_derived_and_recognizable() {
        assert_eq!(shadow_name("orders"), "orders.__vectors");
        assert!(is_shadow("orders.__vectors"));
        assert!(!is_shadow("orders"));
        assert!(!is_shadow("orders.vectors"));
    }

    #[test]
    fn a_valid_config_passes() {
        config().validate().unwrap();
    }

    #[test]
    fn configs_that_cannot_work_are_rejected() {
        let mut c = config();
        c.fields.clear();
        assert!(c.validate().is_err());

        let mut c = config();
        c.dim = 0;
        assert!(c.validate().is_err());

        let mut c = config();
        c.dim = 1_000_000;
        assert!(c.validate().is_err(), "an absurd dimension should not allocate");
    }

    #[test]
    fn overlap_must_leave_room_to_advance() {
        // Equal overlap means the splitter never moves forward.
        let mut c = config();
        c.chunk = ChunkConfig { max_chars: 100, overlap: 100 };
        assert!(c.validate().is_err());
        c.chunk = ChunkConfig { max_chars: 100, overlap: 99 };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn remote_providers_need_a_usable_endpoint() {
        let mut c = config();
        c.provider = ProviderConfig::Ollama {
            model: "nomic-embed-text".into(),
            endpoint: "localhost:11434".into(),
        };
        assert!(c.validate().is_err(), "a scheme-less endpoint should be rejected");

        c.provider = ProviderConfig::Ollama {
            model: "nomic-embed-text".into(),
            endpoint: "http://localhost:11434".into(),
        };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn the_local_provider_is_refused_without_the_feature() {
        // The default build has no ONNX runtime; failing at configuration time
        // is far clearer than failing on the first document write.
        let mut c = config();
        c.provider = ProviderConfig::Local { model: "bge-small-en-v1.5".into() };
        let result = c.validate();

        if cfg!(feature = "local-embeddings") {
            assert!(result.is_ok());
        } else {
            let err = result.unwrap_err();
            assert!(err.contains("local-embeddings"), "unhelpful error: {err}");
        }
    }

    #[test]
    fn byo_does_not_embed_server_side() {
        assert!(!ProviderConfig::Byo.embeds_server_side());
        assert!(
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://x".into() }
                .embeds_server_side()
        );
    }

    #[test]
    fn api_keys_are_referenced_by_env_var_not_stored() {
        // Collection metadata is readable by anyone who can read the data
        // directory, so it must never hold a credential.
        let json = r#"{"kind":"open_ai","model":"text-embedding-3-small"}"#;
        let p: ProviderConfig = serde_json::from_str(json).unwrap();
        match p {
            ProviderConfig::OpenAi { api_key_env, .. } => {
                assert_eq!(api_key_env, "OPENAI_API_KEY");
            }
            other => panic!("unexpected provider {other:?}"),
        }
        let text = serde_json::to_string(&config()).unwrap();
        assert!(!text.contains("sk-"), "no key material should ever be serialized");
    }

    #[test]
    fn config_round_trips_through_json() {
        let c = config();
        let text = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<VectorConfig>(&text).unwrap(), c);
    }

    #[test]
    fn unknown_config_keys_are_rejected() {
        // A typo should fail loudly rather than be silently ignored.
        let json = r#"{"fields":["a"],"provider":{"kind":"byo"},"dim":8,"metrik":"cosine"}"#;
        assert!(serde_json::from_str::<VectorConfig>(json).is_err());
    }

    // -----------------------------------------------------------------------
    // Chunking
    // -----------------------------------------------------------------------

    #[test]
    fn short_text_is_a_single_chunk() {
        let c = ChunkConfig { max_chars: 100, overlap: 10 };
        assert_eq!(c.split("hello"), vec!["hello"]);
        assert!(c.split("").is_empty());
    }

    #[test]
    fn long_text_splits_with_overlap() {
        let c = ChunkConfig { max_chars: 10, overlap: 3 };
        let chunks = c.split(&"abcdefghij".repeat(3)); // 30 chars
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 10, "chunk exceeded max_chars: {chunk:?}");
        }
        // Adjacent chunks must share their overlap, or a phrase split across a
        // boundary appears in neither whole.
        let first: Vec<char> = chunks[0].chars().collect();
        let second: Vec<char> = chunks[1].chars().collect();
        assert_eq!(first[first.len() - 3..], second[..3]);
    }

    #[test]
    fn splitting_covers_the_whole_input() {
        let c = ChunkConfig { max_chars: 7, overlap: 2 };
        let text = "abcdefghijklmnopqrstuvwxyz";
        let chunks = c.split(text);
        // Every character must appear somewhere, or embedding silently drops
        // part of the document.
        let joined: String = chunks.concat();
        for ch in text.chars() {
            assert!(joined.contains(ch), "character {ch:?} was dropped by chunking");
        }
        assert!(chunks.last().unwrap().ends_with('z'), "the tail must be included");
    }

    #[test]
    fn chunking_never_splits_a_multibyte_character() {
        let c = ChunkConfig { max_chars: 5, overlap: 1 };
        let text = "日本語のテキストです";
        let chunks = c.split(text);
        // Reassembling proves no chunk cut a UTF-8 sequence — a byte-based
        // split would have panicked or produced invalid strings.
        assert!(chunks.iter().all(|s| !s.is_empty()));
        assert!(chunks.concat().contains('日'));
    }
}
