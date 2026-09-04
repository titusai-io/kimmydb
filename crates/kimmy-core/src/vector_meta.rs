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
    /// Text put in front of every chunk before it is sent to the provider.
    /// Many models are trained to see a task marker in the input —
    /// `passage: ` (E5, BGE), `title: none | text: ` (EmbeddingGemma), an
    /// instruction line (Nemotron, Qwen3-Embedding) — and rank noticeably
    /// worse without it. Never stored with the chunk and never returned in a
    /// hit; changing it is a reconfigure, and so a reindex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_prefix: Option<String>,
    /// The same for search queries embedded by the server (`query: `,
    /// `task: search result | query: `, …). Applied to `query` text only;
    /// a caller-supplied `vector` is used as is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_prefix: Option<String>,
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
        // A requested width that disagrees with the pinned one would store
        // vectors of one shape under a config that promises another.
        if let ProviderConfig::OpenAi { dimensions: Some(requested), .. } = &self.provider
            && *requested != self.dim
        {
            return Err(format!(
                "vector.provider.dimensions ({requested}) must equal vector.dim ({})",
                self.dim
            ));
        }
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
    ///
    /// Written `Byo {}` rather than `Byo` on purpose. `deny_unknown_fields`
    /// above is applied per variant, and an internally-tagged *unit* variant
    /// has no field list for serde to apply it to: it reads the tag and stops,
    /// so `{"kind":"byo","nosuch":1}` was accepted and `nosuch` dropped while
    /// every struct variant beside it refused (ADR-121). The empty body gives
    /// serde the field list it needs — an unknown key is now "unknown field
    /// `nosuch`, there are no fields". The encoding does not move: `Byo` and
    /// `Byo {}` are indistinguishable in JSON, BSON and TOML alike, all three
    /// carrying exactly `{"kind": "byo"}`, so stored metadata and the
    /// replication wire read back unchanged.
    Byo {},
    /// OpenAI-compatible `/v1/embeddings`.
    OpenAi {
        model: String,
        #[serde(default)]
        endpoint: Option<String>,
        /// Name of the environment variable holding the key. The key itself is
        /// never stored in collection metadata.
        #[serde(default = "default_openai_key_env")]
        api_key_env: String,
        /// Ask the provider for this width — the OpenAI `dimensions` field,
        /// honoured by Matryoshka-trained models (OpenAI `text-embedding-3-*`,
        /// Nemotron-3-Embed, Qwen3-Embedding, EmbeddingGemma, Voyage). Must
        /// equal `dim`; absent means the model's native width. A provider
        /// that ignores the field returns its native width and fails the
        /// dimension check on the first embed rather than storing the wrong
        /// shape.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dimensions: Option<usize>,
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
    /// Cohere `/v2/embed`. Not covered by `custom_http`: the request key is
    /// `texts`, `input_type` is required, and the response nests under
    /// `embeddings.float`. See ADR-047.
    Cohere {
        model: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default = "default_cohere_key_env")]
        api_key_env: String,
    },
    /// Google Gemini `:batchEmbedContents`. Not covered by `custom_http`: the
    /// text nests under `content.parts`, vectors return under `values`, and
    /// auth is an `x-goog-api-key` header. See ADR-047.
    Gemini {
        model: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default = "default_gemini_key_env")]
        api_key_env: String,
    },
    /// In-process ONNX inference.
    ///
    /// Requires a build with the `local-embeddings` feature. The default build
    /// stays free of native dependencies, so this is rejected at configuration
    /// time rather than failing later on the first write.
    Local { model: String },
    /// A provider the operator defined server-side, under
    /// `[vector.providers.<name>]` in the node's configuration (ADR-115).
    ///
    /// The collection names it; the endpoint, model and key variable are the
    /// operator's and never appear in collection metadata. With
    /// `vector.provider.endpoints_locked` set this is the only way a
    /// collection can reach a remote provider at all.
    Profile { name: String },
}

/// Where an `open_ai` provider is called when no `endpoint` is given.
pub const OPENAI_ENDPOINT: &str = "https://api.openai.com";
/// Where a `cohere` provider is called when no `endpoint` is given.
pub const COHERE_ENDPOINT: &str = "https://api.cohere.com";
/// Where a `gemini` provider is called when no `endpoint` is given.
pub const GEMINI_ENDPOINT: &str = "https://generativelanguage.googleapis.com";

fn default_openai_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

fn default_cohere_key_env() -> String {
    "COHERE_API_KEY".to_string()
}

fn default_gemini_key_env() -> String {
    "GEMINI_API_KEY".to_string()
}

impl ProviderConfig {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Byo {} => "byo",
            Self::OpenAi { .. } => "openai",
            Self::Ollama { .. } => "ollama",
            Self::CustomHttp { .. } => "custom_http",
            Self::Cohere { .. } => "cohere",
            Self::Gemini { .. } => "gemini",
            Self::Local { .. } => "local",
            Self::Profile { .. } => "profile",
        }
    }

    /// The base URL this provider is called at, with the dialect's default
    /// applied where the configuration left it out.
    ///
    /// `None` for the providers that make no request (`byo`, `local`) and for
    /// a profile, whose endpoint is the operator's and known only once the
    /// profile is resolved. This is the one place the defaults live, so the
    /// address policy checks the URL the provider will actually be built with.
    pub fn endpoint(&self) -> Option<&str> {
        match self {
            Self::OpenAi { endpoint, .. } => Some(endpoint.as_deref().unwrap_or(OPENAI_ENDPOINT)),
            Self::Cohere { endpoint, .. } => Some(endpoint.as_deref().unwrap_or(COHERE_ENDPOINT)),
            Self::Gemini { endpoint, .. } => Some(endpoint.as_deref().unwrap_or(GEMINI_ENDPOINT)),
            Self::Ollama { endpoint, .. } | Self::CustomHttp { endpoint, .. } => Some(endpoint),
            Self::Byo {} | Self::Local { .. } | Self::Profile { .. } => None,
        }
    }

    /// The environment variable the provider's key is read from, if it
    /// authenticates at all. The *name*; the value is never held here.
    pub fn api_key_env(&self) -> Option<&str> {
        match self {
            Self::OpenAi { api_key_env, .. }
            | Self::Cohere { api_key_env, .. }
            | Self::Gemini { api_key_env, .. } => Some(api_key_env),
            Self::CustomHttp { api_key_env, .. } => api_key_env.as_deref(),
            Self::Byo {} | Self::Ollama { .. } | Self::Local { .. } | Self::Profile { .. } => None,
        }
    }

    /// Whether this provider embeds server-side.
    ///
    /// `byo` does not, which means the embedding worker has nothing to do and
    /// vectors arrive with the document instead.
    pub fn embeds_server_side(&self) -> bool {
        !matches!(self, Self::Byo {})
    }

    /// Reject a provider that cannot work. Public because a server-side
    /// profile is a `ProviderConfig` too, and is held to the same rules where
    /// it is defined.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Byo {} => Ok(()),
            Self::OpenAi { model, .. } if model.is_empty() => {
                Err("openai provider needs a model".into())
            }
            Self::OpenAi { dimensions: Some(0), .. } => {
                Err("openai provider dimensions must be greater than zero".into())
            }
            Self::Ollama { model, endpoint } => {
                if model.is_empty() {
                    return Err("ollama provider needs a model".into());
                }
                check_url(endpoint)
            }
            Self::CustomHttp { endpoint, .. } => check_url(endpoint),
            Self::Cohere { model, endpoint, .. } => {
                if model.is_empty() {
                    return Err("cohere provider needs a model".into());
                }
                match endpoint {
                    Some(url) => check_url(url),
                    None => Ok(()),
                }
            }
            Self::Gemini { model, endpoint, .. } => {
                if model.is_empty() {
                    return Err("gemini provider needs a model".into());
                }
                match endpoint {
                    Some(url) => check_url(url),
                    None => Ok(()),
                }
            }
            Self::Local { model } if model.is_empty() => Err("local provider needs a model".into()),
            Self::Local { .. } if !cfg!(feature = "local-embeddings") => {
                Err("the local embedding provider requires a build with the \
                 `local-embeddings` feature; the default build has no ONNX runtime. \
                 Use a remote provider, or the `kimmydb:local` image"
                    .into())
            }
            Self::Profile { name } if name.is_empty() => {
                Err("profile provider needs a name".into())
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
    /// A ceiling on the *estimated* token count of a chunk, on top of
    /// `max_chars`.
    ///
    /// `max_chars` assumes prose at roughly four characters per token. Dense
    /// text — code, JSON, CJK — runs at one to two, so a chunk that fits the
    /// character budget can exceed the provider's window and be refused
    /// outright: seen on a live cluster as a 1073-token chunk cut at 2000
    /// characters, rejected with `400` on every scan. When set, a chunk is
    /// also cut once its estimated token count reaches this — estimated as
    /// **one token per two bytes of UTF-8**, which is conservative for every
    /// script the model is likely to meet (prose ≈ 4 bytes/token, code ≈
    /// 2.5, CJK ≈ 3). Set it to the provider's per-input limit. `None` keeps
    /// the character rule alone, exactly as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<usize>,
}

/// The estimator behind [`ChunkConfig::max_tokens`]: bytes of UTF-8 per
/// estimated token. Two rather than a tokenizer, because the storage layer
/// has no business knowing the model's, and two is below every common
/// script's real ratio — an overestimate, which is the safe direction.
pub const BYTES_PER_TOKEN_ESTIMATE: usize = 2;

impl Default for ChunkConfig {
    fn default() -> Self {
        // ~512 tokens at a typical 4 chars/token, with a sentence of overlap.
        Self { max_chars: 2_000, overlap: 200, max_tokens: None }
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
        if self.max_tokens == Some(0) {
            return Err("vector.chunk.max_tokens must be greater than zero when set".into());
        }
        Ok(())
    }

    /// The byte budget `max_tokens` translates to, if any.
    fn max_bytes(&self) -> Option<usize> {
        self.max_tokens.map(|t| t.saturating_mul(BYTES_PER_TOKEN_ESTIMATE))
    }

    /// Estimated tokens in a piece of text, by the same rule the splitter cuts on.
    pub fn estimate_tokens(text: &str) -> usize {
        text.len().div_ceil(BYTES_PER_TOKEN_ESTIMATE)
    }

    /// Split text into overlapping chunks.
    ///
    /// Splits on character boundaries, never inside a UTF-8 sequence. A chunk
    /// ends at `max_chars` characters or, when `max_tokens` is set, at the
    /// byte budget it implies — whichever comes first. Overlap is always
    /// counted in characters.
    pub fn split(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return Vec::new();
        }
        let max_bytes = self.max_bytes();
        if chars.len() <= self.max_chars && max_bytes.is_none_or(|b| text.len() <= b) {
            return vec![text.to_string()];
        }

        let mut out = Vec::new();
        let mut start = 0;
        while start < chars.len() {
            // The window: up to `max_chars` characters, shortened to fit the
            // byte budget. At least one character always goes, so a single
            // wide character cannot stall the splitter.
            let mut end = (start + self.max_chars).min(chars.len());
            if let Some(budget) = max_bytes {
                let mut bytes = 0;
                let mut fit = start;
                for (i, c) in chars[start..end].iter().enumerate() {
                    bytes += c.len_utf8();
                    if bytes > budget && i > 0 {
                        break;
                    }
                    fit = start + i + 1;
                }
                end = fit;
            }
            out.push(chars[start..end].iter().collect());
            if end == chars.len() {
                break;
            }
            // Advance by this window's length less the overlap, never by
            // less than one character.
            let stride = (end - start).saturating_sub(self.overlap).max(1);
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
            provider: ProviderConfig::Byo {},
            dim: 384,
            metric: Metric::Cosine,
            document_prefix: None,
            query_prefix: None,
            chunk: ChunkConfig::default(),
        }
    }

    #[test]
    fn every_provider_has_a_distinct_wire_tag() {
        // The tag is what a stored configuration round-trips through, so two
        // providers sharing one — or one going empty — would silently
        // reinterpret an existing collection's configuration as another
        // provider's. Note `open_ai`'s serde tag is snake_case while its
        // `name()` is not; this pins the latter.
        let all = [
            ProviderConfig::Byo {},
            ProviderConfig::OpenAi {
                model: "m".into(),
                endpoint: None,
                api_key_env: default_openai_key_env(),
                dimensions: None,
            },
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://x".into() },
            ProviderConfig::Cohere {
                model: "m".into(),
                endpoint: None,
                api_key_env: default_cohere_key_env(),
            },
            ProviderConfig::Gemini {
                model: "m".into(),
                endpoint: None,
                api_key_env: default_gemini_key_env(),
            },
            ProviderConfig::Local { model: "m".into() },
            ProviderConfig::Profile { name: "p".into() },
        ];

        let names: std::collections::BTreeSet<_> = all.iter().map(|p| p.name()).collect();
        assert_eq!(names.len(), all.len(), "tags must be distinct: {names:?}");
        assert!(!names.contains(""), "and none may be empty");
        assert_eq!(ProviderConfig::Byo {}.name(), "byo");
        assert_eq!(all[3].name(), "cohere");
        assert_eq!(all[4].name(), "gemini");
    }

    #[test]
    fn the_default_key_variables_are_the_documented_ones() {
        // An operator sets these; a wrong default means the provider silently
        // finds no key and every embedding fails at request time rather than
        // at configuration time. Pinned because they are documented in
        // ADR-047 and in the vector docs.
        assert_eq!(default_openai_key_env(), "OPENAI_API_KEY");
        assert_eq!(default_cohere_key_env(), "COHERE_API_KEY");
        assert_eq!(default_gemini_key_env(), "GEMINI_API_KEY");
    }

    #[test]
    fn the_dialects_added_in_m8_validate_like_the_others() {
        // Cohere and Gemini were added with the same rules as the providers
        // beside them, and nothing checked that: a missing model or a
        // nonsense endpoint has to be refused at configuration time, when
        // there is someone to tell.
        let cohere = |model: &str, endpoint: Option<&str>| ProviderConfig::Cohere {
            model: model.into(),
            endpoint: endpoint.map(Into::into),
            api_key_env: default_cohere_key_env(),
        };
        let gemini = |model: &str, endpoint: Option<&str>| ProviderConfig::Gemini {
            model: model.into(),
            endpoint: endpoint.map(Into::into),
            api_key_env: default_gemini_key_env(),
        };

        for (label, empty, good, bad_url) in [
            ("cohere", cohere("", None), cohere("m", None), cohere("m", Some("not a url"))),
            ("gemini", gemini("", None), gemini("m", None), gemini("m", Some("not a url"))),
        ] {
            assert!(empty.validate().is_err(), "{label}: an empty model must be refused");
            assert!(good.validate().is_ok(), "{label}: a model and no endpoint is the common case");
            assert!(bad_url.validate().is_err(), "{label}: a malformed endpoint must be refused");
        }
    }

    #[test]
    fn a_profile_names_the_operators_provider_and_nothing_else() {
        // The whole point of the variant: a collection carries a name, and
        // the endpoint, model and key variable stay in the node's
        // configuration. Round-trips through the wire tag, and an empty name
        // is refused where there is someone to tell.
        let json = r#"{"kind":"profile","name":"corp-embed"}"#;
        let p: ProviderConfig = serde_json::from_str(json).unwrap();
        assert_eq!(p, ProviderConfig::Profile { name: "corp-embed".into() });
        assert_eq!(serde_json::to_string(&p).unwrap(), json);
        assert!(p.embeds_server_side(), "a profile is a server-side provider");
        assert_eq!(p.endpoint(), None, "the endpoint is the profile's, not the collection's");
        assert_eq!(p.api_key_env(), None);
        assert!(p.validate().is_ok());
        assert!(ProviderConfig::Profile { name: String::new() }.validate().is_err());
    }

    #[test]
    fn the_effective_endpoint_is_the_default_when_none_is_given() {
        // The address policy has to check the URL the provider will really
        // be built with, which for the hosted dialects is a default the
        // configuration never spells out.
        let openai = ProviderConfig::OpenAi {
            model: "m".into(),
            endpoint: None,
            api_key_env: default_openai_key_env(),
            dimensions: None,
        };
        assert_eq!(openai.endpoint(), Some(OPENAI_ENDPOINT));
        assert_eq!(openai.api_key_env(), Some("OPENAI_API_KEY"));
        let voyage = ProviderConfig::OpenAi {
            model: "m".into(),
            endpoint: Some("https://api.voyageai.com".into()),
            api_key_env: "VOYAGE_API_KEY".into(),
            dimensions: None,
        };
        assert_eq!(voyage.endpoint(), Some("https://api.voyageai.com"));
        assert_eq!(voyage.api_key_env(), Some("VOYAGE_API_KEY"));
        let custom =
            ProviderConfig::CustomHttp { endpoint: "http://x/embed".into(), api_key_env: None };
        assert_eq!(custom.endpoint(), Some("http://x/embed"));
        assert_eq!(
            custom.api_key_env(),
            None,
            "an unauthenticated custom endpoint names no variable"
        );
        assert_eq!(ProviderConfig::Byo {}.endpoint(), None);
        assert_eq!(ProviderConfig::Local { model: "m".into() }.endpoint(), None);
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
        c.chunk = ChunkConfig { max_chars: 100, overlap: 100, max_tokens: None };
        assert!(c.validate().is_err());
        c.chunk = ChunkConfig { max_chars: 100, overlap: 99, max_tokens: None };
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
        assert!(!ProviderConfig::Byo {}.embeds_server_side());
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

    #[test]
    fn an_unknown_key_inside_the_provider_is_rejected_for_byo_too() {
        // `deny_unknown_fields` on an internally-tagged enum is applied per
        // variant, and a *unit* variant has no field list to apply it to:
        // serde reads the tag and stops. That made `byo` — the default, and
        // the kind most likely to be hand-written — the one provider that
        // swallowed a typo, while every struct variant beside it refused.
        // Giving it an empty body closes that.
        let json = r#"{"fields":["a"],"provider":{"kind":"byo","nosuch":1},"dim":8}"#;
        assert!(serde_json::from_str::<VectorConfig>(json).is_err());
        assert!(serde_json::from_str::<ProviderConfig>(r#"{"kind":"byo","nosuch":1}"#).is_err());

        // The control, and the sibling that has always refused.
        serde_json::from_str::<ProviderConfig>(r#"{"kind":"byo"}"#).unwrap();
        assert!(
            serde_json::from_str::<ProviderConfig>(r#"{"kind":"open_ai","model":"m","nosuch":1}"#)
                .is_err()
        );
    }

    #[test]
    fn a_byo_provider_still_encodes_to_the_bare_tag_on_every_wire() {
        // What makes closing the hole above non-breaking, pinned so a later
        // edit cannot quietly break it: an empty struct variant encodes
        // exactly as the unit variant did, on both encodings a stored
        // configuration travels over. `CollectionMeta` is JSON on disk;
        // `VectorSet` is BSON on the replication wire (see
        // `kimmy-storage`'s `serialize_to_document` / `deserialize_from_document`
        // round trip). An extra key on either side — an empty `{}` body
        // serialized as such, say — would be a format change, and a peer or a
        // data directory written by an older node would stop reading.
        let c = config();

        let json = serde_json::to_value(&c).unwrap();
        assert_eq!(json["provider"], serde_json::json!({ "kind": "byo" }));

        let doc = bson::serialize_to_document(&c).unwrap();
        let provider = doc.get_document("provider").unwrap();
        assert_eq!(provider.len(), 1, "byo must carry the tag and nothing else: {provider:?}");
        assert_eq!(provider.get_str("kind").unwrap(), "byo");

        // And a value in that encoding — which is what every `byo` record
        // written before this change looks like — still decodes to it.
        assert_eq!(serde_json::from_value::<VectorConfig>(json).unwrap(), c);
        assert_eq!(bson::deserialize_from_document::<VectorConfig>(doc).unwrap(), c);
    }

    // -----------------------------------------------------------------------
    // Chunking
    // -----------------------------------------------------------------------

    #[test]
    fn short_text_is_a_single_chunk() {
        let c = ChunkConfig { max_chars: 100, overlap: 10, max_tokens: None };
        assert_eq!(c.split("hello"), vec!["hello"]);
        assert!(c.split("").is_empty());
    }

    #[test]
    fn long_text_splits_with_overlap() {
        let c = ChunkConfig { max_chars: 10, overlap: 3, max_tokens: None };
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
        let c = ChunkConfig { max_chars: 7, overlap: 2, max_tokens: None };
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
        let c = ChunkConfig { max_chars: 5, overlap: 1, max_tokens: None };
        let text = "日本語のテキストです";
        let chunks = c.split(text);
        // Reassembling proves no chunk cut a UTF-8 sequence — a byte-based
        // split would have panicked or produced invalid strings.
        assert!(chunks.iter().all(|s| !s.is_empty()));
        assert!(chunks.concat().contains('日'));
    }

    #[test]
    fn max_tokens_cuts_dense_text_the_character_rule_would_let_through() {
        // 300 CJK characters are 900 bytes: ~450 estimated tokens, well past a
        // 100-token window, while 300 characters is comfortably under the
        // character budget. The character rule alone would ship one chunk.
        let text: String = std::iter::repeat_n('漢', 300).collect();
        let by_chars = ChunkConfig { max_chars: 2_000, overlap: 0, max_tokens: None };
        assert_eq!(by_chars.split(&text).len(), 1);

        let by_tokens = ChunkConfig { max_chars: 2_000, overlap: 0, max_tokens: Some(100) };
        let chunks = by_tokens.split(&text);
        assert!(chunks.len() >= 5, "{} chunks", chunks.len());
        for chunk in &chunks {
            assert!(chunk.len() <= 200, "a chunk exceeded the byte budget: {} bytes", chunk.len());
            assert!(ChunkConfig::estimate_tokens(chunk) <= 100);
        }
        assert_eq!(chunks.concat().chars().count(), 300, "nothing lost, nothing repeated");
    }

    #[test]
    fn max_tokens_leaves_prose_within_budget_alone() {
        // 100 ASCII characters are 100 bytes ≈ 50 tokens: under a 512-token
        // window, so the character rule decides and one chunk goes.
        let text = "a".repeat(100);
        let c = ChunkConfig { max_chars: 2_000, overlap: 200, max_tokens: Some(512) };
        assert_eq!(c.split(&text), vec![text]);
    }

    #[test]
    fn overlap_still_applies_when_the_byte_budget_cuts() {
        // 40 two-byte characters, budget 10 tokens = 20 bytes = 10 chars per
        // window, overlap 2: windows start at 0, 8, 16, 24, 32 — four full
        // windows and a final one of the 8 characters that remain.
        let text: String = std::iter::repeat_n('é', 40).collect();
        let c = ChunkConfig { max_chars: 100, overlap: 2, max_tokens: Some(10) };
        let chunks = c.split(&text);
        let lengths: Vec<usize> = chunks.iter().map(|k| k.chars().count()).collect();
        assert_eq!(lengths, vec![10, 10, 10, 10, 8], "{chunks:?}");
    }

    #[test]
    fn max_tokens_of_zero_is_refused_and_absent_round_trips_absent() {
        let mut c = config();
        c.chunk = ChunkConfig { max_chars: 100, overlap: 10, max_tokens: Some(0) };
        assert!(c.validate().is_err());

        // Stored configurations from before the field existed decode with
        // it absent, and one that never set it does not start writing it.
        let json = serde_json::to_string(&ChunkConfig::default()).unwrap();
        assert!(!json.contains("max_tokens"), "{json}");
        let decoded: ChunkConfig = serde_json::from_str(r#"{"max_chars":50,"overlap":5}"#).unwrap();
        assert_eq!(decoded.max_tokens, None);
    }

    /// `endpoint` on the three remote-with-a-default providers, and
    /// `CustomHttp`'s `api_key_env`, have `#[serde(default)]` but no
    /// `skip_serializing_if`, unlike `dimensions` and `max_tokens` beside
    /// them — so an unset one has always serialized as a literal JSON `null`
    /// rather than an absent key, into `CollectionMeta` on disk and into a
    /// `VectorSet` oplog entry alike. `VectorConfig` and `ProviderConfig`
    /// stay exactly this permissive on purpose: they are not only the
    /// `POST .../vector` request body but the stored and replicated form, and
    /// ADR-128's null-refusal is deliberately kept off the type a node must
    /// still be able to load and apply after every earlier version wrote it
    /// this way (`kimmy_api::vectors::VectorConfigInput` carries the refusal
    /// on the request side instead). This is the compatibility this decision
    /// rests on, pinned so nobody "closes" it here later.
    #[test]
    fn a_null_endpoint_or_key_variable_still_decodes_as_absent() {
        let json = r#"{"fields":["a"],"dim":8,
            "provider":{"kind":"open_ai","model":"m","endpoint":null}}"#;
        let c: VectorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(c.provider, ProviderConfig::OpenAi { endpoint: None, .. }));

        let json = r#"{"fields":["a"],"dim":8,
            "provider":{"kind":"cohere","model":"m","endpoint":null}}"#;
        let c: VectorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(c.provider, ProviderConfig::Cohere { endpoint: None, .. }));

        let json = r#"{"fields":["a"],"dim":8,
            "provider":{"kind":"gemini","model":"m","endpoint":null}}"#;
        let c: VectorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(c.provider, ProviderConfig::Gemini { endpoint: None, .. }));

        let json = r#"{"fields":["a"],"dim":8,
            "provider":{"kind":"custom_http","endpoint":"http://x","api_key_env":null}}"#;
        let c: VectorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(c.provider, ProviderConfig::CustomHttp { api_key_env: None, .. }));
    }

    #[test]
    fn requested_dimensions_must_equal_dim() {
        let mut config: VectorConfig = serde_json::from_value(serde_json::json!({
            "fields": ["text"], "dim": 1024,
            "provider": { "kind": "open_ai", "model": "m", "dimensions": 1024 }
        }))
        .unwrap();
        assert!(config.validate().is_ok());
        config.dim = 2048;
        let err = config.validate().unwrap_err();
        assert!(err.contains("dimensions (1024)") && err.contains("dim (2048)"), "{err}");
        // Round-trips, and is omitted when absent so older metadata is unchanged.
        let json = serde_json::to_value(&config).unwrap();
        assert_eq!(json["provider"]["dimensions"], 1024);
        let plain: VectorConfig = serde_json::from_value(serde_json::json!({
            "fields": ["text"], "dim": 8, "provider": { "kind": "open_ai", "model": "m" }
        }))
        .unwrap();
        assert!(serde_json::to_value(&plain).unwrap()["provider"].get("dimensions").is_none());
    }
}
