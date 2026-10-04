//! Embedding providers.
//!
//! One trait, several implementations. The provider is the only part of the
//! vector pipeline that reaches outside the process, so it is also the only
//! part that can be slow, fail intermittently, or cost money — which is why
//! embedding runs off the write path entirely.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Provider calls that returned a usable response, process-wide.
static PROVIDER_REQUESTS: AtomicU64 = AtomicU64::new(0);
/// Input tokens the provider reported billing for, process-wide. Zero for
/// dialects that report none.
static PROVIDER_TOKENS: AtomicU64 = AtomicU64::new(0);

/// `(requests, input tokens)` every HTTP provider in this process has been
/// answered for since start — documents embedded by the worker and queries
/// embedded for a search alike. Process-wide rather than per worker because
/// the API's query path builds its own provider, and the question this
/// answers ("what did this node send the provider, and what was it billed
/// for?") is about the node. Rendered as `kimmy_embed_provider_requests_total`
/// and `kimmy_embed_provider_tokens_total`.
pub fn provider_totals() -> (u64, u64) {
    (PROVIDER_REQUESTS.load(Ordering::Relaxed), PROVIDER_TOKENS.load(Ordering::Relaxed))
}

/// The input-token count a response carries, if its dialect reports one.
///
/// OpenAI-compatible APIs (OpenAI, DeepInfra, llama.cpp, Voyage) put it at
/// `usage.prompt_tokens`; Cohere v2 at `meta.billed_units.input_tokens`;
/// Ollama at `prompt_eval_count`. Gemini's batch endpoint reports nothing.
/// Absent is zero, not an error: billing detail is never worth failing a
/// batch over.
fn billed_tokens(dialect: Dialect, body: &serde_json::Value) -> u64 {
    let n = match dialect {
        Dialect::OpenAi | Dialect::Custom => body.pointer("/usage/prompt_tokens"),
        Dialect::Cohere => body.pointer("/meta/billed_units/input_tokens"),
        Dialect::Ollama => body.get("prompt_eval_count"),
        Dialect::Gemini => None,
    };
    n.and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64))).unwrap_or(0)
}

use async_trait::async_trait;
use kimmy_core::ProviderConfig;
use kimmy_egress::CheckedResolver;

use crate::error::{Result, TransportKind, VectorError};
use crate::policy::{PolicyError, ProviderPolicy};

/// How long a connection attempt may take before it counts as failed.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The whole request, connect included. A hung provider must not hold the
/// worker's position forever; the worker's own retry takes it from here.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a pooled connection may sit idle before the client drops it.
/// Shorter than the typical load-balancer idle limit, so the client never
/// reuses a connection the far side has already closed.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause before the one in-client retry of a transport failure.
const RETRY_PAUSE: Duration = Duration::from_millis(250);

/// One HTTP client per provider, built once and reused for every call.
///
/// Built once because that is what makes it a client at all: `reqwest` pools
/// connections per `Client`, so a new one per call is a fresh DNS lookup and
/// TCP + TLS handshake for every document — measured on a three-member cluster on
/// 2026-08-28 as bursts of handshakes during a deferral drain, with 31
/// `error sending request` failures in seven seconds against a provider that
/// answered 40 of 40 sequential probes in the same minute.
///
/// The client resolves names through the address policy and follows no
/// redirect, for the reasons the webhook delivery client does the same
/// (ADR-115): the endpoint is checked before each call, but a name can resolve
/// inward between the check and the dial, and a permitted host answering `302`
/// to a private address would otherwise walk the request through the policy.
fn http_client(policy: &ProviderPolicy) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .tcp_keepalive(Duration::from_secs(30))
        .user_agent(concat!("kimmyd/", env!("CARGO_PKG_VERSION")))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(std::sync::Arc::new(CheckedResolver::new(policy.egress().clone())))
        .build()
        // The builder only fails when a TLS backend cannot initialise, which
        // is a broken build rather than a runtime condition.
        .expect("HTTP client")
}

/// What a provider reaches its endpoint with: the client, and the address
/// policy its endpoint is checked against before each call
/// ([`HttpProvider::check_destination`]).
struct Outbound {
    http: reqwest::Client,
    egress: kimmy_egress::EgressPolicy,
}

fn outbound(policy: &ProviderPolicy) -> Outbound {
    Outbound { http: http_client(policy), egress: policy.egress().clone() }
}

/// The URL an OpenAI-dialect endpoint setting resolves to.
///
/// A bare base (`https://api.openai.com`, `http://llama-embed:5301`) gets the
/// standard `/v1/embeddings` appended. A setting that already names the
/// embeddings route is used verbatim, query string and all: providers that
/// mount the OpenAI-compatible API under a prefix — DeepInfra's documented
/// `…/v1/openai/embeddings`, Azure's `…/openai/deployments/<name>/embeddings
/// ?api-version=…`, any gateway under a path — would otherwise be unreachable
/// with this dialect, and the only sign would be a 404 on every call. Found
/// on 2026-08-28: DeepInfra happened to answer on `/v1/embeddings` too, which
/// is the only reason the appended form worked.
fn openai_url(base: &str) -> String {
    let path = base.split_once('?').map_or(base, |(p, _)| p).trim_end_matches('/');
    if path.ends_with("/embeddings") { base.to_string() } else { format!("{path}/v1/embeddings") }
}

/// Turn a reqwest error into what an operator needs to read: which stage
/// failed, and the whole cause chain rather than reqwest's outer message —
/// `error sending request for url (...)` on its own says nothing about
/// whether DNS, the handshake or the far end's reset was the problem.
fn describe(e: &reqwest::Error) -> (TransportKind, String) {
    let kind = if e.is_connect() {
        TransportKind::Connect
    } else if e.is_timeout() {
        TransportKind::Timeout
    } else if e.is_request() || e.is_body() || e.is_decode() {
        TransportKind::Reset
    } else {
        TransportKind::Other
    };
    let mut detail = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    (kind, detail)
}

/// Turns text into vectors.
///
/// Batched rather than one-at-a-time: every remote provider charges a
/// round-trip per call, and a document usually produces several chunks.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed a batch of texts, returning one vector per input **in order**.
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// The width this provider produces.
    fn dim(&self) -> usize;

    fn name(&self) -> &'static str;
}

/// Build a provider from its configuration, under this node's policy.
///
/// The policy is asked here, at use time, and not only when the configuration
/// was accepted: a configuration also arrives by replication from another
/// member, having passed that member's API and never this one's. A profile
/// is resolved to the operator's definition first; then the key variable and
/// the endpoint are checked before the variable is read, so a refused name is
/// never even looked up in the environment.
///
/// The endpoint's check here is the half that needs no resolver: its shape,
/// the allowlist and a literal address. Building a provider never waits on
/// DNS, so it is cheap to keep cached and is not rebuilt while the endpoint's
/// host does not resolve. The addresses the host resolves to are checked
/// before every call instead, off the runtime's workers and under
/// [`CONNECT_TIMEOUT`], in [`HttpProvider::embed`].
///
/// The `Byo` provider has no implementation here on purpose: it means the
/// client supplies vectors and the server never embeds, so there is nothing to
/// call. Callers check [`ProviderConfig::embeds_server_side`] first.
pub fn build(
    config: &ProviderConfig,
    dim: usize,
    policy: &ProviderPolicy,
) -> Result<Box<dyn EmbeddingProvider>> {
    let config = policy.resolve(config).map_err(|e| refused(e, config.name()))?;
    policy.check_provider(config).map_err(|e| refused(e, config.name()))?;
    let endpoint = config.endpoint().map(str::to_string);
    match config {
        ProviderConfig::Byo {} => Err(VectorError::NoProvider),

        ProviderConfig::OpenAi { model, api_key_env, dimensions, .. } => {
            Ok(Box::new(HttpProvider::openai(
                endpoint.expect("openai has an endpoint"),
                model.clone(),
                api_key_env.clone(),
                dim,
                *dimensions,
                outbound(policy),
            )?))
        }
        ProviderConfig::Ollama { model, endpoint } => Ok(Box::new(HttpProvider::ollama(
            endpoint.clone(),
            model.clone(),
            dim,
            outbound(policy),
        ))),
        ProviderConfig::CustomHttp { endpoint, api_key_env } => Ok(Box::new(HttpProvider::custom(
            endpoint.clone(),
            api_key_env.clone(),
            dim,
            outbound(policy),
        )?)),
        ProviderConfig::Cohere { model, api_key_env, .. } => Ok(Box::new(HttpProvider::cohere(
            endpoint.expect("cohere has an endpoint"),
            model.clone(),
            api_key_env.clone(),
            dim,
            outbound(policy),
        )?)),
        ProviderConfig::Gemini { model, api_key_env, .. } => Ok(Box::new(HttpProvider::gemini(
            endpoint.expect("gemini has an endpoint"),
            model.clone(),
            api_key_env.clone(),
            dim,
            outbound(policy),
        )?)),

        ProviderConfig::Local { model } => local_provider(model, dim),
        // `resolve` replaced a profile with its definition, and a definition
        // is never itself a profile (the policy refuses one at construction).
        ProviderConfig::Profile { name } => Err(VectorError::UnknownProfile { name: name.clone() }),
    }
}

/// A policy refusal as the worker and the search path see it.
///
/// A host that could not be resolved is the one refusal that is a condition
/// of the moment rather than of the configuration, so it is reported as the
/// transport failure it is and retried on the worker's clock. Everything
/// else asks the same policy the same question on retry, and is permanent.
fn refused(e: PolicyError, provider: &'static str) -> VectorError {
    match e {
        PolicyError::UnknownProfile { name } => VectorError::UnknownProfile { name },
        PolicyError::Endpoint(e) if e.is_lookups_busy() => {
            VectorError::ResolverBusy { provider, detail: e.to_string() }
        }
        e if e.is_unresolvable() => {
            VectorError::Transport { provider, kind: TransportKind::Connect, detail: e.to_string() }
        }
        e => VectorError::PolicyRefused(e.to_string()),
    }
}

/// Run `work` with the class stamped `Remote`, putting it back to `Local` after,
/// whichever way it ends. For work the heartbeat cannot beat inside: the in-process
/// model's download and load, and its inference, which hold a runtime worker for
/// longer than the 30 s local bound without a point to beat at. `Remote` is the
/// phase with the longer bound (an attempt's 60 s and its grace, 75 s: the nearest to
/// the design's 30 s and a second per text, 62 s at the default 32 chunks), and past
/// it the class is overdue and counted a local fault, as for any call that outlives
/// its own timer. A stand-in, since the cell has no per-call local bound. **A
/// first-use model download that takes more than 75 s therefore reads as a local
/// fault and can make the member yield embeddings.** That is accepted: while the
/// model downloads the member cannot embed anyway.
#[cfg(any(feature = "local-embeddings", test))]
pub(crate) fn held_as_remote<T>(work: impl FnOnce() -> T) -> T {
    let _remote = RemoteCall::begin();
    work()
}

#[cfg(feature = "local-embeddings")]
fn local_provider(model: &str, dim: usize) -> Result<Box<dyn EmbeddingProvider>> {
    // The model's download and load run on the caller's worker, with nothing to beat
    // at: held as `Remote` for the length of it.
    Ok(Box::new(held_as_remote(|| crate::local::LocalProvider::new(model, dim))?))
}

#[cfg(not(feature = "local-embeddings"))]
fn local_provider(_model: &str, _dim: usize) -> Result<Box<dyn EmbeddingProvider>> {
    // Configuration validation rejects this earlier; reaching here means a
    // config was written by a build that had the feature and is now being read
    // by one that does not.
    Err(VectorError::LocalUnavailable)
}

/// Which request and response shape a remote endpoint speaks.
///
/// Audited against each provider's **documented** API shape and pinned with
/// the fixture tests below — the same verification every dialect here has had
/// since M2, since the suite has never called a live embedding endpoint (that
/// needs a key and would publish text to a third party). A provider that
/// changes its shape is a fixture update, and the tests are where a reviewer
/// checks the shape against current docs. See ADR-047.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
    /// `{"input": [...], "model": "..."}` → `{"data": [{"embedding": [...]}]}`.
    ///
    /// **Voyage AI speaks this** — it is deliberately OpenAI-compatible — so a
    /// Voyage collection is an `openai` provider with
    /// `endpoint: "https://api.voyageai.com"`. No separate dialect; the test
    /// `voyage_is_the_openai_dialect` pins that it stays covered.
    OpenAi,
    /// One text per request: `{"prompt": "...", "model": "..."}` →
    /// `{"embedding": [...]}`.
    Ollama,
    /// `{"input": [...]}` → `{"embeddings": [[...]]}`. The escape hatch for an
    /// endpoint that fits neither named dialect.
    Custom,
    /// Cohere `/v2/embed`: `{"texts": [...], "model", "input_type",
    /// "embedding_types": ["float"]}` → `{"embeddings": {"float": [[...]]}}`,
    /// with the v1 `{"embeddings": [[...]]}` shape accepted too.
    ///
    /// Not covered by `custom`: the request key is `texts` not `input`, and
    /// `input_type` is required for the v3+ models — omitting it embeds
    /// documents under the wrong role and quietly degrades recall. The worker
    /// only ever embeds documents, so it always sends `search_document`; a
    /// query embedded client-side must use `search_query`, which is a Cohere
    /// property callers meet outside this server.
    Cohere,
    /// Gemini `:batchEmbedContents`: `{"requests": [{"model", "content":
    /// {"parts": [{"text"}]}}]}` → `{"embeddings": [{"values": [...]}]}`.
    ///
    /// Not covered by `custom` in three ways at once: the request nests text
    /// under `content.parts`, the vectors come back under `values`, and auth
    /// is an `x-goog-api-key` header rather than a bearer token — which is why
    /// [`Auth`] exists.
    Gemini,
}

/// How a provider authenticates.
///
/// A bearer token covers OpenAI, Voyage, Cohere and Ollama-behind-a-proxy;
/// Gemini wants its key in a named header instead. Split out so a new
/// provider's auth is a variant here rather than an `if` in the send path.
enum Auth {
    None,
    Bearer(String),
    Header(&'static str, String),
}

/// A provider that calls an HTTP endpoint.
pub struct HttpProvider {
    endpoint: String,
    model: String,
    dialect: Dialect,
    auth: Auth,
    dim: usize,
    /// Shared across every call this provider makes; see [`outbound`].
    client: Outbound,
    /// The OpenAI `dimensions` request field, when the configuration asks
    /// for a width other than the model's native one.
    dimensions: Option<usize>,
}

impl HttpProvider {
    fn openai(
        base: String,
        model: String,
        key_env: String,
        dim: usize,
        dimensions: Option<usize>,
        client: Outbound,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: openai_url(&base),
            model,
            dialect: Dialect::OpenAi,
            auth: Auth::Bearer(read_key(&key_env)?),
            dim,
            client,
            dimensions,
        })
    }

    fn ollama(endpoint: String, model: String, dim: usize, client: Outbound) -> Self {
        Self {
            endpoint: format!("{}/api/embeddings", endpoint.trim_end_matches('/')),
            model,
            dialect: Dialect::Ollama,
            auth: Auth::None,
            dim,
            client,
            dimensions: None,
        }
    }

    fn custom(
        endpoint: String,
        key_env: Option<String>,
        dim: usize,
        client: Outbound,
    ) -> Result<Self> {
        let auth = match key_env {
            Some(var) => Auth::Bearer(read_key(&var)?),
            None => Auth::None,
        };
        Ok(Self {
            endpoint,
            model: String::new(),
            dialect: Dialect::Custom,
            auth,
            dim,
            client,
            dimensions: None,
        })
    }

    fn cohere(
        base: String,
        model: String,
        key_env: String,
        dim: usize,
        client: Outbound,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: format!("{}/v2/embed", base.trim_end_matches('/')),
            model,
            dialect: Dialect::Cohere,
            auth: Auth::Bearer(read_key(&key_env)?),
            dim,
            client,
            dimensions: None,
        })
    }

    fn gemini(
        base: String,
        model: String,
        key_env: String,
        dim: usize,
        client: Outbound,
    ) -> Result<Self> {
        Ok(Self::gemini_with_key(&base, &model, read_key(&key_env)?, dim, client))
    }

    /// The Gemini shape with the key already in hand. Split from [`Self::gemini`]
    /// so a test can build one without touching the environment.
    fn gemini_with_key(base: &str, model: &str, key: String, dim: usize, client: Outbound) -> Self {
        // The model rides both the URL and the request body; the URL wants it
        // bare, the body wants a `models/` prefix. Stored bare.
        let bare = model.strip_prefix("models/").unwrap_or(model);
        Self {
            endpoint: format!(
                "{}/v1beta/models/{bare}:batchEmbedContents",
                base.trim_end_matches('/')
            ),
            model: bare.to_string(),
            dialect: Dialect::Gemini,
            // Gemini reads the key from a header, not a bearer token.
            auth: Auth::Header("x-goog-api-key", key),
            dim,
            client,
            dimensions: None,
        }
    }

    /// Check, before a call, every address the endpoint's host resolves to.
    ///
    /// Before each call rather than once when the provider was built, as a
    /// webhook's host is before each delivery: a name that resolved publicly
    /// then can resolve inward now. And kept beside the client's own resolver,
    /// because behind a proxy that resolver sees only the proxy's name.
    ///
    /// The lookup runs on the blocking pool under [`CONNECT_TIMEOUT`]. A host
    /// that does not resolve, or whose lookup does not answer in time, is a
    /// connect failure, retried on the worker's clock like any other; an
    /// address the policy refuses is the permanent refusal it always was.
    async fn check_destination(&self) -> Result<()> {
        match tokio::time::timeout(CONNECT_TIMEOUT, self.client.egress.check_async(&self.endpoint))
            .await
        {
            Ok(checked) => checked.map_err(|e| refused(PolicyError::Endpoint(e), self.name())),
            Err(_) => Err(VectorError::Transport {
                provider: self.name(),
                kind: TransportKind::Connect,
                detail: format!(
                    "the endpoint's host did not resolve within the {} s connect timeout",
                    CONNECT_TIMEOUT.as_secs()
                ),
            }),
        }
    }

    /// The request body for a batch, in this provider's dialect.
    fn request_body(&self, texts: &[String]) -> serde_json::Value {
        match self.dialect {
            Dialect::OpenAi => {
                let mut body = serde_json::json!({ "input": texts, "model": self.model });
                if let Some(d) = self.dimensions {
                    body["dimensions"] = serde_json::json!(d);
                }
                body
            }
            // Ollama embeds one text per call, so a batch is sent as separate
            // requests; this builds the body for a single one.
            Dialect::Ollama => {
                serde_json::json!({ "prompt": texts.first(), "model": self.model })
            }
            Dialect::Custom => serde_json::json!({ "input": texts }),
            // `search_document` because the server only embeds documents;
            // Cohere's asymmetric models want `search_query` for queries,
            // which are embedded client-side and arrive here as raw vectors.
            Dialect::Cohere => serde_json::json!({
                "texts": texts,
                "model": self.model,
                "input_type": "search_document",
                "embedding_types": ["float"],
            }),
            Dialect::Gemini => serde_json::json!({
                "requests": texts.iter().map(|text| serde_json::json!({
                    "model": format!("models/{}", self.model),
                    "content": { "parts": [ { "text": text } ] },
                })).collect::<Vec<_>>(),
            }),
        }
    }

    /// Pull vectors out of a response body, in this provider's dialect.
    fn parse_response(&self, body: &serde_json::Value) -> Result<Vec<Vec<f32>>> {
        let malformed = || VectorError::MalformedResponse {
            provider: self.name(),
            detail: format!("unexpected shape: {body}"),
        };

        let rows: Vec<Vec<f32>> = match self.dialect {
            Dialect::OpenAi => body
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(malformed)?
                .iter()
                .map(|row| numbers(row.get("embedding")))
                .collect::<Option<_>>()
                .ok_or_else(malformed)?,
            Dialect::Ollama => {
                vec![numbers(body.get("embedding")).ok_or_else(malformed)?]
            }
            Dialect::Custom => body
                .get("embeddings")
                .and_then(|e| e.as_array())
                .ok_or_else(malformed)?
                .iter()
                .map(|row| numbers(Some(row)))
                .collect::<Option<_>>()
                .ok_or_else(malformed)?,
            // v2 nests the rows under `embeddings.float`; v1 puts the array
            // directly under `embeddings`. Accept either, so a user on either
            // API version — and an account migrated between them — works.
            Dialect::Cohere => {
                let rows = match body.get("embeddings") {
                    Some(serde_json::Value::Object(map)) => map.get("float"),
                    other => other,
                };
                rows.and_then(|e| e.as_array())
                    .ok_or_else(malformed)?
                    .iter()
                    .map(|row| numbers(Some(row)))
                    .collect::<Option<_>>()
                    .ok_or_else(malformed)?
            }
            Dialect::Gemini => body
                .get("embeddings")
                .and_then(|e| e.as_array())
                .ok_or_else(malformed)?
                .iter()
                .map(|row| numbers(row.get("values")))
                .collect::<Option<_>>()
                .ok_or_else(malformed)?,
        };

        // A wrong width silently corrupts an index whose other vectors are a
        // different size, so it is checked here rather than discovered later.
        for row in &rows {
            if row.len() != self.dim {
                return Err(VectorError::DimensionMismatch {
                    expected: self.dim,
                    found: row.len(),
                });
            }
        }
        Ok(rows)
    }
}

fn numbers(value: Option<&serde_json::Value>) -> Option<Vec<f32>> {
    value?.as_array()?.iter().map(|n| n.as_f64().map(|f| f as f32)).collect()
}

/// Read an API key from the environment.
///
/// Keys live in the environment, never in collection metadata, which is
/// readable by anyone who can read the data directory. Only reached for a
/// variable the policy has already admitted: [`build`] checks the name before
/// anything looks it up.
fn read_key(var: &str) -> Result<String> {
    std::env::var(var).map_err(|_| VectorError::MissingApiKey { var: var.to_string() })
}

/// A provider call in progress, as the class cell of the task that made it sees
/// it (ADR-213): the class is `Remote` for as long as this lives, **re-stamped
/// before each attempt** (the check of the host, each send, the in-client retry,
/// each of Ollama's per-text requests) so the bound is one attempt's own timeout
/// and never the whole call's. Dropped however the call ends, an error or a
/// cancellation included, which puts the class back to `Local` with a beat.
struct RemoteCall;

impl RemoteCall {
    fn begin() -> Self {
        kimmy_storage::class_step::remote_attempt();
        Self
    }

    /// Another attempt of the same call begins.
    fn attempt(&self) {
        kimmy_storage::class_step::remote_attempt();
    }
}

impl Drop for RemoteCall {
    fn drop(&mut self) {
        kimmy_storage::class_step::remote_done();
    }
}

#[async_trait]
impl EmbeddingProvider for HttpProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let remote = RemoteCall::begin();
        self.check_destination().await?;

        // Ollama takes one text per request; the others take the whole batch.
        let batches: Vec<&[String]> = match self.dialect {
            Dialect::Ollama => texts.iter().map(std::slice::from_ref).collect(),
            _ => vec![texts],
        };

        let mut out = Vec::with_capacity(texts.len());

        for batch in batches {
            let body = self.request_body(batch);
            // One retry, inside the client, for a transport failure only: the
            // common case is a pooled connection the far side closed, which
            // the next attempt simply does not reuse. A rejected request (4xx,
            // 5xx) is returned as is; the worker decides whether to retry
            // those, on its own longer clock.
            let mut attempt = 0;
            let response = loop {
                attempt += 1;
                let mut request = self.client.http.post(&self.endpoint).json(&body);
                request = match &self.auth {
                    Auth::None => request,
                    Auth::Bearer(key) => request.bearer_auth(key),
                    Auth::Header(name, key) => request.header(*name, key),
                };
                remote.attempt();
                match request.send().await {
                    Ok(response) => break response,
                    Err(e) => {
                        let (kind, detail) = describe(&e);
                        if attempt >= 2 {
                            return Err(VectorError::Transport {
                                provider: self.name(),
                                kind,
                                detail,
                            });
                        }
                        tracing::debug!(provider = self.name(), %kind, %detail, "provider request failed; retrying once");
                        // A little jitter so a drain of deferred documents does
                        // not retry in lockstep.
                        let jitter = Duration::from_millis(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| u64::from(d.subsec_millis() % 250))
                                .unwrap_or(0),
                        );
                        tokio::time::sleep(RETRY_PAUSE + jitter).await;
                    }
                }
            };

            let status = response.status();
            if !status.is_success() {
                // The body often explains the failure (bad model, quota); the
                // status alone rarely does.
                let detail = response.text().await.unwrap_or_default();
                return Err(VectorError::ProviderRejected {
                    provider: self.name(),
                    status: status.as_u16(),
                    detail: detail.chars().take(300).collect(),
                });
            }

            let body: serde_json::Value = response.json().await.map_err(|e| {
                VectorError::MalformedResponse { provider: self.name(), detail: e.to_string() }
            })?;
            out.extend(self.parse_response(&body)?);
            PROVIDER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            PROVIDER_TOKENS.fetch_add(billed_tokens(self.dialect, &body), Ordering::Relaxed);
        }

        if out.len() != texts.len() {
            return Err(VectorError::MalformedResponse {
                provider: self.name(),
                detail: format!("expected {} vectors, got {}", texts.len(), out.len()),
            });
        }
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn name(&self) -> &'static str {
        match self.dialect {
            Dialect::OpenAi => "openai",
            Dialect::Ollama => "ollama",
            Dialect::Custom => "custom_http",
            Dialect::Cohere => "cohere",
            Dialect::Gemini => "gemini",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(dialect: Dialect, dim: usize) -> HttpProvider {
        HttpProvider {
            endpoint: "http://example.invalid".into(),
            model: "m".into(),
            dialect,
            auth: Auth::None,
            dim,
            client: outbound(&ProviderPolicy::default()),
            dimensions: None,
        }
    }

    /// A policy that admits the loopback endpoints these tests listen on.
    fn loopback_policy() -> ProviderPolicy {
        ProviderPolicy::new(
            crate::policy::default_allowed_key_env(),
            vec!["127.0.0.1".into(), "localhost".into()],
            false,
            Default::default(),
        )
        .unwrap()
    }

    /// A provider that accepts the connection and never answers (a provider that hangs),
    /// against the class cell of the task that called it, on paused time. **The class
    /// is `Remote` for the whole call and the phase is re-stamped per attempt**: no
    /// sample ever reads more than one attempt's timeout (plus a margin) since the
    /// phase began, though the call lasts two of them, so `ATTEMPT_TIMEOUT + 15 s`
    /// is a real bound and a hung provider never makes the class overdue. When the
    /// call ends, however it ends, the class is `Local` again.
    #[tokio::test(flavor = "current_thread")]
    async fn work_held_as_remote_reads_remote_inside_and_local_again_after() {
        use kimmy_storage::class_step::{self, ClassCell, MonotonicClock, Phase};
        let cell = ClassCell::leak(std::sync::Arc::new(MonotonicClock::new()));
        class_step::scope(cell, async {
            class_step::phase(Phase::Local);
            assert_eq!(cell.phase(), Phase::Local, "control");
            let inside = held_as_remote(|| cell.phase());
            assert_eq!(inside, Phase::Remote);
            assert_eq!(cell.phase(), Phase::Local, "back to local with a beat");
            // However the work ends: a panic inside it unwinds through the guard.
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                held_as_remote(|| -> () { panic!("model load failed") })
            }));
            assert!(caught.is_err());
            assert_eq!(cell.phase(), Phase::Local, "put back on the way out of a panic");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_provider_is_remote_for_one_attempt_at_a_time_and_local_again_after() {
        use kimmy_storage::class_step::{self, ClassCell, Phase, StepClock};
        struct Paused(tokio::time::Instant);
        impl StepClock for Paused {
            fn now_ms(&self) -> u64 {
                self.0.elapsed().as_millis() as u64
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let held = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let keep = std::sync::Arc::clone(&held);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                keep.lock().unwrap().push(stream);
            }
        });
        let p = HttpProvider {
            endpoint: format!("http://{addr}/embed"),
            model: String::new(),
            dialect: Dialect::Custom,
            auth: Auth::None,
            dim: 2,
            client: outbound(&loopback_policy()),
            dimensions: None,
        };
        let cell = ClassCell::leak(std::sync::Arc::new(Paused(tokio::time::Instant::now())));
        let call = tokio::spawn(class_step::scope(cell, async move {
            class_step::phase(Phase::Local);
            let outcome = p.embed(&["x".to_string()]).await;
            (outcome.is_err(), cell.reading().phase)
        }));
        let mut longest = Duration::ZERO;
        let mut samples = 0;
        let started = tokio::time::Instant::now();
        while !call.is_finished() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let r = cell.reading();
            if r.phase == Phase::Remote {
                longest = longest.max(r.since_phase);
                samples += 1;
            }
        }
        let (failed, after) = call.await.unwrap();
        assert!(failed, "the hung provider's call fails");
        assert_eq!(after, Phase::Local, "the attempt ended: not Remote any more");
        assert!(
            started.elapsed() > REQUEST_TIMEOUT * 2,
            "premise: the call ran two attempts' worth, {:?}",
            started.elapsed()
        );
        assert!(samples > 100, "premise: it was Remote throughout, {samples} samples");
        assert!(
            longest <= REQUEST_TIMEOUT + Duration::from_secs(2),
            "the phase is stamped per attempt, never per call: {longest:?}"
        );
    }

    /// Ollama takes one text per request: 32 texts answered 5 s each is one call of 160 s
    /// and 32 attempts of 5 s, and the phase is stamped per text, so the class is
    /// never anywhere near its bound while a slow but answering provider works.
    #[tokio::test(start_paused = true)]
    async fn a_slow_ollama_is_remote_one_text_at_a_time() {
        use kimmy_storage::class_step::{self, ClassCell, Phase, StepClock};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        struct Paused(tokio::time::Instant);
        impl StepClock for Paused {
            fn now_ms(&self) -> u64 {
                self.0.elapsed().as_millis() as u64
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let body = r#"{"embedding":[1.0,2.0]}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        let p = HttpProvider {
            endpoint: format!("http://{addr}/api/embeddings"),
            model: "m".into(),
            dialect: Dialect::Ollama,
            auth: Auth::None,
            dim: 2,
            client: outbound(&loopback_policy()),
            dimensions: None,
        };
        let cell = ClassCell::leak(std::sync::Arc::new(Paused(tokio::time::Instant::now())));
        let texts: Vec<String> = (0..32).map(|i| format!("text {i}")).collect();
        let call = tokio::spawn(class_step::scope(cell, async move {
            class_step::phase(Phase::Local);
            p.embed(&texts).await.map(|v| v.len())
        }));
        let mut longest = Duration::ZERO;
        let started = tokio::time::Instant::now();
        while !call.is_finished() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let r = cell.reading();
            if call.is_finished() {
                break;
            }
            assert_eq!(r.phase, Phase::Remote, "{r:?}");
            longest = longest.max(r.since_phase);
        }
        assert_eq!(call.await.unwrap().unwrap(), 32, "the call answered all 32");
        assert!(started.elapsed() >= Duration::from_secs(150), "premise: one long call");
        assert!(
            longest <= Duration::from_secs(15),
            "stamped per text, not per call of 160 s: {longest:?}"
        );
    }

    /// The far side closing a connection before answering is the failure a
    /// pooled client meets after an idle period. One retry inside `embed`
    /// covers it; a second failure is reported with its kind and cause.
    #[tokio::test]
    async fn a_reset_connection_is_retried_once_and_the_cause_is_named() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Connection 1: dropped unanswered. Connection 2: a real response.
        tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = second.read(&mut buf).await;
            let body = r#"{"embeddings":[[1.0,2.0]]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            second.write_all(response.as_bytes()).await.unwrap();
        });
        let p = HttpProvider {
            endpoint: format!("http://{addr}/embed"),
            model: String::new(),
            dialect: Dialect::Custom,
            auth: Auth::None,
            dim: 2,
            client: outbound(&loopback_policy()),
            dimensions: None,
        };
        let out = p.embed(&["a".to_string()]).await.unwrap();
        assert_eq!(out, vec![vec![1.0, 2.0]]);

        // Nothing listening: both attempts fail, and the error says why.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        let p = HttpProvider { endpoint: format!("http://{dead_addr}/embed"), ..p };
        match p.embed(&["a".to_string()]).await {
            Err(VectorError::Transport { kind, detail, .. }) => {
                assert_eq!(kind, TransportKind::Connect, "{detail}");
                assert!(detail.contains("refused") || detail.contains("connect"), "{detail}");
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    #[test]
    fn billed_tokens_are_read_per_dialect_and_absent_is_zero() {
        let openai =
            serde_json::json!({ "data": [], "usage": { "prompt_tokens": 17, "total_tokens": 17 } });
        assert_eq!(billed_tokens(Dialect::OpenAi, &openai), 17);
        let cohere = serde_json::json!({ "meta": { "billed_units": { "input_tokens": 23 } } });
        assert_eq!(billed_tokens(Dialect::Cohere, &cohere), 23);
        let ollama = serde_json::json!({ "embedding": [], "prompt_eval_count": 5 });
        assert_eq!(billed_tokens(Dialect::Ollama, &ollama), 5);
        assert_eq!(billed_tokens(Dialect::OpenAi, &serde_json::json!({ "data": [] })), 0);
        assert_eq!(billed_tokens(Dialect::Gemini, &openai), 0);
    }

    #[test]
    fn an_openai_endpoint_that_names_the_route_is_used_verbatim() {
        assert_eq!(openai_url("https://api.openai.com"), "https://api.openai.com/v1/embeddings");
        assert_eq!(openai_url("http://llama-embed:5301/"), "http://llama-embed:5301/v1/embeddings");
        assert_eq!(
            openai_url("https://api.deepinfra.com/v1/openai/embeddings"),
            "https://api.deepinfra.com/v1/openai/embeddings"
        );
        assert_eq!(
            openai_url(
                "https://x.openai.azure.com/openai/deployments/e/embeddings?api-version=2024-02-01"
            ),
            "https://x.openai.azure.com/openai/deployments/e/embeddings?api-version=2024-02-01"
        );
        // A trailing slash after the route is still the route.
        assert_eq!(openai_url("https://h/v1/embeddings/"), "https://h/v1/embeddings/");
    }

    #[test]
    fn byo_has_no_provider_to_build() {
        // The client supplies vectors, so there is nothing to call.
        assert!(matches!(
            build(&ProviderConfig::Byo {}, 8, &ProviderPolicy::default()).err(),
            Some(VectorError::NoProvider)
        ));
    }

    #[test]
    fn request_bodies_match_each_dialect() {
        let texts = vec!["a".to_string(), "b".to_string()];

        let openai = provider(Dialect::OpenAi, 2).request_body(&texts);
        assert_eq!(openai["input"], serde_json::json!(["a", "b"]));
        assert_eq!(openai["model"], "m");
        assert!(openai.get("dimensions").is_none(), "absent unless configured");
        let mut narrow = provider(Dialect::OpenAi, 2);
        narrow.dimensions = Some(256);
        assert_eq!(narrow.request_body(&texts)["dimensions"], 256);

        // Ollama takes a single prompt per request.
        let ollama = provider(Dialect::Ollama, 2).request_body(&texts[..1]);
        assert_eq!(ollama["prompt"], "a");

        let custom = provider(Dialect::Custom, 2).request_body(&texts);
        assert_eq!(custom["input"], serde_json::json!(["a", "b"]));

        // Cohere: `texts`, not `input`, and `input_type` is mandatory for the
        // v3+ models — a request without it embeds under the wrong role.
        let cohere = provider(Dialect::Cohere, 2).request_body(&texts);
        assert_eq!(cohere["texts"], serde_json::json!(["a", "b"]));
        assert_eq!(cohere["input_type"], "search_document");
        assert_eq!(cohere["embedding_types"], serde_json::json!(["float"]));

        // Gemini: text nests under content.parts, and the model carries a
        // `models/` prefix in the body even though the URL wants it bare.
        let gemini = provider(Dialect::Gemini, 2).request_body(&texts);
        assert_eq!(gemini["requests"][0]["content"]["parts"][0]["text"], "a");
        assert_eq!(gemini["requests"][1]["content"]["parts"][0]["text"], "b");
        assert_eq!(gemini["requests"][0]["model"], "models/m");
    }

    #[test]
    fn responses_parse_in_each_dialect() {
        let openai = serde_json::json!({
            "data": [ { "embedding": [1.0, 2.0] }, { "embedding": [3.0, 4.0] } ]
        });
        assert_eq!(
            provider(Dialect::OpenAi, 2).parse_response(&openai).unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );

        let ollama = serde_json::json!({ "embedding": [1.0, 2.0] });
        assert_eq!(
            provider(Dialect::Ollama, 2).parse_response(&ollama).unwrap(),
            vec![vec![1.0, 2.0]]
        );

        let custom = serde_json::json!({ "embeddings": [[1.0, 2.0]] });
        assert_eq!(
            provider(Dialect::Custom, 2).parse_response(&custom).unwrap(),
            vec![vec![1.0, 2.0]]
        );

        // Cohere v2 nests under `embeddings.float`...
        let cohere_v2 = serde_json::json!({ "embeddings": { "float": [[1.0, 2.0], [3.0, 4.0]] } });
        assert_eq!(
            provider(Dialect::Cohere, 2).parse_response(&cohere_v2).unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );
        // ...and the v1 flat shape must still parse, so a user on either API
        // version works.
        let cohere_v1 = serde_json::json!({ "embeddings": [[1.0, 2.0]] });
        assert_eq!(
            provider(Dialect::Cohere, 2).parse_response(&cohere_v1).unwrap(),
            vec![vec![1.0, 2.0]]
        );

        // Gemini returns each vector under `values`.
        let gemini = serde_json::json!({
            "embeddings": [ { "values": [1.0, 2.0] }, { "values": [3.0, 4.0] } ]
        });
        assert_eq!(
            provider(Dialect::Gemini, 2).parse_response(&gemini).unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );
    }

    #[test]
    fn voyage_is_the_openai_dialect() {
        // The audit's Voyage finding, pinned: Voyage is OpenAI-compatible, so
        // it is configured as `openai` with a Voyage endpoint — no separate
        // dialect. If Voyage ever diverges, this is where it shows.
        let config = ProviderConfig::OpenAi {
            model: "voyage-3".into(),
            endpoint: Some("https://api.voyageai.com".into()),
            api_key_env: "VOYAGE_API_KEY".into(),
            dimensions: None,
        };
        assert_eq!(config.name(), "openai", "voyage is configured as the openai provider");

        // The request Voyage receives is OpenAI's, which the dialect already
        // emits, and its response is OpenAI's, which the dialect already
        // parses — so a Voyage endpoint needs nothing new.
        let dialect = provider(Dialect::OpenAi, 2);
        assert_eq!(dialect.request_body(&["a".into()])["input"], serde_json::json!(["a"]));
        let voyage_response = serde_json::json!({ "data": [ { "embedding": [1.0, 2.0] } ] });
        assert_eq!(dialect.parse_response(&voyage_response).unwrap(), vec![vec![1.0, 2.0]]);
    }

    #[test]
    fn gemini_authenticates_by_header_and_puts_the_model_in_the_url() {
        // The reason Auth exists: a bearer token would never authenticate a
        // Gemini call. Constructed with a literal key so the test does not
        // mutate the process environment.
        let p = HttpProvider::gemini_with_key(
            "https://generativelanguage.googleapis.com",
            "models/text-embedding-004",
            "k".into(),
            768,
            outbound(&ProviderPolicy::default()),
        );
        assert!(matches!(p.auth, Auth::Header("x-goog-api-key", _)));
        // The `models/` prefix is stripped for the URL — bare there — and the
        // URL names the batch method.
        assert_eq!(p.model, "text-embedding-004");
        assert!(
            p.endpoint.ends_with("/models/text-embedding-004:batchEmbedContents"),
            "{}",
            p.endpoint
        );
    }

    #[test]
    fn a_wrong_width_is_rejected_rather_than_stored() {
        // Mixing widths in one index is meaningless, and the failure would
        // otherwise surface far from its cause.
        let body = serde_json::json!({ "embeddings": [[1.0, 2.0, 3.0]] });
        let err = provider(Dialect::Custom, 2).parse_response(&body).unwrap_err();
        assert!(matches!(err, VectorError::DimensionMismatch { expected: 2, found: 3 }));
    }

    #[test]
    fn malformed_responses_are_reported_not_silently_empty() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({ "data": "not an array" }),
            serde_json::json!({ "embeddings": [["not", "numbers"]] }),
        ] {
            assert!(
                provider(Dialect::Custom, 2).parse_response(&body).is_err()
                    || provider(Dialect::OpenAi, 2).parse_response(&body).is_err(),
                "should have rejected {body}"
            );
        }
    }

    #[tokio::test]
    async fn an_empty_batch_makes_no_request() {
        // Otherwise every document with no embeddable text costs a round trip.
        let result = provider(Dialect::Custom, 2).embed(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn a_missing_api_key_is_reported_by_variable_name() {
        // Naming the variable is what makes this fixable without reading source.
        // A `KIMMY_PROVIDER_*` name, so the policy admits it and the only
        // thing left to fail is the lookup.
        let config = ProviderConfig::OpenAi {
            model: "text-embedding-3-small".into(),
            endpoint: None,
            api_key_env: "KIMMY_PROVIDER_TEST_KEY_DEFINITELY_UNSET".into(),
            dimensions: None,
        };
        let Err(err) = build(&config, 1536, &ProviderPolicy::default()) else {
            panic!("an unset key variable should not build a provider");
        };
        assert!(matches!(err, VectorError::MissingApiKey { ref var } if var.contains("UNSET")));
        assert!(err.to_string().contains("KIMMY_PROVIDER_TEST_KEY_DEFINITELY_UNSET"));
    }

    #[test]
    fn a_configuration_naming_a_node_secret_is_refused_before_the_variable_is_read() {
        // The finding, at the layer replication reaches: a stored
        // configuration that never passed this node's API. The variable is
        // set here so that, were the policy consulted after the lookup, the
        // provider would build — the refusal has to come first. Nothing
        // reads the value; the error carries the name and nothing else.
        //
        // `set_var` is unsafe on this edition because another thread could
        // be reading the environment. The value is a throwaway, the name is
        // unique to this test, and nothing else in the crate looks it up.
        const VAR: &str = "KIMMY_TEST_NODE_SECRET_FOR_POLICY";
        unsafe { std::env::set_var(VAR, "not-a-real-secret") };
        let config = ProviderConfig::OpenAi {
            model: "m".into(),
            endpoint: Some("https://93.184.216.34".into()),
            api_key_env: VAR.into(),
            dimensions: None,
        };
        let err = build(&config, 8, &ProviderPolicy::default()).err().expect("refused");
        assert!(matches!(err, VectorError::PolicyRefused(_)), "{err:?}");
        assert!(!err.is_retryable(), "a policy refusal is permanent");
        let text = err.to_string();
        assert!(text.contains(VAR), "{text}");
        assert!(!text.contains("not-a-real-secret"), "the value must never appear: {text}");
        unsafe { std::env::remove_var(VAR) };

        // An unlisted name and a private endpoint are refused the same way.
        let unlisted = ProviderConfig::CustomHttp {
            endpoint: "https://93.184.216.34/embed".into(),
            api_key_env: Some("SOMEBODY_ELSES_KEY".into()),
        };
        let err = build(&unlisted, 8, &ProviderPolicy::default()).err().expect("refused");
        assert!(
            matches!(err, VectorError::PolicyRefused(ref m) if m.contains("SOMEBODY_ELSES_KEY")),
            "{err:?}"
        );
        let private =
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://10.0.0.5:11434".into() };
        let err = build(&private, 8, &ProviderPolicy::default()).err().expect("refused");
        assert!(
            matches!(err, VectorError::PolicyRefused(ref m) if m.contains("10.0.0.5")),
            "{err:?}"
        );
        // ...and admitted once the operator lists the host.
        let lan = ProviderPolicy::new(
            crate::policy::default_allowed_key_env(),
            vec!["10.0.0.5".into()],
            false,
            Default::default(),
        )
        .unwrap();
        build(&private, 8, &lan).expect("an allowed host builds");
    }

    #[test]
    fn a_profile_builds_the_operators_provider_and_a_missing_one_fails_permanently() {
        let mut profiles = std::collections::BTreeMap::new();
        profiles.insert(
            "lan".to_string(),
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://10.0.0.5:11434".into() },
        );
        let policy = ProviderPolicy::new(
            crate::policy::default_allowed_key_env(),
            vec!["10.0.0.5".into()],
            false,
            profiles,
        )
        .unwrap();
        let built = build(&ProviderConfig::Profile { name: "lan".into() }, 8, &policy).unwrap();
        assert_eq!(built.name(), "ollama", "the profile's dialect, not \"profile\"");
        assert_eq!(built.dim(), 8);

        let err = build(&ProviderConfig::Profile { name: "nope".into() }, 8, &policy)
            .err()
            .expect("refused");
        assert!(
            matches!(err, VectorError::UnknownProfile { ref name } if name == "nope"),
            "{err:?}"
        );
        assert!(!err.is_retryable(), "a missing profile is a configuration, not a blip");
        assert!(err.to_string().contains("vector.providers.nope"), "{err}");
    }

    #[test]
    fn endpoints_are_built_without_double_slashes() {
        let p = HttpProvider::ollama(
            "http://localhost:11434/".into(),
            "m".into(),
            8,
            outbound(&ProviderPolicy::default()),
        );
        assert_eq!(p.endpoint, "http://localhost:11434/api/embeddings");
    }

    #[cfg(not(feature = "local-embeddings"))]
    #[test]
    fn the_local_provider_is_unavailable_without_the_feature() {
        let config = ProviderConfig::Local { model: "bge-small-en-v1.5".into() };
        assert!(matches!(
            build(&config, 384, &ProviderPolicy::default()).err(),
            Some(VectorError::LocalUnavailable)
        ));
    }
}
