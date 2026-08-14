//! Change streams, and surviving the socket dropping.
//!
//! A change stream is a WebSocket that stays open for as long as the
//! application wants events — which is longer than networks stay up. So the
//! interesting part of this file is not opening one; it is what happens after
//! it closes.
//!
//! **Resume tokens are portable across nodes**, verified on a real cluster, so
//! a reconnect may land somewhere else and continue correctly. That is what
//! makes automatic reconnection safe here where it would not be in a system
//! whose cursors belong to a session on one machine.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, client_async};

use crate::{Client, Error, Result};

/// How a change stream starts.
#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    resume_after: Option<String>,
    from_start: bool,
    full_document: bool,
}

impl WatchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resume immediately after a token from a previous stream.
    pub fn resume_after(mut self, token: impl Into<String>) -> Self {
        self.resume_after = Some(token.into());
        self
    }

    /// Replay from the beginning of the retained oplog.
    ///
    /// Named for what it does rather than for the wire parameter it sets
    /// (`from_start`): a `from_*` method on a builder reads as a constructor.
    pub fn replay_from_start(mut self, replay: bool) -> Self {
        self.from_start = replay;
        self
    }

    /// Include the whole document on every event, where there is one.
    pub fn full_document(mut self, full: bool) -> Self {
        self.full_document = full;
        self
    }

    fn query(&self, resume: Option<&str>) -> String {
        let mut parts = Vec::new();
        // A resume point learned while streaming wins over the configured one:
        // it is where this stream actually got to.
        if let Some(token) = resume.or(self.resume_after.as_deref()) {
            parts.push(format!("resume_after={token}"));
        } else if self.from_start {
            parts.push("from_start=true".to_string());
        }
        if self.full_document {
            parts.push("full_document=true".to_string());
        }
        if parts.is_empty() { String::new() } else { format!("?{}", parts.join("&")) }
    }
}

/// One event from a collection.
#[derive(Clone, Debug)]
pub struct ChangeEvent {
    /// `insert`, `update`, `replace`, `delete`, `uniqueViolation`, `invalidate`.
    pub operation: String,
    /// Where to resume. Absent on `invalidate`, which cannot be resumed past.
    pub resume_token: Option<String>,
    /// The whole event, for everything the fields above do not name.
    pub raw: Value,
}

impl ChangeEvent {
    /// The changed document's `_id`, when the event has one.
    pub fn document_id(&self) -> Option<&Value> {
        self.raw.get("documentKey").and_then(|k| k.get("_id"))
    }

    /// The post-image, when `full_document` was asked for and the event
    /// carried one. **An oversized event drops it and still arrives**, so
    /// absence does not mean the document is gone.
    pub fn full_document(&self) -> Option<&Value> {
        self.raw.get("fullDocument")
    }

    /// Whether the stream cannot continue past this event.
    pub fn is_invalidate(&self) -> bool {
        self.operation == "invalidate"
    }
}

/// Whatever the socket turned out to be.
///
/// The WebSocket crate is used for framing only, with **no TLS feature**: its
/// TLS features select a rustls provider, and a feature-chosen provider is how
/// `aws-lc-rs` gets into a build that already pays for `ring` (ADR-050). So the
/// connection is made here and handed over as bytes.
trait Transport: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Transport for T {}

type Socket = WebSocketStream<Box<dyn Transport>>;

/// A live change stream that reconnects on its own.
pub struct ChangeStream {
    client: Client,
    db: String,
    collection: String,
    options: WatchOptions,
    socket: Option<Socket>,
    /// The last token seen, which is where a reconnect resumes.
    resume: Option<String>,
    /// Set by an `invalidate`: the stream is over and reconnecting would start
    /// a different one, silently.
    ended: bool,
}

impl ChangeStream {
    pub(crate) async fn open(
        client: Client,
        db: String,
        collection: String,
        options: WatchOptions,
    ) -> Result<Self> {
        let mut stream =
            Self { client, db, collection, options, socket: None, resume: None, ended: false };
        stream.connect().await?;
        Ok(stream)
    }

    /// The token this stream would resume from.
    ///
    /// Worth storing if the application will restart: it is portable, so the
    /// next run may hand it to a different node.
    pub fn resume_token(&self) -> Option<&str> {
        self.resume.as_deref()
    }

    /// The next event, reconnecting if the socket has dropped.
    ///
    /// `None` means the stream has ended for good — an `invalidate`, which is
    /// the collection going away. Everything else is retried.
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        loop {
            if self.ended {
                return Ok(None);
            }
            let Some(socket) = self.socket.as_mut() else {
                self.reconnect().await?;
                continue;
            };

            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let raw: Value = serde_json::from_str(&text)
                        .map_err(|e| Error::Stream(format!("an event was not JSON: {e}")))?;
                    let event = ChangeEvent {
                        operation: raw["operationType"].as_str().unwrap_or("").to_string(),
                        resume_token: raw["resumeToken"].as_str().map(str::to_string),
                        raw,
                    };
                    // Recorded *after* the event is built and before it is
                    // handed over, so a caller that drops the stream mid-event
                    // resumes at the last one it actually saw.
                    if let Some(token) = &event.resume_token {
                        self.resume = Some(token.clone());
                    }
                    if event.is_invalidate() {
                        self.ended = true;
                    }
                    return Ok(Some(event));
                }
                // Anything else on the wire is not an event: a ping, a close,
                // or a broken socket. All three mean "reconnect and resume".
                Some(Ok(Message::Close(_))) | None => {
                    self.socket = None;
                    self.reconnect().await?;
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) => {
                    self.socket = None;
                    self.reconnect().await?;
                }
            }
        }
    }

    /// Reconnect, backing off, resuming from the last token seen.
    async fn reconnect(&mut self) -> Result<()> {
        const ATTEMPTS: u32 = 5;
        let mut delay = Duration::from_millis(100);

        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            match self.connect().await {
                Ok(()) => return Ok(()),
                // A resume point past the retention horizon cannot be waited
                // out: retrying the same token loops forever, and the caller
                // has to decide what to do about the gap.
                Err(e) if e.code() == Some(crate::ErrorCode::ResumeTokenExpired) => {
                    self.ended = true;
                    return Err(e);
                }
                Err(_) if attempt + 1 < ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Open the socket against the current endpoint.
    async fn connect(&mut self) -> Result<()> {
        let endpoint = self.client.primary().await;
        let token = self.client.token().await.ok_or(Error::NotAuthenticated)?;
        let query = self.options.query(self.resume.as_deref());
        let path = format!("/v1/db/{}/coll/{}/watch{}", self.db, self.collection, query);

        let (host, port, tls) = split_endpoint(&endpoint)?;
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| Error::Stream(format!("connecting to {endpoint}: {e}")))?;

        // TLS is built here rather than selected by a feature on the WebSocket
        // crate, so the provider is the ring-backed one the workspace already
        // vets rather than whatever a feature flag chose (ADR-050).
        let stream: Box<dyn Transport> = if tls {
            let mut roots = tokio_rustls::rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = tokio_rustls::rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.clone())
                .map_err(|e| Error::Stream(format!("{host} is not a valid server name: {e}")))?;
            let connected = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
                .connect(name, stream)
                .await
                .map_err(|e| Error::Stream(format!("TLS to {endpoint}: {e}")))?;
            Box::new(connected)
        } else {
            Box::new(stream)
        };

        let scheme = if tls { "wss" } else { "ws" };
        let request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(format!("{scheme}://{host}:{port}{path}"))
            .header("Host", format!("{host}:{port}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .body(())
            .map_err(|e| Error::Stream(format!("building the upgrade request: {e}")))?;

        match client_async(request, stream).await {
            Ok((socket, _)) => {
                self.socket = Some(socket);
                Ok(())
            }
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                // The server refused before upgrading — which it does with the
                // ordinary error envelope, so the reason survives.
                let status = response.status().as_u16();
                let body = response
                    .body()
                    .as_ref()
                    .and_then(|b| serde_json::from_slice(b).ok())
                    .unwrap_or(Value::Null);
                Err(crate::error::from_response(status, None, &body))
            }
            Err(e) => Err(Error::Stream(format!("opening the change stream: {e}"))),
        }
    }

    /// Close the socket politely.
    pub async fn close(mut self) {
        if let Some(mut socket) = self.socket.take() {
            let _ = socket.send(Message::Close(None)).await;
        }
    }
}

/// `http://host:port` → `(host, port, tls)`.
fn split_endpoint(endpoint: &str) -> Result<(String, u16, bool)> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| Error::Stream(format!("{endpoint} has no scheme")))?;
    let tls = match scheme {
        "https" | "wss" => true,
        "http" | "ws" => false,
        other => return Err(Error::Stream(format!("cannot open a change stream over {other}"))),
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host.to_string(),
            port.parse().map_err(|_| Error::Stream(format!("{authority} has no valid port")))?,
        ),
        None => (authority.to_string(), if tls { 443 } else { 80 }),
    };
    Ok((host, port, tls))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_splits_into_something_dialable() {
        assert_eq!(
            split_endpoint("http://localhost:7878").unwrap(),
            ("localhost".into(), 7878, false)
        );
        assert_eq!(
            split_endpoint("https://db.example.com:8443").unwrap(),
            ("db.example.com".into(), 8443, true)
        );
        // The default port for the scheme, since a URL need not carry one.
        assert_eq!(split_endpoint("https://db.example.com").unwrap().1, 443);
        assert!(split_endpoint("db.example.com:7878").is_err(), "a scheme is required");
    }

    #[test]
    fn a_resume_token_learned_while_streaming_wins() {
        // Reconnecting has to continue where the *stream* got to, not where it
        // was told to start. Resuming from the configured point would replay
        // everything since, which for a stream that has been up for a day is
        // a day of events delivered twice.
        let options = WatchOptions::new().resume_after("configured").full_document(true);
        assert!(options.query(None).contains("resume_after=configured"));
        assert!(options.query(Some("live")).contains("resume_after=live"));
        assert!(!options.query(Some("live")).contains("configured"));
        assert!(options.query(None).contains("full_document=true"));
    }

    #[test]
    fn from_start_yields_to_a_resume_point() {
        // Both would be a contradiction: one says "everything retained", the
        // other says "after this". The resume point is the more specific.
        let options = WatchOptions::new().replay_from_start(true);
        assert!(options.query(None).contains("from_start=true"));
        let resumed = options.query(Some("token"));
        assert!(resumed.contains("resume_after=token"));
        assert!(!resumed.contains("from_start"));
    }
}
