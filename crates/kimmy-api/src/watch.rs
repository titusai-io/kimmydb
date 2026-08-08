//! The change-stream WebSocket endpoint.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::Response;
use kimmy_auth::Action;
use kimmy_core::{Hlc, ResumeToken};
use kimmy_storage::{ChangeEvent, WatchOptions, WatchScope};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::error::ApiError;
use crate::json::document_to_json;
use crate::state::{Auth, SharedState};

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct WatchQuery {
    /// Resume immediately after this token.
    pub resume_after: Option<String>,
    /// Replay from the beginning of the retained oplog.
    pub from_start: bool,
    /// Include the full post-image on every event.
    pub full_document: bool,
}

pub async fn watch_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Query(q): Query<WatchQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    // Authorize *before* upgrading. Once the socket is open the client has a
    // connection it can hold, and refusing after the handshake is both harder
    // to report and easy to get wrong.
    auth.require(Action::Watch, &db, Some(&coll))?;
    let meta = state.engine.get_collection(&db, &coll)?;

    let options = WatchOptions {
        resume_after: match &q.resume_after {
            Some(raw) => Some(ResumeToken::decode(raw).map_err(ApiError::from)?),
            None => None,
        },
        start_at: q.from_start.then_some(Hlc::ZERO),
    };

    // Opening the stream here rather than inside the upgrade callback means an
    // expired resume token is reported as a 410 rather than as an immediate,
    // unexplained socket close.
    let stream = state.engine.watch(WatchScope::Collection(meta.id), options)?;

    Ok(upgrade.on_upgrade(move |socket| pump(socket, state, stream, q.full_document)))
}

async fn pump(
    mut socket: WebSocket,
    state: SharedState,
    mut stream: kimmy_storage::ChangeStream,
    full_document: bool,
) {
    loop {
        tokio::select! {
            // Watch for the client going away, so a dropped connection does not
            // leave this task holding a stream forever.
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    Some(Ok(_)) => continue,
                }
            }
            event = stream.next(&state.engine) => {
                let Some(event) = event else { break };
                let payload = match render(&event, full_document) {
                    Some(p) => p,
                    None => continue,
                };
                if socket.send(Message::Text(payload.to_string().into())).await.is_err() {
                    break;
                }
                if matches!(event, ChangeEvent::Invalidate { .. }) {
                    break;
                }
            }
        }
    }
    debug!("change stream socket closed");
}

/// Render an event as the JSON a client sees.
fn render(event: &ChangeEvent, full_document: bool) -> Option<Value> {
    match event {
        ChangeEvent::Change { entry, token } => {
            // Collection create/drop entries have no document and would appear
            // as malformed document events.
            if entry.kind == kimmy_core::OpKind::Collection {
                return None;
            }
            // A violation is not a document change and has no documentKey; it
            // reports a constraint that a merge broke. See ADR-020.
            if entry.kind == kimmy_core::OpKind::UniqueViolation {
                let detail: Option<kimmy_core::UniqueViolationDetail> =
                    entry.body.as_ref().and_then(|b| bson::deserialize_from_slice(b).ok());
                let detail = detail?;
                return Some(json!({
                    "operationType": "uniqueViolation",
                    "resumeToken": token.encode(),
                    "clusterTime": entry.stamp.hlc.to_string(),
                    "index": detail.index,
                    "merged": crate::json::bson_to_json(&detail.merged.to_bson()),
                    "documentKeys": detail
                        .ids
                        .iter()
                        .map(|id| json!({ "_id": crate::json::bson_to_json(&id.to_bson()) }))
                        .collect::<Vec<_>>(),
                }));
            }
            let mut payload = json!({
                "operationType": operation_name(entry.kind),
                "resumeToken": token.encode(),
                "clusterTime": entry.stamp.hlc.to_string(),
            });
            if let Some(id) = &entry.doc_id {
                payload["documentKey"] = json!({
                    "_id": crate::json::bson_to_json(&id.to_bson())
                });
            }
            if full_document && let Ok(Some(doc)) = entry.document() {
                payload["fullDocument"] = document_to_json(&doc);
            }
            Some(payload)
        }
        ChangeEvent::Invalidate { reason } => Some(json!({
            "operationType": "invalidate",
            "reason": format!("{reason:?}"),
        })),
    }
}

fn operation_name(kind: kimmy_core::OpKind) -> &'static str {
    match kind {
        kimmy_core::OpKind::Insert => "insert",
        kimmy_core::OpKind::Update => "update",
        kimmy_core::OpKind::Replace => "replace",
        kimmy_core::OpKind::Delete => "delete",
        kimmy_core::OpKind::Collection => "collection",
        kimmy_core::OpKind::UniqueViolation => "uniqueViolation",
    }
}
