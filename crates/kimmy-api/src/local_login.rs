//! Where a local token may be minted from (ADR-100).
//!
//! `POST /v1/auth/login` is the one unauthenticated route that accepts a guess
//! from anywhere and spends Argon2 work on each one. A node whose people all
//! arrive through an identity provider has no reason to leave it open to the
//! network — and every reason to keep it open to the host, because the
//! break-glass root that `admin` is reserved to (ADR-067) has to be able to log
//! in from somewhere. [`LocalLogin`] is that choice, and [`LocalMinting`] is
//! how a route says it is bound by it.
//!
//! # Minting, not verifying
//!
//! The mode is consulted by the two routes that *issue* a local token — login
//! and refresh — and by nothing that verifies one. A token already issued keeps
//! working under every mode, on every node, until it expires or is revoked;
//! switching modes ends nobody's session. Refresh is included because it mints
//! too: a mode that closed the front door and left the side door open would
//! let a session opened from the host be extended forever from anywhere.
//!
//! # The peer, not a header
//!
//! `loopback_only` judges the address the connection was accepted from, and
//! deliberately ignores `server.rate_limit.trusted_proxy_header`. The setting
//! is about who can reach the process; a forwarded header is something a
//! client writes, and the rate limiter already documents why trusting one
//! without a proxy that rewrites it is worse than nothing. The consequence is
//! stated rather than hidden: a reverse proxy on the same host connects from
//! loopback, so everything behind it looks local.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;

use crate::error::ApiError;
use crate::state::SharedState;

/// Where the password login answers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LocalLogin {
    /// Every caller. The default, and exactly what shipped.
    #[default]
    Always,
    /// Only a connection whose TCP peer address is loopback. Anyone else is
    /// refused with a 403 that names the setting.
    LoopbackOnly,
    /// Nobody: the route answers 404. Startup refuses this unless an identity
    /// provider is configured, because a node with neither could authenticate
    /// no one.
    Disabled,
}

impl LocalLogin {
    /// Parse the configured name.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "always" => Ok(LocalLogin::Always),
            "loopback_only" => Ok(LocalLogin::LoopbackOnly),
            "disabled" => Ok(LocalLogin::Disabled),
            other => Err(format!(
                "unknown local login mode {other:?}; expected always, loopback_only or disabled"
            )),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            LocalLogin::Always => "always",
            LocalLogin::LoopbackOnly => "loopback_only",
            LocalLogin::Disabled => "disabled",
        }
    }

    /// Whether a local token may be minted for a connection from `peer`.
    ///
    /// `None` is a connection the server could not attribute to a peer at all
    /// — a router served without connect info, which `kimmyd` never does. Under
    /// `loopback_only` that is refused: failing open would be the wrong
    /// direction for a setting whose only purpose is to close, and the
    /// rate-limit key for the same case already takes the strict reading.
    pub fn admit(self, peer: Option<SocketAddr>) -> Result<(), ApiError> {
        match self {
            LocalLogin::Always => Ok(()),
            LocalLogin::LoopbackOnly if peer.is_some_and(|p| p.ip().is_loopback()) => Ok(()),
            // 403 rather than 404, because the route exists and answers the
            // neighbour: a 404 here would be a puzzle, not a secret. The
            // `forbidden` code is the one a client already knows how to act
            // on, and the message says what to do instead.
            LocalLogin::LoopbackOnly => Err(ApiError::new(
                axum::http::StatusCode::FORBIDDEN,
                crate::error::ErrorCode::Forbidden,
                "local login is restricted to loopback connections on this node \
                 (auth.local.login = \"loopback_only\"); log in from the node's own host, or \
                 through the identity provider",
            )),
            // 404, because here the route really is absent from what this node
            // offers, and a client should stop trying it rather than retry
            // from somewhere else.
            LocalLogin::Disabled => Err(ApiError::not_found(
                "local login is disabled on this node (auth.local.login = \"disabled\"); \
                 authenticate through the identity provider",
            )),
        }
    }
}

/// Proof that the node's local login mode admits this connection.
///
/// An extractor rather than a call inside the handler, for the reason `Auth`
/// is one: a route that mints a local token takes this and is visibly bound
/// by the mode, and it runs **before** any extractor declared after it. That
/// ordering is what makes `disabled` a 404 for everyone — a refresh with no
/// token gets the mode's answer, not a 401 inviting the caller to fetch a
/// token by a route that does not exist for them.
pub struct LocalMinting;

impl FromRequestParts<SharedState> for LocalMinting {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &SharedState,
    ) -> Result<Self, Self::Rejection> {
        // The socket peer and only the socket peer. `ClientAddr` — the
        // rate-limit key — honours a trusted forwarded header; this does not,
        // and the module documentation says why.
        let peer = parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|ConnectInfo(a)| *a);
        state.local_login().admit(peer)?;
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(addr: &str) -> Option<SocketAddr> {
        Some(addr.parse().unwrap())
    }

    #[test]
    fn modes_parse_and_round_trip() {
        for name in ["always", "loopback_only", "disabled"] {
            let mode = LocalLogin::parse(name).unwrap();
            assert_eq!(mode.name(), name);
        }
        assert_eq!(LocalLogin::default(), LocalLogin::Always, "the default is what shipped");
    }

    #[test]
    fn an_unknown_mode_lists_the_valid_ones() {
        let err = LocalLogin::parse("localhost").unwrap_err();
        assert!(err.contains("localhost"), "{err}");
        assert!(err.contains("loopback_only"), "the error should say what is valid: {err}");
    }

    #[test]
    fn always_admits_everyone_including_an_unattributed_connection() {
        for peer in [peer("127.0.0.1:5000"), peer("203.0.113.9:5000"), None] {
            LocalLogin::Always.admit(peer).unwrap();
        }
    }

    #[test]
    fn loopback_only_admits_both_loopback_families_and_nothing_else() {
        LocalLogin::LoopbackOnly.admit(peer("127.0.0.1:5000")).unwrap();
        LocalLogin::LoopbackOnly.admit(peer("127.8.8.8:5000")).unwrap();
        LocalLogin::LoopbackOnly.admit(peer("[::1]:5000")).unwrap();

        // A private address is still not loopback: "on my network" is not
        // "on my host", and the setting means the second.
        for outside in ["10.0.0.5:5000", "192.168.0.10:5000", "203.0.113.9:5000", "[fe80::1]:5000"]
        {
            let err = LocalLogin::LoopbackOnly.admit(peer(outside)).unwrap_err();
            assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN, "{outside}");
            assert_eq!(err.code, crate::error::ErrorCode::Forbidden, "{outside}");
            assert!(err.message.contains("auth.local.login"), "name the setting: {}", err.message);
        }
    }

    #[test]
    fn loopback_only_fails_closed_when_the_peer_is_unknown() {
        // A router served without connect info cannot say where a connection
        // came from. The rate limiter treats that case strictly too.
        let err = LocalLogin::LoopbackOnly.admit(None).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn disabled_answers_not_found_to_everyone_including_loopback() {
        for peer in [peer("127.0.0.1:5000"), peer("[::1]:5000"), peer("203.0.113.9:5000"), None] {
            let err = LocalLogin::Disabled.admit(peer).unwrap_err();
            assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);
            assert_eq!(err.code, crate::error::ErrorCode::NotFound);
            assert!(err.message.contains("disabled"), "{}", err.message);
        }
    }
}
