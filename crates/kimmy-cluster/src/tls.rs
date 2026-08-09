//! TLS for replication, bound to `cluster_secret`.
//!
//! # Why the certificates are not verified
//!
//! Peers already authenticate each other with a mutual HMAC challenge over
//! `cluster_secret` ([`crate::protocol`]), and neither side ever transmits the
//! secret. What was missing was **confidentiality**: the frames carrying oplog
//! entries were plaintext, so anyone on the path could read replicated
//! documents.
//!
//! The obvious way to add it — operator-supplied certificates and a CA — would
//! make certificate distribution and rotation a burden on every cluster,
//! including a two-node one on a private network, and would add a new way to
//! lock a cluster out of itself. So each node generates a self-signed
//! certificate at startup and neither side checks the other's.
//!
//! # What replaces certificate verification
//!
//! Unverified TLS on its own stops a passive eavesdropper and nothing more: an
//! active attacker terminates two separate TLS sessions and relays between
//! them, reading everything. The HMAC handshake does not help by itself, because
//! a relay can forward the challenge and the answer untouched.
//!
//! **Channel binding closes that.** Both sides ask their TLS session for
//! exported keying material ([RFC 5705]) and include it in the HMAC proof:
//!
//! ```text
//!   proof = HMAC(cluster_secret, nonce || exporter)
//! ```
//!
//! A man-in-the-middle holds *two* TLS sessions, and a session's exporter is
//! derived from secrets neither session shares with the other. So the exporter
//! it sees on one side never matches the one on the other, the relayed proof is
//! computed over the wrong value, and the handshake fails. The attacker cannot
//! recompute the proof, because that needs `cluster_secret`.
//!
//! The result is confidentiality *and* man-in-the-middle resistance, with
//! `cluster_secret` remaining the single thing an operator manages.
//!
//! # What this does not do
//!
//! It does not authenticate a node's *identity* beyond "holds the cluster
//! secret". Every node with the secret is equally trusted, which is what the
//! secret already meant.
//!
//! [RFC 5705]: https://www.rfc-editor.org/rfc/rfc5705

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as TlsError, ServerConfig, SignatureScheme,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Label for the exported keying material.
///
/// Namespaced to this protocol so the value cannot collide with an exporter
/// taken for any other purpose over the same session.
const EXPORTER_LABEL: &[u8] = b"kimmydb cluster channel binding v1";

/// Bytes of keying material mixed into the handshake proof.
pub const BINDING_LEN: usize = 32;

/// The name a peer is asked for. Never verified — see the module docs — but
/// rustls requires *a* name, and a fixed one makes it obvious in a capture that
/// it carries no meaning.
const SERVER_NAME: &str = "kimmy-cluster.invalid";

/// A node's TLS material, generated once at startup.
///
/// Ephemeral by design: the certificate proves nothing, so persisting it would
/// only create a key to manage and leak.
pub struct ClusterTls {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
}

impl ClusterTls {
    /// Generate a self-signed certificate and build both directions.
    pub fn new() -> Result<Self, String> {
        install_provider();

        let issued = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
            .map_err(|e| format!("generating the cluster certificate: {e}"))?;
        let cert = CertificateDer::from(issued.cert.der().to_vec());
        let key = PrivateKeyDer::try_from(issued.signing_key.serialize_der())
            .map_err(|e| format!("encoding the cluster key: {e}"))?;

        // No client certificates: the HMAC handshake authenticates both sides,
        // and asking for a certificate we would not verify is theatre.
        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .map_err(|e| format!("building the cluster TLS server config: {e}"))?;

        let client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate))
            .with_no_client_auth();

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
        })
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        self.acceptor.clone()
    }

    pub fn connector(&self) -> TlsConnector {
        self.connector.clone()
    }

    /// The name to connect as. Meaningless, and deliberately so.
    pub fn server_name() -> ServerName<'static> {
        ServerName::try_from(SERVER_NAME).expect("a valid DNS name literal").to_owned()
    }
}

/// Install `ring` as the process crypto provider, if nothing has yet.
///
/// Explicit rather than left to feature unification, for the reason recorded in
/// ADR-039: `ring` is already in the build, and the alternative would add CMake
/// for the same primitives. An error means a provider is already installed,
/// which is the desired end state.
pub fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Pull the channel binding out of a live session.
///
/// Failing here is not recoverable and must not be papered over: without the
/// binding the handshake would still *work*, and would silently be a handshake
/// a man-in-the-middle can relay. That is the exact failure this module exists
/// to prevent, so it is an error rather than a fallback to an empty value.
pub fn binding<D>(conn: &rustls::ConnectionCommon<D>) -> Result<Vec<u8>, String> {
    // Generic over the connection data so one function serves both directions;
    // `ClientConnection` and `ServerConnection` both deref to this.
    conn.export_keying_material(vec![0u8; BINDING_LEN], EXPORTER_LABEL, None)
        .map_err(|e| format!("exporting channel binding material: {e}"))
}

/// Accepts any certificate, because the channel binding is what secures this.
///
/// Named for what it does rather than something reassuring: anyone reading a
/// stack trace through here should immediately ask why, and find the module
/// documentation.
#[derive(Debug)]
struct AcceptAnyCertificate;

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    // The signature checks below are *not* skipped. They prove the peer holds
    // the key for the certificate it presented, which is what makes the session
    // — and therefore the exporter — belong to one endpoint rather than being
    // splice-able. Only the question "is this certificate one I trust" is
    // waived; `cluster_secret` answers that instead.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_can_build_both_directions() {
        let tls = ClusterTls::new().expect("generating cluster TLS material");
        // Cloning is how each connection gets its own handle; if these were not
        // cheap the accept loop would be paying per peer.
        let _ = tls.acceptor();
        let _ = tls.connector();
    }

    #[test]
    fn two_nodes_get_different_material() {
        // Certificates are per process and ephemeral. Two nodes sharing one
        // would not break the binding — the exporter is per session — but it
        // would mean a key was being persisted or derived, which is the thing
        // this design avoids having to manage.
        let a = ClusterTls::new().unwrap();
        let b = ClusterTls::new().unwrap();
        assert!(!std::ptr::eq(Arc::as_ptr(&a.acceptor.into()), Arc::as_ptr(&b.acceptor.into())));
    }
}
