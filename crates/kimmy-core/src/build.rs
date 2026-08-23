//! What build this is: version, commit, tag, date.
//!
//! Baked in by `build.rs` and re-exported from the crate at the root of the
//! dependency graph, so the server's startup log, `kimmy --version` and
//! `GET /v1/version` cannot disagree about what was built. The workspace
//! version is the single source of truth for both binaries (ADR-062); the
//! protocol version is deliberately **not** here — `/v1` is a promise about
//! the wire, not a fact about a build, and it lives with the API.
//!
//! `COMMIT` is `unknown` when the source was built without `.git` — a source
//! tarball, or a container build that did not pass `KIMMY_BUILD_COMMIT`.
//! That is a fallback, not an error: a tarball build is a supported build.

/// The workspace version, e.g. `0.1.0`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Short commit hash, with no decoration; `unknown` without git.
pub const COMMIT: &str = env!("KIMMY_BUILD_COMMIT");

/// The tag sitting exactly on the built commit, or empty. On a release
/// artifact this is `v{VERSION}`; anywhere else it is usually empty.
pub const TAG: &str = env!("KIMMY_BUILD_TAG");

/// The day the build ran, `YYYY-MM-DD` UTC. Honours `SOURCE_DATE_EPOCH`.
pub const DATE: &str = env!("KIMMY_BUILD_DATE");

/// The one-line identity both binaries print: `0.1.0 (abc123def456 2026-08-22)`.
///
/// A function rather than a `const`, because the pieces are separate
/// constants and `concat!` cannot join values that only exist as `env!`
/// expansions in another crate. `&'static str` rather than `String` so it
/// slots into clap's `version =` without the `string` feature; the one
/// allocation is made once and lives as long as the constants it joins.
pub fn ident() -> &'static str {
    static IDENT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    IDENT.get_or_init(|| format!("{VERSION} ({COMMIT} {DATE})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fallback is a supported build, so the constants must be usable
    /// whichever way they were produced: never empty, never whitespace.
    #[test]
    fn the_baked_constants_are_well_formed() {
        assert!(!VERSION.is_empty());
        assert!(!COMMIT.is_empty(), "COMMIT must be a hash or the literal `unknown`");
        assert!(COMMIT.chars().all(|c| c.is_ascii_alphanumeric()));
        // 2026-08-22, digits and dashes in the right places.
        assert_eq!(DATE.len(), 10);
        assert!(DATE.as_bytes()[4] == b'-' && DATE.as_bytes()[7] == b'-');
        assert!(ident().starts_with(VERSION));
    }
}
