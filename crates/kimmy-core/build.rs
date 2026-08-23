//! Embeds what `git` knows about this checkout, at compile time.
//!
//! # Why here, and why this small
//!
//! `kimmy-core` sits at the root of the dependency graph, so a constant baked
//! here is visible to the server, the CLI and the version endpoint without
//! three build scripts saying the same thing. `vergen` was considered and
//! passed over: it answers many questions nobody here asks, and this needs
//! exactly three — the commit, the tag if there is one, and the date.
//!
//! # A tarball must still build
//!
//! Every value has a fallback, because `git` is absent in two builds that
//! matter: a source tarball (no `.git` at all) and the Docker build context
//! (`.git` is not copied in). The commit falls back to `unknown` unless the
//! environment supplies `KIMMY_BUILD_COMMIT`, which is how the container
//! image gets a real commit without shipping its history — the release
//! workflow passes it as a build argument.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Run `git` and return trimmed stdout, or `None` for any kind of failure —
/// git missing, not a repository, or the query having no answer.
fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let s = s.trim();
    if s.is_empty() { None } else { Some(s.to_string()) }
}

/// `YYYY-MM-DD` from a Unix timestamp, without pulling a date crate into
/// every build. Days-to-civil conversion after Howard Hinnant's algorithm.
fn civil_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let doe = days.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

fn main() {
    // Recompile when the checkout moves. HEAD covers a checkout or detach;
    // the ref file it points at covers a commit landing on the same branch.
    if let Some(dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={dir}/HEAD");
        if let Some(head) = git(&["symbolic-ref", "-q", "HEAD"]) {
            println!("cargo:rerun-if-changed={dir}/{head}");
        }
    }
    println!("cargo:rerun-if-env-changed=KIMMY_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");

    // The environment wins over git, because the environment is how a build
    // *without* git — the container image — says what it was built from.
    let commit = std::env::var("KIMMY_BUILD_COMMIT")
        .ok()
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .or_else(|| git(&["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());

    // Exact tag on HEAD, or empty. Not `--always`: a near-miss like
    // `v0.2.0-3-gabc1234` is a commit description, not a tag.
    let tag = git(&["describe", "--tags", "--exact-match", "HEAD"]).unwrap_or_default();

    // SOURCE_DATE_EPOCH is the reproducible-builds convention: honouring it
    // means two builds of the same source can be byte-identical.
    let epoch =
        std::env::var("SOURCE_DATE_EPOCH").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or_else(
            || SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
        );

    println!("cargo:rustc-env=KIMMY_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=KIMMY_BUILD_TAG={tag}");
    println!("cargo:rustc-env=KIMMY_BUILD_DATE={}", civil_date(epoch));
}
