#!/usr/bin/env bash
#
# Fail when the default build gains a dependency that compiles native code.
#
# Why this exists
# ---------------
# ADR-001 chose redb over RocksDB and ADR-016 chose `rust_crypto` over
# `aws_lc_rs`, both to keep the build free of a C toolchain. That property then
# stopped being true in M2 — `reqwest` pulled in `ring`, which ships C and
# assembly — and **nobody noticed for two milestones**, because the claim lived
# in prose that nothing checked.
#
# The correction (see the note on ADR-016) settled on a narrower, enforceable
# rule: the build already pays for `ring`, so do not add a *second* native
# stack. This script is that rule as a check rather than a sentence.
#
# What it does
# ------------
# Resolves the dependency graph for the **default** feature set — the build that
# actually ships — and reports any package matching a native-toolchain
# indicator that is not on the allowlist beside this script.
#
# Opt-in features that knowingly add native code are out of scope: building with
# `--features local-embeddings` pulls ONNX Runtime on purpose, and that decision
# is recorded in ADR-021. This checks what an unqualified `cargo build`
# produces.
#
# Updating the allowlist is fine — it is a review gate, not a prohibition. The
# point is that adding native code becomes a visible, deliberate line in a diff
# rather than something that arrives with a routine dependency bump.

set -euo pipefail

cd "$(dirname "$0")/.."
ALLOWLIST="scripts/allowed-native-deps.txt"

# Package-name patterns that mean "this builds native code":
#   cc / cmake / nasm-rs  — build-time compiler drivers
#   bindgen / pkg-config  — binding generation against system libraries
#   *-sys                 — the convention for a crate wrapping a native library
#   *-src                 — the convention for a crate that vendors and builds one
INDICATOR='^(cc|cmake|nasm-rs|bindgen|pkg-config)$|-sys$|-src$'

# `-e normal,build` includes build-dependencies, which is where `cc` lives —
# a native dependency is invisible in the normal edges alone. Dev-dependencies
# are excluded on purpose: they are not in anything shipped.
found=$(
  cargo tree --workspace -e normal,build --prefix none 2>/dev/null |
    awk '{print $1}' |
    grep -E "$INDICATOR" |
    sort -u || true
)

# `|| true` matters: grep exits 1 when an allowlist holds only comments, and
# under `set -e` that killed the script *before it printed anything* — exiting
# 1 for the wrong reason, which reads as a working check right up until someone
# needs the message.
allowed=$(grep -vE '^\s*(#|$)' "$ALLOWLIST" | sort -u || true)

unexpected=$(comm -23 <(echo "$found") <(echo "$allowed") || true)
missing=$(comm -13 <(echo "$found") <(echo "$allowed") || true)

status=0

if [ -n "$unexpected" ]; then
  status=1
  echo "New native build dependencies in the default build:"
  echo
  while read -r crate; do
    [ -z "$crate" ] && continue
    echo "  $crate — pulled in by:"
    # Default tree indentation rather than `--prefix depth`, which renders the
    # chain as "1ring" / "2rustls" and loses the shape that makes it readable.
    cargo tree --workspace -e normal,build -i "$crate" 2>/dev/null |
      sed -n '2,7p' | sed 's/^/      /'
    echo
  done <<<"$unexpected"
  cat <<'MSG'
This build now compiles native code it did not before, which means it needs a
C toolchain to build and cross-compile.

That may be fine — but it is a decision, not a detail. If it is intended, add
the crate to scripts/allowed-native-deps.txt with a one-line reason, and say so
in the ADR that the dependency affects. If it arrived with a version bump you
did not intend, this is the check doing its job.

Background: docs/decisions.md, the correction on ADR-016.
MSG
fi

if [ -n "$missing" ]; then
  # Not a failure. An allowlist that still names something the build dropped is
  # how a list stops describing reality, which is the failure mode this whole
  # check was written in response to.
  echo "Allowlisted but no longer present (safe to remove from the allowlist):"
  echo "$missing" | sed 's/^/  /'
  echo
fi

if [ "$status" -eq 0 ] && [ -z "$missing" ]; then
  echo "Native build dependencies are as expected:"
  echo "${found:-  (none)}" | sed 's/^/  /'
fi

exit "$status"
