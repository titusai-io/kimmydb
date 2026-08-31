#!/usr/bin/env bash
#
# Generate the CycloneDX software bills of materials a release ships.
#
# Why this exists
# ---------------
# A release is two binaries on three targets, each linking a few hundred
# crates. Which crates, at which versions, under which licences, is knowable
# from `Cargo.lock` — but only by whoever has the source at the right commit
# and a Rust toolchain. An SBOM beside each archive answers the question for
# whoever downloaded the archive, in the format their scanner already reads
# (ADR-110).
#
# What it does
# ------------
# For every target triple given on the command line, and for each of the two
# shipped binaries, runs `cargo cyclonedx` against that binary's crate with
# the dependency graph filtered to that target, and writes
#
#     target/sbom/kimmyd-<target>.cdx.json
#     target/sbom/kimmy-cli-<target>.cdx.json
#
# named exactly as dist names the archives (`kimmyd-<target>.tar.xz`,
# `kimmy-cli-<target>.tar.xz`), so the archive and the bill that describes it
# sort together on the Release page.
# One file per binary per target rather than one for the workspace, because
# the dependency graph is not the same on every platform and a scanner fed
# the union would flag Windows-only crates against a Linux image.
#
# `dist` calls this from `[[dist.extra-artifacts]]` in dist-workspace.toml,
# in the global-artifacts job, with the targets that file lists; it then
# uploads each named file to the Release and checksums it like any other
# artifact. Run it by hand the same way:
#
#     scripts/sbom.sh x86_64-unknown-linux-musl aarch64-apple-darwin
#
# The tool
# --------
# `cargo-cyclonedx` is pinned to one version. On the release runner (Linux,
# x86_64) it is downloaded as the prebuilt musl binary from the tool's own
# release and checked against a SHA-256 recorded here — not the one published
# beside the download, which whoever replaced the download could replace too.
# Anywhere else, `cargo install --locked` at the same version. An already
# installed copy at the pinned version is used as is.
#
# The output carries a timestamp and a serial number. `SOURCE_DATE_EPOCH` is
# set from the commit being described, so two runs over one commit differ
# only in the serial, which the format requires to be unique per document.

set -euo pipefail

CYCLONEDX_VERSION=0.5.9
# sha256 of cargo-cyclonedx-x86_64-unknown-linux-musl.tar.xz from
# https://github.com/CycloneDX/cyclonedx-rust-cargo/releases/tag/cargo-cyclonedx-0.5.9
CYCLONEDX_LINUX_X86_64_SHA256=9bd3e599314f50810c9d98b8b68a617ff9d3cc20873968d90b29d121f6b226ff
CYCLONEDX_BASE_URL="https://github.com/CycloneDX/cyclonedx-rust-cargo/releases/download/cargo-cyclonedx-${CYCLONEDX_VERSION}"
# 1.5 rather than the tool's 1.3 default: `dependencies`, `licenses` as SPDX
# expressions and `hashes` are all better specified, and every current
# consumer reads it.
SPEC_VERSION=1.5

# app name (what dist calls the archive) → the crate that builds it
APPS=("kimmyd:crates/kimmyd" "kimmy-cli:crates/kimmy-cli")

usage() {
    echo "usage: $0 <target-triple> [<target-triple>...]" >&2
    exit 2
}

[ "$#" -ge 1 ] || usage

cd "$(dirname "$0")/.."
OUT=target/sbom
mkdir -p "$OUT"

have_pinned_tool() {
    # The binary reports itself as `cargo-cyclonedx-cyclonedx 0.5.9` when run
    # through cargo, so match the version rather than the whole line.
    cargo cyclonedx --version 2>/dev/null | grep -Eq "cyclonedx ${CYCLONEDX_VERSION//./\\.}$"
}

ensure_tool() {
    if have_pinned_tool; then
        return
    fi
    if [ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ]; then
        local dir archive
        dir="$(mktemp -d)"
        archive="cargo-cyclonedx-x86_64-unknown-linux-musl.tar.xz"
        echo "downloading cargo-cyclonedx ${CYCLONEDX_VERSION}" >&2
        curl --proto '=https' --tlsv1.2 -sSfL -o "$dir/$archive" "${CYCLONEDX_BASE_URL}/${archive}"
        echo "${CYCLONEDX_LINUX_X86_64_SHA256}  $dir/$archive" | sha256sum -c - >&2
        tar -xJf "$dir/$archive" -C "$dir"
        export PATH="$dir/${archive%.tar.xz}:$PATH"
    else
        echo "installing cargo-cyclonedx ${CYCLONEDX_VERSION} with cargo" >&2
        cargo install cargo-cyclonedx --locked --version "${CYCLONEDX_VERSION}" >&2
    fi
    if ! have_pinned_tool; then
        echo "cargo-cyclonedx ${CYCLONEDX_VERSION} is not on PATH after installing it" >&2
        exit 1
    fi
}

ensure_tool

# `<hex> *<name>`, as dist writes its own `.sha256` files. `sha256sum` on
# Linux, `shasum` on macOS; both print that shape with `-b`/`--binary`.
sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum --binary "$1"
    else
        shasum -a 256 -b "$1"
    fi
}

if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then
    SOURCE_DATE_EPOCH="$(git log -1 --format=%ct 2>/dev/null || date +%s)"
    export SOURCE_DATE_EPOCH
fi

# cargo-cyclonedx writes one document per workspace member next to that
# member's Cargo.toml whatever manifest it is pointed at, and names them all
# after `--override-filename`. Only the shipped binary's document is kept; the
# rest are removed before the next run so nothing stale can be picked up.
for target in "$@"; do
    for entry in "${APPS[@]}"; do
        app="${entry%%:*}"
        crate="${entry#*:}"
        scratch="sbom-${app}-${target}"
        cargo cyclonedx \
            --manifest-path "$crate/Cargo.toml" \
            --format json \
            --spec-version "$SPEC_VERSION" \
            --target "$target" \
            --override-filename "$scratch" \
            -q
        produced="$crate/$scratch.json"
        if [ ! -s "$produced" ]; then
            echo "cargo cyclonedx wrote nothing at $produced" >&2
            exit 1
        fi
        mv "$produced" "$OUT/$app-$target.cdx.json"
        find crates -maxdepth 2 -name 'sbom-*.json' -delete
        # A checksum beside each file, in the format dist writes beside each
        # archive, so `sha256sum -c` works the same way on both. dist checksums
        # only the archives it builds itself; extra artifacts get none.
        (cd "$OUT" && sha256 "$app-$target.cdx.json" > "$app-$target.cdx.json.sha256")
        echo "wrote $OUT/$app-$target.cdx.json" >&2
    done
done
