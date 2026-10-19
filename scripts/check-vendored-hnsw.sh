#!/usr/bin/env bash
# vendor/hnsw_rs is hnsw_rs as published on crates.io, plus kimmydb.patch and
# README.kimmydb.md, and nothing else (ADR-241). Downloads the published crate
# (or takes HNSW_CRATE, a local copy), checks its sha256, applies the patch to
# it, and compares the result with the vendored tree byte for byte.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=0.3.5
SHA256=c9b4dcb9840006fae9964e97dfdb354cd00b7a15d69d143caae7eeca0410af9f
VENDOR=vendor/hnsw_rs

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
crate="$tmp/hnsw_rs-$VERSION.crate"
if [ -n "${HNSW_CRATE:-}" ]; then
  cp "$HNSW_CRATE" "$crate"
else
  curl --proto '=https' --tlsv1.2 -sSfL \
    "https://static.crates.io/crates/hnsw_rs/hnsw_rs-$VERSION.crate" -o "$crate"
fi
got=$(shasum -a 256 "$crate" | awk '{print $1}')
if [ "$got" != "$SHA256" ]; then
  echo "hnsw_rs-$VERSION.crate has sha256 $got, not $SHA256"
  exit 1
fi

tar -xzf "$crate" -C "$tmp"
expected="$tmp/hnsw_rs-$VERSION"
patch --quiet --batch -p1 -d "$expected" < "$VENDOR/kimmydb.patch"
cp "$VENDOR/kimmydb.patch" "$VENDOR/README.kimmydb.md" "$expected/"
if ! diff -r "$expected" "$VENDOR"; then
  echo "$VENDOR is not hnsw_rs $VERSION as published plus kimmydb.patch"
  exit 1
fi
echo "$VENDOR is hnsw_rs $VERSION as published, plus kimmydb.patch"
