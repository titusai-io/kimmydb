#!/usr/bin/env bash
# Fetch the General Decimal Arithmetic test suite's `dq` files into a directory
# (ADR-252). Usage: scripts/fetch-dectest.sh <directory>
#
# The files are (c) Mike Cowlishaw and IBM, "all rights reserved", offered "as
# is" with no licence grant, so kimmydb never vendors them: they are fetched
# here, checked against a pinned sha256, and kept in CI's own cache. A public
# mirror of them would be a redistribution; fetching is not.
#
#   1. https://speleotrove.com/decimal/dectest.zip, whose own sha256 is pinned,
#      and which holds version 2.62 of the five files.
#   2. If that fails, CPython's copy (version 2.59) at a pinned commit, each file
#      checked against its own sha256.
#
# Nothing is moved into the directory until it has been checked. The pins are
# the ones in crates/kimmy-core/tests/common/dectest.rs, which checks them again
# when the tests read the files; keep the two in step.
set -euo pipefail

dir="${1:?usage: fetch-dectest.sh <directory>}"

zip_url="https://speleotrove.com/decimal/dectest.zip"
zip_sha256="b70a224cd52e82b7a8150aedac5efa2d0cb3941696fd829bdbe674f9f65c3926"
cpython_commit="0fb18b02c8ad56299d6a2910be0bab8ad601ef24"

files=(dqBase dqAdd dqSubtract dqMultiply dqDivide)
sha262=(
  22ee8af01c98c26eb581503bed93d38f9218530d1a7700a6c2a2f73ae972e60a
  17d0213375adc66241ddb6000ecdc124a892396b9d44fffd94207ec92697944f
  977398b439fc3014addfdff3427182fe85fd106b725bf723c6567f30dc158d06
  70f1f67c5bf8e0d89bd92c83d5ebebd15984bb746e33fca1afc7aff3585b476f
  a9443f8ea566c61b7f6e2b55d44a242d9f46f98db9304ac2a649fb54b5b754da
)
sha259=(
  766b3086d3b98ede72cd5c9f98eca908fd9a72410b2679a0c6aa2e9e39c25430
  c177a8be4d5c325db9c8357907b046bcf3160fe998192c81da2b3b756cc31ed7
  922e49be8743f06c4b150a1fce409a53028fca4805e85a19be0f982d246d1ca3
  0cf9dd544e740aa467dde13541ad10c942600518cc436b1f5562bdf1be54a7d8
  e689e4eb4404c3e58229b4fb7b93eef39e2c5deaf757ed813023c20dd3eb09d4
)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Every file in $work/set against the given sha256 list, then into place.
install_set() {
  local version="$1"; shift
  local -a want=("$@")
  local i
  for i in "${!files[@]}"; do
    local got
    got="$(sha256sum "$work/set/${files[$i]}.decTest" | cut -d' ' -f1)"
    if [ "$got" != "${want[$i]}" ]; then
      echo "dectest $version: ${files[$i]}.decTest has sha256 $got, expected ${want[$i]}" >&2
      return 1
    fi
  done
  mkdir -p "$dir"
  cp "$work"/set/*.decTest "$dir/"
  echo "dectest $version installed in $dir"
}

from_zip() {
  rm -rf "$work/set" && mkdir -p "$work/set"
  curl --fail --silent --show-error --location --max-time 120 -o "$work/dectest.zip" "$zip_url" || return 1
  local got
  got="$(sha256sum "$work/dectest.zip" | cut -d' ' -f1)"
  if [ "$got" != "$zip_sha256" ]; then
    echo "dectest.zip has sha256 $got, expected $zip_sha256" >&2
    return 1
  fi
  local f
  for f in "${files[@]}"; do
    unzip -q -o -j "$work/dectest.zip" "$f.decTest" -d "$work/set" || return 1
  done
  install_set 2.62 "${sha262[@]}"
}

from_cpython() {
  rm -rf "$work/set" && mkdir -p "$work/set"
  local f
  for f in "${files[@]}"; do
    curl --fail --silent --show-error --location --max-time 120 \
      -o "$work/set/$f.decTest" \
      "https://raw.githubusercontent.com/python/cpython/$cpython_commit/Lib/test/decimaltestdata/$f.decTest" \
      || return 1
  done
  install_set 2.59 "${sha259[@]}"
}

from_zip || { echo "falling back to CPython's copy at $cpython_commit" >&2; from_cpython; }
