#!/usr/bin/env bash
#
# Fail when the Apache-2.0 client depends on an AGPL crate.
#
# Why this exists
# ---------------
# LICENSING.md draws one line through the workspace: the server and the CLI
# are AGPL-3.0-only, `kimmy-client` is Apache-2.0, so that an application
# author never has to think about the AGPL. That holds only while the client's
# shipped dependency graph contains none of the server crates — and a
# convenience `use kimmy_core::...` in the client is the easiest edit in the
# repository to make without thinking about it.
#
# deny.toml cannot state the rule. Its license allowlist is per lockfile, not
# per dependent: it can permit the AGPL for the server crates by name (it
# does) but cannot say "and nothing Apache-2.0 may depend on them". Its
# `wrappers` mechanism can, but warns on every wrapper it does not encounter
# on every run — most of them, by construction — which is noise a policy file
# cannot afford. So the rule lives here, as a check, beside the one for native
# code.
#
# What it does
# ------------
# Resolves the client's **normal** dependency graph — what a downstream
# `cargo add kimmy-client` gets — and fails if a crate under the workspace
# license is in it. Dev-dependencies are excluded on purpose: the client's
# tests start a real server, which makes the server a dependency of the
# tests and of nothing anyone ships.

set -euo pipefail

cd "$(dirname "$0")/.."

# Every crate that takes the workspace license (`license.workspace = true` in
# its Cargo.toml). `kimmy-client` is the one that does not.
#
# **Derived, not written out.** This was a literal list, while the sentence above
# it described a derivation -- and the two had drifted: `kimmy-egress` and
# `kimmy-fuzz-harness` both take the workspace license and neither was named, so
# the boundary could not see them at all. A list cannot see a crate it does not
# name, and the crate it will not name is whichever is added after it was
# written.
#
# The members come from `[workspace] members` rather than from a `crates/*` glob,
# because a glob is the same drift one level up: it cannot see a member kept
# anywhere else. `cargo metadata` would be equally authoritative, but every other
# script here uses only sed and awk, and this needs no JSON parser to read a list
# that is already a list.
members=$(
  awk '/^members = \[/,/^\]/' Cargo.toml |
    sed -n 's/^[[:space:]]*"\(.*\)",\{0,1\}$/\1/p'
)
if [ -z "$members" ]; then
  echo "could not read [workspace] members from Cargo.toml" >&2
  exit 1
fi

names=''
for member in $members; do
  # Globs are expanded, so a `crates/*` style entry still resolves; a literal
  # path expands to itself.
  for dir in $member; do
    manifest="$dir/Cargo.toml"
    if [ ! -f "$manifest" ]; then
      echo "workspace member $dir has no Cargo.toml" >&2
      exit 1
    fi
    grep -q '^license\.workspace = true' "$manifest" || continue
    names="$names$(sed -n 's/^name = "\(.*\)"/\1/p' "$manifest" | head -1)
"
  done
done

names=$(printf '%s' "$names" | sed '/^$/d' | sort -u)
# A derivation that produces nothing would make every check below vacuous, which
# is the failure this whole change is about: it would report success loudly.
if [ -z "$names" ]; then
  echo "found no crate under the workspace license; the derivation is broken" >&2
  exit 1
fi
AGPL="^($(printf '%s' "$names" | tr '\n' '|' | sed 's/|$//')) "

found=$(
  cargo tree -p kimmy-client -e normal --prefix none 2>/dev/null |
    grep -E "$AGPL" |
    awk '{print $1}' |
    sort -u || true
)

if [ -z "$found" ]; then
  echo "kimmy-client (Apache-2.0) depends on no AGPL-3.0-only crate"
  exit 0
fi

echo "kimmy-client is Apache-2.0 and now depends on AGPL-3.0-only crates:"
echo
while read -r crate; do
  [ -z "$crate" ] && continue
  echo "  $crate — reached through:"
  cargo tree -p kimmy-client -e normal -i "$crate" 2>/dev/null |
    sed -n '1,6p' | sed 's/^/      /'
  echo
done <<<"$found"
cat <<'MSG'
An Apache-2.0 library that links an AGPL crate hands the AGPL's obligations to
every application that uses it, which is the outcome LICENSING.md promises
application authors will not happen. Move what the client needs into the
client, or into a crate that is itself Apache-2.0.
MSG
exit 1
