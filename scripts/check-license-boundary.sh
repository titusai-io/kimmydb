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
# The lines between `members = [` and its `]`, so a line can be judged rather
# than just matched: a pattern that only matches what it understands drops what
# it does not, and dropping a member is exactly the failure being fixed here.
members_lines=$(
  awk '/^members = \[/ { inside = 1; next } inside && /^\]/ { exit } inside { print }' Cargo.toml
)

members=''
while IFS= read -r line; do
  # A line with no quote holds no entry: blank, or a comment of its own.
  case "$line" in
  *'"'*) ;;
  *) continue ;;
  esac
  # An entry, with an optional comma and an optional trailing comment. Anything
  # else holding a quote is unparsed rather than skipped.
  entry=$(
    printf '%s\n' "$line" |
      sed -n 's/^[[:space:]]*"\([^"]*\)"[[:space:]]*,\{0,1\}[[:space:]]*\(#.*\)\{0,1\}$/\1/p'
  )
  if [ -z "$entry" ]; then
    echo "cannot parse this [workspace] members line, so a member could be missed:" >&2
    echo "  $line" >&2
    exit 1
  fi
  members="$members$entry
"
done <<MEMBERS
$members_lines
MEMBERS

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
    name=$(sed -n 's/^name = "\([^"]*\)"/\1/p' "$manifest" | head -1)
    if [ -z "$name" ]; then
      echo "workspace member $dir has no package name in its Cargo.toml" >&2
      exit 1
    fi
    # Three outcomes, and the third is an error rather than an exclusion. Both
    # spellings of inheriting the workspace licence count -- `license.workspace`
    # and `license = { workspace = true }` are the same TOML, and recognising
    # only the first would silently class the crate as not AGPL.
    if grep -qE '^license[[:space:]]*\.[[:space:]]*workspace[[:space:]]*=[[:space:]]*true' \
      "$manifest" ||
      grep -qE '^license[[:space:]]*=[[:space:]]*\{[^}]*workspace[[:space:]]*=[[:space:]]*true' \
        "$manifest"; then
      names="$names$name
"
    elif grep -qE '^license[[:space:]]*=[[:space:]]*"' "$manifest"; then
      : # Its own licence string, so not the workspace's.
    else
      echo "cannot tell which licence $manifest takes, so $name cannot be classified:" >&2
      grep -n '^license' "$manifest" >&2 || echo "  it states no licence" >&2
      exit 1
    fi
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
