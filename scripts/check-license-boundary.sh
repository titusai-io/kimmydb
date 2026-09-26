#!/usr/bin/env bash
#
# Fail when a workspace crate does not take the workspace licence, or when
# deny.toml's per-crate licence exceptions and those crates disagree.
#
# Why this exists
# ---------------
# LICENSING.md draws the line: everything built here is AGPL-3.0-only, and the
# client libraries an application links are Apache-2.0, so that an application
# author never has to think about the AGPL. The clients were once members of
# this workspace, and this script resolved the Rust client's shipped
# dependency graph to prove it reached no server crate -- a convenience
# `use kimmy_core::...` in the client was the easiest edit in the repository to
# make without thinking about it.
#
# The clients now live in their own repositories (ADR-193), so the rule here is
# the simpler one that keeps that true: **no member takes any licence but the
# workspace's.** An Apache-2.0 crate added back would be one `path` dependency
# away from linking the server, and nothing below could see it do so.
#
# deny.toml cannot state either rule. Its license allowlist is per lockfile,
# not per dependent: it permits the AGPL for the workspace's crates by name and
# cannot say what may depend on them. So the rules live here, as a check,
# beside the one for native code.

set -euo pipefail

cd "$(dirname "$0")/.."

# Every crate that takes the workspace license (`license.workspace = true` in
# its Cargo.toml), and every one that states a licence of its own.
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
own=''
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
      # Its own licence string, so not the workspace's.
      own="$own$name
"
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
# ---------------------------------------------------------------------------
# deny.toml's `exceptions` is the same literal list beside the same derivation.
#
# `cargo deny` permits the workspace licence per crate, **by name**, and a new
# AGPL crate that is not on that list fails the licence check. That is exactly
# the drift this script was rewritten to remove, living in a second file: a new
# crate was added, the list was not, and `cargo deny` went red on every head of
# the branch that added it while the pull request still showed mergeable,
# because it is not a required check.
#
# So the two sets must be equal, and a difference in either direction is named.
# An extra exception matters as much as a missing one: it permits the AGPL for a
# crate that no longer takes it, which is the list quietly outliving its reason.
exceptions=$(
  awk '
    /^exceptions[[:space:]]*=[[:space:]]*\[/ { inside = 1; next }
    inside && /^\]/ { inside = 0 }
    inside {
      if ($0 ~ /^[[:space:]]*#/ || $0 ~ /^[[:space:]]*$/) next
      # `{ crate = "x", allow = [...] }`, and the quote must follow `crate =`.
      # Taking the *first* quoted string on the line was wrong: with the name
      # unquoted, that is the licence, so the set gained "AGPL-3.0-only" and the
      # comparison failed naming a crate nobody had written.
      if (match($0, /crate[[:space:]]*=[[:space:]]*"[^"]+"/)) {
        field = substr($0, RSTART, RLENGTH)
        sub(/^crate[[:space:]]*=[[:space:]]*"/, "", field)
        sub(/"$/, "", field)
        print field
        next
      }
      # A line inside the block that parses as nothing would make this set
      # quietly short, which is the failure being removed.
      print "cannot parse this deny.toml exceptions line, so a crate could be missed:" > "/dev/stderr"
      print "  " $0 > "/dev/stderr"
      bad = 1
    }
    END { if (bad) exit 1 }
  ' deny.toml | sort -u
) || exit 1

if [ -z "$exceptions" ]; then
  echo "found no [licenses] exceptions in deny.toml; this comparison would be vacuous" >&2
  exit 1
fi

missing=$(comm -23 <(printf '%s\n' "$names") <(printf '%s\n' "$exceptions"))
stale=$(comm -13 <(printf '%s\n' "$names") <(printf '%s\n' "$exceptions"))
if [ -n "$missing" ] || [ -n "$stale" ]; then
  echo "deny.toml's [licenses] exceptions and the crates taking the workspace licence disagree." >&2
  [ -n "$missing" ] && {
    echo "  takes the workspace licence and has no exception, so cargo deny will fail:" >&2
    printf '    %s\n' $missing >&2
  }
  [ -n "$stale" ] && {
    echo "  has an exception and does not take the workspace licence, so the entry outlived its \
reason:" >&2
    printf '    %s\n' $stale >&2
  }
  exit 1
fi
echo "deny.toml's exceptions match the $(printf '%s' "$names" | grep -c .) crates under the \
workspace licence"

own=$(printf '%s' "$own" | sed '/^$/d' | sort -u)
if [ -z "$own" ]; then
  echo "every workspace member takes the workspace licence"
  exit 0
fi

echo "these workspace members state a licence other than the workspace's:"
echo
printf '  %s\n' $own
cat <<'MSG'

Everything in this workspace is AGPL-3.0-only (LICENSING.md). A crate under
another licence here is one `path` dependency away from linking the server,
and an Apache-2.0 library that links an AGPL crate hands the AGPL's
obligations to every application that uses it. The Apache-2.0 client
libraries live in their own repositories (ADR-193); a crate like them belongs
there too.
MSG
exit 1
