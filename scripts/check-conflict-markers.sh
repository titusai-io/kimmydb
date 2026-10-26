#!/usr/bin/env bash
#
# Fail when a tracked file carries a git conflict marker.
#
# Why this exists
# ---------------
# A merge or rebase that is committed with its markers left in a file is a
# commit that was never resolved. In Rust source it fails to compile, which is
# a loud failure; in a Markdown page, a TOML file or a workflow it does not,
# and a documentation-only change runs no code step at all, so nothing else
# would object. This is the check that looks at every tracked file whatever the
# change touched.
#
# What it matches
# ---------------
# A line that begins with seven `<` or seven `>` followed by a space or the end
# of the line: the opening and closing markers git writes. The `=======`
# divider is not matched on its own, because a Markdown or reStructuredText
# heading underline is that line and is legitimate; a conflict always has the
# other two, so nothing is missed by leaving it out.
#
# Run from anywhere in the repository.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if matches=$(git grep -nIE '^(<{7}|>{7})( |$)' -- .); then
  echo "error: git conflict markers are committed:" >&2
  echo "$matches" >&2
  exit 1
fi
