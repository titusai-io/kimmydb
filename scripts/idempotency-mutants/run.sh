#!/usr/bin/env bash
# The B5 idempotency mutation harness. Run it with bash (zsh does not word-split an
# unquoted command, and every mutant would "survive").
#
#   scripts/idempotency-mutants/run.sh <worktree> <out-dir> [group|name ...]
#
# Exports the worktree's committed HEAD with `git archive` (uncommitted work is not
# taken, and nothing in the worktree is ever reset), checks that every row applies once,
# runs the baseline (every suite must pass unmutated), then for each row: copy the
# pristine files back, apply the row, run each of its suites under a timeout, record
# killed / survived / build failure / timeout, and the invariants a model failure names
# (`MODEL-KILL:`). One target dir of its own, reused across rows.
#
# Environment: MUTANT_TIMEOUT (seconds per suite, default 1500), MUTANT_TARGET_DIR
# (default ~/.cargo-target-b5m-mut; an inherited CARGO_TARGET_DIR is not used, since a
# shared one lets another build clobber the rows'), IDEM_CI_SEEDS (passed through to the model),
# MUTANT_INCLUDE_IGNORED (1, the default: the model's ignored rows run too, which on a
# tree without their fix fail the baseline; 0 leaves them out).
set -u
set -o pipefail

WT=${1:?worktree}
OUT=${2:?out dir}
shift 2
SELECT=("$@")
[ ${#SELECT[@]} -eq 0 ] && SELECT=(all)

HERE=$(cd "$(dirname "$0")" && pwd)
TABLE="$HERE/mutants.py"
TIMEOUT=${MUTANT_TIMEOUT:-1500}
export CARGO_TARGET_DIR=${MUTANT_TARGET_DIR:-$HOME/.cargo-target-b5m-mut}
export IDEM_CI_SEEDS=${IDEM_CI_SEEDS:-24}

mkdir -p "$OUT"
REV=$(git -C "$WT" rev-parse --short HEAD)
BASE="$OUT/base-$REV"
WORK="$OUT/work"
if [ ! -d "$BASE" ]; then
    mkdir -p "$BASE"
    git -C "$WT" archive HEAD | tar -x -C "$BASE"
fi
rm -rf "$WORK"
mkdir -p "$WORK"
git -C "$WT" archive HEAD | tar -x -C "$WORK"

if ! python3 "$TABLE" check "$WORK"; then
    echo "a row does not apply exactly once: fix the table first" >&2
    exit 2
fi

ROWS=()
for s in "${SELECT[@]}"; do
    case "$s" in
        all|design|fix-e|lives|delta|elsewhere|fixes) while read -r n; do ROWS+=("$n"); done < <(python3 "$TABLE" list "$s") ;;
        *) ROWS+=("$s") ;;
    esac
done

# timeout(1) is GNU; macOS has none by default, so a perl alarm stands in.
run_timed() {
    local secs=$1
    shift
    perl -e 'my $s = shift; $SIG{ALRM} = sub { kill "TERM", -$$; exit 124 }; setpgrp(0, 0); alarm $s; exec @ARGV' "$secs" "$@"
}

run_suites() {
    # $1: row name (or "baseline"); runs every suite of the row, or of every row.
    local name=$1 log=$2 suites line crate target filt ignored status=0
    if [ "$name" = baseline ]; then
        suites=$(for n in "${ROWS[@]}"; do python3 "$TABLE" suites "$n"; done | sort -u)
    else
        suites=$(python3 "$TABLE" suites "$name")
    fi
    local secs=$TIMEOUT own
    if [ "$name" != baseline ]; then
        own=$(python3 "$TABLE" timeout "$name")
        [ -n "$own" ] && secs=$own
    fi
    : > "$log"
    while IFS='|' read -r crate target filt ignored; do
        [ -z "$crate" ] && continue
        local extra=()
        [ "$ignored" = yes ] && [ "${MUTANT_INCLUDE_IGNORED:-1}" = 1 ] && extra=(--include-ignored)
        echo "== cargo test -p $crate $target $filt -- ${extra[*]+${extra[*]}}" >> "$log"
        # shellcheck disable=SC2086
        (cd "$WORK" && run_timed "$secs" nice -n 10 cargo test -p "$crate" $target -- "$filt" ${extra[@]+"${extra[@]}"} --test-threads 4) >> "$log" 2>&1
        local rc=$?
        echo "== exit $rc" >> "$log"
        if [ $rc -eq 124 ]; then return 124; fi
        if grep -q "could not compile\|^error\[E" "$log"; then return 3; fi
        if [ $rc -ne 0 ]; then status=1; fi
    done <<< "$suites"
    return $status
}

RESULTS="$OUT/results-$REV.tsv"
echo -e "row\tresult\tinvariants\tseconds" > "$RESULTS"

echo "baseline at $REV"
if ! run_suites baseline "$OUT/baseline.log"; then
    echo "the baseline fails: see $OUT/baseline.log" >&2
    exit 2
fi

for name in "${ROWS[@]}"; do
    start=$(date +%s)
    for f in $(python3 "$TABLE" files "$name"); do cp "$BASE/$f" "$WORK/$f"; done
    python3 "$TABLE" apply "$name" "$WORK" || { echo -e "$name\tdid-not-apply\t-\t0" >> "$RESULTS"; continue; }
    log="$OUT/$name.log"
    run_suites "$name" "$log"
    rc=$?
    case $rc in
        0) result=SURVIVED ;;
        1) result=killed ;;
        3) result=build-failure ;;
        124) result=timeout ;;
        *) result="exit-$rc" ;;
    esac
    invs=$(grep -h "^MODEL-KILL:" "$log" | sed 's/MODEL-KILL: //' | tr ',' '\n' | sort -u | paste -sd, -)
    [ -z "$invs" ] && invs=$(grep -h " \.\.\. FAILED" "$log" | sed 's/^test //; s/ \.\.\. FAILED.*//' | sort -u | paste -sd, -)
    secs=$(( $(date +%s) - start ))
    echo -e "$name\t$result\t${invs:--}\t$secs" >> "$RESULTS"
    echo "$name: $result ($invs) in ${secs}s"
    for f in $(python3 "$TABLE" files "$name"); do cp "$BASE/$f" "$WORK/$f"; done
done
echo "results: $RESULTS"
