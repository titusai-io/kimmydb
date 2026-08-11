#!/usr/bin/env python3
"""Record and check the benchmark baseline.

The baseline is how a performance regression gets *caught* rather than
noticed months later in an unrelated profile. It is advisory by design —
benchmarks stay "recorded, not gated" (docs/benchmarks.md): Criterion on a
shared CI runner is noisy enough that a hard threshold produces failures
people learn to ignore. What this script changes is the cost of checking by
hand, which used to be "diff numbers against a table in a document by eye".

Usage, after `cargo bench -p kimmy-storage -p kimmy-vector`:

    scripts/bench-baseline.py record    # overwrite the baseline with this run
    scripts/bench-baseline.py check     # compare this run against the baseline

`check` exits non-zero when any benchmark drifted beyond the tolerance, so it
can be scripted — but the intended reader is a person deciding whether a
branch made something slower, on the machine the baseline was recorded on.
Comparing numbers from a different machine tells you about the machines.
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CRITERION = ROOT / "target" / "criterion"
BASELINE = ROOT / "scripts" / "bench-baseline.json"

# Generous on purpose: durable commits on a development machine jitter tens
# of percent run to run. The baseline exists to catch a *shape* change — a
# 2x, an accidental O(n) — not a five-percent wobble.
TOLERANCE = 0.5


def current() -> dict[str, float]:
    """Median point estimates from the last bench run, in nanoseconds."""
    if not CRITERION.is_dir():
        sys.exit("no target/criterion; run `cargo bench` first")
    out = {}
    for estimates in sorted(CRITERION.glob("**/new/estimates.json")):
        name = "/".join(estimates.parent.parent.relative_to(CRITERION).parts)
        with open(estimates) as f:
            out[name] = json.load(f)["median"]["point_estimate"]
    return out


def record() -> None:
    measurements = current()
    with open(BASELINE, "w") as f:
        json.dump({"unit": "ns_median", "benchmarks": measurements}, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"recorded {len(measurements)} benchmarks to {BASELINE.relative_to(ROOT)}")


def check() -> None:
    with open(BASELINE) as f:
        baseline = json.load(f)["benchmarks"]
    drifted = 0
    for name, now in sorted(current().items()):
        then = baseline.get(name)
        if then is None:
            print(f"  new       {name}: {now / 1e6:.2f} ms (not in baseline)")
            continue
        ratio = now / then
        marker = "ok" if abs(ratio - 1) <= TOLERANCE else ("SLOWER" if ratio > 1 else "FASTER")
        drifted += marker != "ok"
        print(f"  {marker:<9} {name}: {then / 1e6:.2f} ms -> {now / 1e6:.2f} ms ({ratio:.2f}x)")
    for name in sorted(set(baseline) - set(current())):
        print(f"  missing   {name}: in the baseline but not this run")
    if drifted:
        sys.exit(f"{drifted} benchmark(s) beyond the {TOLERANCE:.0%} tolerance")
    print("within tolerance")


if __name__ == "__main__":
    match sys.argv[1:]:
        case ["record"]:
            record()
        case ["check"]:
            check()
        case _:
            sys.exit(__doc__)
