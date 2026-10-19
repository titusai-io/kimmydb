#!/usr/bin/env python3
"""The idempotency model's long-run failures against the open findings' signatures.

    classify.py <test binary> <log of IDEM_KEEP_GOING=1 generated_schedules_long>

Prints one line per `FAILED seed` line of the log: the known open finding it matches,
or OTHER, a finding to report at once. Run it from a scratch directory, with
PYTHONDONTWRITEBYTECODE=1.

**No finding is open**: every failure is OTHER. RW1 (a rewind after retention lowered the
version vectors below collected writes) and BR1 (a restore counted a page's held rows as
covered) were fixed on main by #209 and #211; their checks stay in the model
(`row_rw1_*`, and the restore check, whose detail ends `(BR1)`), so a failure that
matches either is a regression, and OTHER. A new open finding adds its signature to
`KNOWN` with how to recognise it, and leaves it when its fix lands.

For each failure the script also says what it can see without judging it: whether the
failing step is a restore or a rewind, and whether the schedule passes without its
rewinds (`IDEM_DROP`), a first lead for the minimisation.
"""
import os, re, subprocess, sys

binary, log = sys.argv[1], sys.argv[2]
fails = re.findall(r"^FAILED seed (\d+) track (\w+): (\w+): (.*)$", open(log).read(), re.M)

# (name, predicate on (detail, schedule lines, rerun)): none open.
KNOWN = []


def run(seed, track, drop=""):
    env = dict(os.environ, IDEM_SEED=seed, IDEM_TRACK="home" if track == "Home" else "anywhere",
               IDEM_DROP=drop)
    out = subprocess.run([binary, "idempotency_model_tests::one_seed", "--ignored", "--exact",
                          "--nocapture"], env=env, capture_output=True, text=True, cwd="/tmp")
    return out.stderr + out.stdout


for seed, track, inv, detail in sorted(set(fails)):
    out = run(seed, track)
    sched = re.findall(r"^  (\d+): (.*)$", out.split("schedule:")[1].split("failures:")[0], re.M)
    verdict = next((name for name, matches in KNOWN if matches(detail, sched, run)), "OTHER")
    leads = []
    if "restore claims" in detail:
        leads.append("the restore check (BR1's) fails: a regression of #211")
    rewinds = [int(i) for i, s in sched if re.match(r"^[A-D]\.rewind", s)]
    if rewinds:
        again = run(seed, track, ",".join(str(i) for i in rewinds))
        if "test result: ok. 1 passed" in again:
            leads.append(f"passes without its rewinds (steps {rewinds}): look at RW1's fix (#209)")
    print(f"{verdict}\tseed {seed} {track}\t{inv}\t{'; '.join(leads)}\t{detail[:200]}")
