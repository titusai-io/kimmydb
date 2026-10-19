#!/usr/bin/env python3
"""The idempotency model's long-run failures against the open findings' signatures.

    classify.py <test binary> <log of IDEM_KEEP_GOING=1 generated_schedules_long>

Prints one line per `FAILED seed` line of the log: the signature it matches, or OTHER,
a finding to report at once. Run it from a scratch directory, with
PYTHONDONTWRITEBYTECODE=1. The signatures (each removed when its fix lands):

- **BR1** (a backup carries no `OPLOG_HELD`, and a restored store counts a page's held
  rows as covered): the model's restore check fails at the restore itself, its detail
  `] BR1: <member>'s restore claims <origin>'s origin through ..., past ... its backup
  held`. First seen as a Home-track I4 relay at quiescence.
- **RW1** (a rewind after retention lowers the version vectors below collected writes):
  a member rewinds after its own retention pass, and the schedule passes without that
  member's rewinds (`IDEM_DROP`), usually as an I4 at quiescence.
"""
import os, re, subprocess, sys
binary, log = sys.argv[1], sys.argv[2]
fails = re.findall(r"^FAILED seed (\d+) track (\w+): (\w+): (.*)$", open(log).read(), re.M)
def run(seed, track, drop=""):
    env = dict(os.environ, IDEM_SEED=seed, IDEM_TRACK="home" if track == "Home" else "anywhere", IDEM_DROP=drop)
    out = subprocess.run([binary, "idempotency_model_tests::one_seed", "--ignored", "--exact", "--nocapture"],
                         env=env, capture_output=True, text=True, cwd="/tmp")
    return out.stderr + out.stdout
for seed, track, inv, detail in fails:
    # BR1: the model's restore check, which fires at the restore itself: the restored
    # store's witnessed vector claims another origin past what its backup held (a
    # page's held rows counted as covered). Downstream it shows as an effect held bare
    # (I2, I4 relay) on the Home track.
    if "] BR1: " in detail:
        print(f"BR1\tseed {seed} {track}\t{inv}\ta restore claims coverage its backup did not hold\t{detail[:200]}")
        continue
    out = run(seed, track)
    sched = re.findall(r"^  (\d+): (.*)$", out.split("schedule:")[1].split("failures:")[0], re.M)
    rewinds = [(int(i), s[0]) for i, s in sched if re.match(r"^[A-D]\.rewind", s)]
    verdict = "OTHER"
    why = ""
    for i, m in rewinds:
        before = [s for j, s in sched if int(j) < i]
        retention = any(s.startswith(f"{m}.retention") or f"{m}.retention" in s for s in before)
        dropped = any(re.search(rf"\b{m}\.drop\(", s) for s in before)
        if not retention:
            continue
        again = run(seed, track, ",".join(str(j) for j, x in rewinds if x == m))
        if "test result: ok. 1 passed" in again:
            verdict = "RW1" if dropped else "RW1-no-own-drop"
            why = f"{m} rewinds at step {i} after its own retention (its own drop before: {dropped}); the schedule passes without {m}'s rewinds"
            break
    print(f"{verdict}\tseed {seed} {track}\t{inv}\t{why}\t{detail[:200]}")
