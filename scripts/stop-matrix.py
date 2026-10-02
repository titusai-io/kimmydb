#!/usr/bin/env python3
"""Timed stop matrix for ADR-195's reset/carry-forward.

The standing rule after a prior stop-path regression that passed every
test yet stopped in 6-10 s and exited 75 is that a stop path is timed
against real processes, not only asserted on in a unit test. ADR-195
makes a draining tick reset the moment its budget runs out rather than
waiting a full interval, and carries peers across ticks -- both are new
state a SIGTERM mid-drain has to unwind cleanly, whatever pull or walk
it lands inside of.

This boots a real 3-node cluster per trial, forces one member (`a`) into an
active drain from the moment it starts (a deep backlog plus
KIMMY_TEST_WALK_ROW_MS/KIMMY_TEST_SERVE_WALK_MS -- process-global env vars,
fine here since each trial is its own process, not a parallel `cargo test`),
runs background load throughout on both the member about to be signalled
and a third, otherwise idle member (a steady insert loop, a TTL collection
expiring every second, one index/DDL create fired without waiting for it
just before the signal, so it is genuinely in flight rather than already
acknowledged), and sends SIGTERM at a randomized offset into the drain to
either (a) `b`, the member pulling from `a` (the "requester"), or (b) `a`
itself (the "server", mid-walk). It records the exit code (any nonzero
value, not a fixed list -- a process a supervisor had to SIGKILL reports a
negative signal number here, not the 128+signal a shell would show), the
signal-to-exit duration, whether the log shows a clean "shutdown complete",
whether a reset chain (ADR-195) was logged in progress right at the signal,
and whether the log shows a background task, HTTP connections, schema-change
push drivers or a write still going at the end of the stop's window. After
a clean exit it then restarts the stopped member on its own data and checks
that start's own log too: an exit code of 0 is not proof the next start
finds nothing to repair.

Usage:
    scripts/stop-matrix.py --baseline-bin PATH --head-bin PATH [--trials N]

`--yield-half {target,peer}` (ADR-213) makes the head build's odd-indexed trials
yield: one member's expiry task is wedged with `KIMMY_TEST_KILL_TASK=ttl_expiry:stall`
at `KIMMY_TEST_YIELD_SCALE=5`, and the trial waits for that member's
`kimmy_yielding{class="ttl"}` to read 1 **and** `kimmy_yield_unconfirmed_peers{class="ttl"}`
to read 0 (every live peer has echoed the yield, so the member has stopped owning)
before the load and the offset start, so the signal lands on a cluster where the
yield is in force and not merely advertised. `target` wedges the member that
is then signalled (its stop has to end a stalled task and an evaluator), `peer` the
third member (the stopped member's pulls and pushes meet a yielded peer). Head only,
because the baseline has no yielding to arm, and a yield that never takes effect
within 120 s fails the run: it is not a stop result. The pairing, the offsets and the
three paired tests are unchanged, so the head's yielded trials are compared with the
baseline's un-yielded ones on the same offsets.

Each binary must be a `--release` build of `kimmyd`:

    git worktree add /tmp/kimmydb-v0410-baseline v0.41.0
    (cd /tmp/kimmydb-v0410-baseline && \\
        CARGO_TARGET_DIR=/tmp/kimmydb-v0410-baseline/.cargo-target \\
        cargo build --release -p kimmyd)
    # baseline-bin: /tmp/kimmydb-v0410-baseline/.cargo-target/release/kimmyd

    CARGO_TARGET_DIR=<this worktree>/.cargo-target-resetcarry \\
        cargo build --release -p kimmyd
    # head-bin: <this worktree>/.cargo-target-resetcarry/release/kimmyd

Pass means, for each build: every trial exits 0 with a logged "shutdown
complete" and no sign of a background task, an HTTP connection, a push
driver or a write left running at the stop window's end, no restart that
found something to repair, and the head build no slower to stop than the
baseline on the paired trials, by three tests on the differences (head minus
baseline) of the pairs that drew the same offset: their mean is within
STOP_TIME_EPSILON; no single pair is worse by more than STOP_TIME_WORST_PAIR;
and the pairs clearly worse by more than STOP_TIME_PAIR_NOISE do not outnumber
the pairs clearly better by that much by as many as a fifth of the pairs (the
excess count, which catches a regression that hits some stops by a few tenths
of a second, where a mean and one worst pair both miss it). Each FAIL line
says which test failed. **A FAIL from the excess count alone warrants a
re-run**: it is the most sensitive of the three, and on a noisy host it fails a
build that is no slower about 1 run in 20. `--seed` is random unless given, and
always printed, so a failing run can be reproduced exactly.

**The two builds' trials are interleaved**, one pair at a time, each pair on
the same offset and in alternating order (baseline first, then head first).
Running one build's whole group and then the other's put the second build
under whatever the host was doing later: on a busy machine the second group
was slower whichever build it was, and swapping the builds swapped the
verdict. Four runs said so before the paired design replaced it. A stop that
is slow because of the host is then as likely to land on either build, and
the mean of the paired differences cancels it.
"""

import argparse
import json
import math
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT_PASSWORD = "stop-matrix-root-password"
JWT_SECRET = "stop-matrix-jwt-secret-at-least-32-bytes-long"
CLUSTER_SECRET = "stop-matrix-cluster-secret"

# How much slower the head build may be to stop than the baseline, on the mean
# of the paired differences, before the matrix reports a regression rather than
# ordinary run-to-run jitter on this machine.
STOP_TIME_EPSILON_S = 0.25

# How much worse one pair may be. Stop times on a busy host jump by up to a
# second or so on either build (a single slow trial of 0.6-0.9 s was seen on
# both), so one pair is judged against a wider bound than the mean: a stop that
# regresses to the budget's seconds, or an exit 75, is far past it. A review
# simulated 24 pairs with host noise of this shape: 1.25 s keeps the false FAIL
# rate of the three tests together at 8% or less, and catches a regression of
# +1.5 s on one stop in eight in 93% of runs, where 2 s caught about half.
STOP_TIME_WORST_PAIR_S = 1.25

# A pair differing by more than this either way is counted by the excess test:
# beyond the 0.1-0.3 s a stop varies by on a quiet host, and below the
# regressions worth stopping a release for.
STOP_TIME_PAIR_NOISE_S = 0.3

# The excess count never fails below this many pairs, whatever a fifth of them is.
EXCESS_COUNT_MIN = 3

# A clean stop is exit 0 with the marker logged -- nothing else. Python's
# subprocess reports a process a supervisor had to kill as a *negative*
# signal number (SIGKILL is -9), never the 128+signal a shell would show,
# so checking against a fixed list of "known bad" codes such as 137 misses
# every such death; only a positive check for the one good outcome catches
# all of them.
# The reset-tick marker peers.rs logs at DEBUG (ADR-195), and the module
# path RUST_LOG must enable for it to appear. Logged at the *start* of a
# reset tick, so seeing it in the last few lines before the signal means
# that tick had just begun -- it may still be running its pulls, or may
# already have finished and moved on to whatever came after it; either
# way, a reset was underway close enough to the signal to be relevant to
# the stop this trial is timing.
RESET_LOG_MARKER = "reset tick starting"
# ADR-213: a yielded member, in the trials that ask for one. The head build only:
# the baseline has no yielding to arm. `ttl_expiry:stall` wedges that member's
# expiry task, and at scale 5 the evaluator judges it stalled in about 17 s and the
# yield is effective, once every peer has echoed it, in about 15 s more.
YIELD_ENV = {"KIMMY_TEST_YIELD_SCALE": "5", "KIMMY_TEST_KILL_TASK": "ttl_expiry:stall"}
YIELD_WAIT_S = 120
RESET_LOG_TARGET = "kimmy_cluster::peers=debug"


def choose_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http(method, url, body=None, token=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def yield_effective(node):
    """Whether `node`'s yield of its expiry is **in force**, not only advertised
    (`/metrics` is open): the bit is set, and every live peer has echoed it, which
    is what `kimmy_yield_unconfirmed_peers` counting 0 says. Until then the member
    keeps owning the class."""
    try:
        with urllib.request.urlopen(node.url("/metrics"), timeout=2) as r:
            body = r.read().decode()
    except Exception:
        return False
    return (
        'kimmy_yielding{class="ttl"} 1' in body
        and 'kimmy_yield_unconfirmed_peers{class="ttl"} 0' in body
    )


class Node:
    def __init__(self, binary, name, http_port, cluster_port, seed_ports, sync_interval_secs, extra_env=None):
        self.name = name
        self.http_port = http_port
        self.dir = tempfile.mkdtemp(prefix=f"kimmy-stopmatrix-{name}-")
        seed_list = ", ".join(f'"127.0.0.1:{p}"' for p in seed_ports)
        config = f"""
[server]
bind = "127.0.0.1:{http_port}"

[storage]
data_dir = "{self.dir}/data"
ttl_interval_secs = 1

[auth]
jwt_secret = "{JWT_SECRET}"

[cluster]
enabled = true
bind = "127.0.0.1:{cluster_port}"
seeds = [{seed_list}]
cluster_secret = "{CLUSTER_SECRET}"
sync_interval_secs = {sync_interval_secs}
discovery_interval_secs = 1

[webhooks]
allowed_hosts = ["127.0.0.1"]
"""
        self.config_path = os.path.join(self.dir, "kimmy.toml")
        with open(self.config_path, "w") as f:
            f.write(config)
        env = dict(os.environ)
        env["KIMMY_ROOT_PASSWORD"] = ROOT_PASSWORD
        env["RUST_LOG"] = f"info,{RESET_LOG_TARGET}"
        if extra_env:
            env.update(extra_env)
        self.stdout_path = os.path.join(self.dir, "stdout.log")
        self._stdout = open(self.stdout_path, "w")
        self.proc = subprocess.Popen(
            [binary, "--config", self.config_path],
            stdout=self._stdout,
            stderr=subprocess.STDOUT,
            env=env,
        )

    def url(self, path):
        return f"http://127.0.0.1:{self.http_port}{path}"

    def wait_ready(self, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited before it was ready:\n{self.log()}")
            try:
                http("GET", self.url("/readyz"), timeout=1)
                return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError(f"{self.name} never became ready:\n{self.log()}")

    def log(self):
        self._stdout.flush()
        with open(self.stdout_path) as f:
            return f.read()

    def signal_term(self):
        self.proc.send_signal(signal.SIGTERM)

    def wait_exit(self, timeout=30):
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def kill_if_alive(self):
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self._stdout.close()

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def restart(self, binary):
        """Start a fresh process on this node's own data dir and config, its
        prior log moved aside -- a clean-looking exit code and a clean start
        are not the same claim, so the only way to check "the next start
        repairs nothing" is to actually run it."""
        self._stdout.close()
        os.replace(self.stdout_path, self.stdout_path + ".before")
        env = dict(os.environ)
        env["KIMMY_ROOT_PASSWORD"] = ROOT_PASSWORD
        self._stdout = open(self.stdout_path, "w")
        self.proc = subprocess.Popen(
            [binary, "--config", self.config_path],
            stdout=self._stdout,
            stderr=subprocess.STDOUT,
            env=env,
        )


def background_writers(node, token, stop_event):
    """A steady insert loop plus periodic TTL-eligible writes, on `node`,
    until `stop_event` is set. Errors are swallowed: the point is load, and
    a request failing because the node is mid-stop is exactly what this
    load is for."""
    i = 0
    while not stop_event.is_set():
        try:
            http(
                "POST",
                node.url("/v1/db/shop/coll/live/docs"),
                {"_id": f"live-{i}", "n": i},
                token,
                timeout=2,
            )
            http(
                "POST",
                node.url("/v1/db/shop/coll/sessions/docs"),
                {"_id": f"sess-{i}", "createdAt": {"$date": "2000-01-01T00:00:00Z"}},
                token,
                timeout=2,
            )
        except Exception:
            pass
        i += 1
        time.sleep(0.05)


def run_trial(binary, target, index, offset_s, walk_row_ms, serve_walk_ms, seed_batches, trial_label, yielded=None):
    """target: 'requester' (signals b) or 'server' (signals a). `index` is
    this trial's position within its (target, build) group -- with a
    shared seed, the same (target, index) pair draws the same offset on
    both builds, which is what lets a caller pair them up afterwards
    rather than comparing group averages that a few unlucky offsets could
    skew either way."""
    http_ports = [choose_port() for _ in range(3)]
    cluster_ports = [choose_port() for _ in range(3)]
    names = ["a", "b", "c"]
    nodes = []
    stop_writers = threading.Event()
    writer_threads = []
    result = {
        "trial": trial_label,
        "target": target,
        "index": index,
        "offset_s": round(offset_s, 3),
        "yielded": yielded,
        "yield_effective_s": None,
    }
    try:
        a_env = {
            "KIMMY_TEST_WALK_ROW_MS": str(walk_row_ms),
            "KIMMY_TEST_SERVE_WALK_MS": str(serve_walk_ms),
        }
        for i in range(3):
            seeds = [cluster_ports[j] for j in range(3) if j != i]
            extra = dict(a_env) if names[i] == "a" else {}
            # The member that yields: the one about to be signalled, or the third.
            if yielded == "target" and i == (1 if target == "requester" else 0):
                extra.update(YIELD_ENV)
            if yielded == "peer" and names[i] == "c":
                extra.update(YIELD_ENV)
            extra = extra or None
            n = Node(binary, names[i], http_ports[i], cluster_ports[i], seeds, sync_interval_secs=2, extra_env=extra)
            nodes.append(n)
        a, b, c = nodes
        for n in nodes:
            n.wait_ready()

        token = http("POST", a.url("/v1/auth/login"), {"user": "root", "password": ROOT_PASSWORD})["token"]
        http("POST", a.url("/v1/db/shop/collections"), {"name": "orders"}, token)
        http("POST", a.url("/v1/db/shop/collections"), {"name": "live"}, token)
        http("POST", a.url("/v1/db/shop/collections"), {"name": "sessions"}, token)
        http(
            "POST",
            a.url("/v1/db/shop/coll/sessions/indexes"),
            {"fields": [{"path": "createdAt"}], "expireAfterSeconds": 1},
            token,
        )
        # `a`'s deep backlog: what its slowed serve walk drains for `b`/`c`
        # to actively pull from the moment they gossip with it.
        for batch in range(seed_batches):
            docs = [{"_id": f"seed-{batch}-{i}"} for i in range(50)]
            http("POST", a.url("/v1/db/shop/coll/orders/bulk"), docs, token)

        # A trial that asks for a yield waits for it to be effective before any load
        # or offset starts, so the signal lands on a cluster where the yield is
        # already in force. A yield that never takes effect is a failed trial, not
        # a stop result.
        if yielded:
            yielded_node = (
                nodes[1 if target == "requester" else 0] if yielded == "target" else nodes[2]
            )
            waited_from = time.monotonic()
            while time.monotonic() - waited_from < YIELD_WAIT_S and not yield_effective(yielded_node):
                time.sleep(0.5)
            if yield_effective(yielded_node):
                result["yield_effective_s"] = round(time.monotonic() - waited_from, 1)
            else:
                result["yield_never_effective"] = True

        # Load on both the member about to be signalled and a third,
        # otherwise idle one: the target's own stop path is what this trial
        # is timing, so its own background writes, TTL expiry and DDL push
        # are what it has to unwind, not only another member's.
        target_node = b if target == "requester" else a
        writer_threads = [
            threading.Thread(target=background_writers, args=(node, token, stop_writers), daemon=True)
            for node in (target_node, c)
        ]
        for wt in writer_threads:
            wt.start()

        time.sleep(offset_s)

        # A reset tick (ADR-195) started close enough to the signal to be
        # relevant: the last few lines before it include the DEBUG marker
        # a reset tick logs at its own start, meaning the signal landed
        # near an active reset chain rather than between chains or on an
        # ordinary tick.
        pre_signal_log = target_node.log()
        recent = pre_signal_log.splitlines()[-5:]
        result["reset_in_progress_at_signal"] = any(RESET_LOG_MARKER in l for l in recent)

        # One DDL create, fired without waiting for its response, so it is
        # genuinely still in flight rather than already acknowledged by the
        # time the signal lands.
        ddl_thread = threading.Thread(
            target=lambda: http(
                "POST",
                c.url("/v1/db/shop/coll/live/indexes"),
                {"fields": [{"path": "n"}], "name": f"ddl-{trial_label}"},
                token,
                timeout=5,
            ),
            daemon=True,
        )
        ddl_thread.start()

        before = time.monotonic()
        target_node.signal_term()
        code = target_node.wait_exit(timeout=30)
        elapsed = time.monotonic() - before
        stop_writers.set()

        log = target_node.log()
        result.update(
            {
                "exit_code": code,
                "stop_time_s": round(elapsed, 3),
                "clean_marker": "shutdown complete" in log,
                "task_aborted": "still stopping" in log and "aborted" in log,
                "connections_left_open": "HTTP connections were still open" in log,
                "push_drivers_running": "schema-change push drivers were still running" in log,
                "write_still_in_progress": "a write was still in progress" in log,
                "timed_out": code is None,
            }
        )

        # A clean exit code is not a clean start: restart the stopped
        # member on its own data and check the next start's own read of it,
        # not just the stop's own exit code.
        if code == 0:
            try:
                target_node.restart(binary)
                target_node.wait_ready(timeout=15)
                restart_log = target_node.log()
                result["restart_clean"] = (
                    "previous run ended cleanly" in restart_log
                    and "repairing the database" not in restart_log
                )
            except Exception as e:
                result["restart_clean"] = False
                result["restart_error"] = str(e)
        else:
            result["restart_clean"] = None
    finally:
        stop_writers.set()
        for wt in writer_threads:
            wt.join(timeout=2)
        for n in nodes:
            n.kill_if_alive()
        for n in nodes:
            n.cleanup()
    return result


def run_paired_matrix(builds, trials, walk_row_ms, serve_walk_ms, seed_batches, seed, yield_mode=None):
    """`builds`: [(label, binary), (label, binary)], baseline first. One trial
    per build for each (target, index), run back to back on the same offset,
    the order alternating from one pair to the next, so the host's state at
    the time falls on both builds alike. Returns {label: results}."""
    rng = random.Random(seed)
    results = {label: [] for label, _ in builds}
    targets = ["requester", "server"]
    per_target = max(1, trials // len(targets))
    n = 0
    for target in targets:
        for i in range(per_target):
            offset = rng.uniform(2.0, 30.0)
            order = builds if n % 2 == 0 else builds[::-1]
            n += 1
            for label, binary in order:
                label_i = f"{label}/{target}/{i}"
                print(f"  [{label_i}] offset={offset:.2f}s ...", file=sys.stderr, flush=True)
                # Half the trials, the odd indexes, on the head build only.
                yielded = yield_mode if yield_mode and i % 2 == 1 and label.startswith("head") else None
                r = run_trial(
                    binary, target, i, offset, walk_row_ms, serve_walk_ms, seed_batches, label_i, yielded
                )
                results[label].append(r)
                print(
                    f"    exit={r['exit_code']} stop_time={r['stop_time_s']}s "
                    f"clean={r['clean_marker']} reset_in_progress={r['reset_in_progress_at_signal']} "
                    f"aborted={r['task_aborted']}",
                    file=sys.stderr,
                    flush=True,
                )
    return results


def summarize(label, results):
    # Any exit other than a clean 0, or a 0 whose log never actually shows
    # the clean marker, is a stop-path failure -- not a fixed list of
    # "known bad" codes, which a negative signal-death code would slip past.
    bad_exits = [
        r for r in results if r["timed_out"] or r["exit_code"] != 0 or not r["clean_marker"]
    ]
    aborted = [
        r
        for r in results
        if r["task_aborted"]
        or r["connections_left_open"]
        or r["push_drivers_running"]
        or r["write_still_in_progress"]
    ]
    dirty_restarts = [r for r in results if r["restart_clean"] is False]
    yielded = [r for r in results if r.get("yielded")]
    unyielded = [r for r in yielded if r.get("yield_never_effective")]
    if yielded:
        print(f"\n   trials with a yielded member: {len(yielded)}, yield never took effect: {len(unyielded)}")
        for r in unyielded:
            print(f"     {r}")
        bad_exits = bad_exits + unyielded
    worst = max((r["stop_time_s"] for r in results), default=0.0)
    resets = sum(1 for r in results if r["reset_in_progress_at_signal"])
    print(f"\n== {label}: {len(results)} trials, worst stop {worst:.3f}s ==")
    print(f"   trials where a reset chain was logged in progress at the signal: {resets}")
    print(f"   bad exits (nonzero, timed out, or no clean marker): {len(bad_exits)}")
    for r in bad_exits:
        print(f"     {r}")
    print(
        f"   aborted background work, left-open connections, a push driver or a write "
        f"still going: {len(aborted)}"
    )
    for r in aborted:
        print(f"     {r}")
    print(f"   next start was not clean (repaired, or restart itself failed): {len(dirty_restarts)}")
    for r in dirty_restarts:
        print(f"     {r}")
    return worst, bad_exits, aborted, dirty_restarts


def pair_and_compare(baseline_results, head_results):
    """With a shared seed, trial i of a (target, build) group draws the
    same offset on both builds, so pairing by (target, index) compares
    like against like -- a fixed set of harder or easier offsets in one
    build's run and not the other's cannot inflate or hide a group
    average's gap the way it could between two independently-averaged
    groups. Prints the worst and mean per-pair delta (head minus
    baseline) and returns the worst, the mean, and every delta."""
    by_key = {(r["target"], r["index"]): r for r in baseline_results}
    deltas = []
    for head in head_results:
        key = (head["target"], head["index"])
        base = by_key.get(key)
        if base is None:
            continue
        deltas.append((key, head["stop_time_s"] - base["stop_time_s"], base["stop_time_s"], head["stop_time_s"]))
    if not deltas:
        print("\n== paired comparison: no matching (target, index) pairs found ==")
        return 0.0, 0.0, []
    deltas.sort(key=lambda d: d[1], reverse=True)
    worst_key, worst_delta, worst_base, worst_head = deltas[0]
    mean_delta = sum(d[1] for d in deltas) / len(deltas)
    print(
        f"\n== paired comparison ({len(deltas)} pairs, same offset each): "
        f"worst delta {worst_delta:+.3f}s at {worst_key} (baseline {worst_base:.3f}s, "
        f"head {worst_head:.3f}s), mean delta {mean_delta:+.3f}s =="
    )
    return worst_delta, mean_delta, [d[1] for d in deltas]


def excess_count(deltas, noise=STOP_TIME_PAIR_NOISE_S):
    """Pairs where head was slower by more than `noise`, minus pairs where it was
    faster by more than `noise`: what a mean and one worst pair can both miss,
    when a regression is a few tenths of a second on some of the stops. The
    deltas are rounded to the millisecond first, so a pair of exactly `noise`
    does not count one way or the other by float error."""
    rounded = [round(d, 3) for d in deltas]
    return sum(1 for d in rounded if d > noise) - sum(1 for d in rounded if d < -noise)


def excess_limit(pairs):
    """The excess at which the count test fails: a fifth of the pairs, and never
    fewer than 3, so that with few trials one noisy pair cannot fail a run."""
    return max(EXCESS_COUNT_MIN, math.ceil(pairs / 5))


def judge_pairs(worst_delta, mean_delta, deltas):
    """The three tests on the paired differences. Returns (failures, notes):
    each failure names its test, and no pair at all is a failure, never a pass:
    a run that compared nothing has said nothing about the build."""
    if not deltas:
        return ["FAIL (no pairs): no (target, index) pair of trials matched between the builds, so nothing was compared"], []
    failures, notes = [], []
    excess = excess_count(deltas)
    limit = excess_limit(len(deltas))
    if len(deltas) < 10:
        notes.append(f"the excess-count test is weak below about 10 pairs; this run has {len(deltas)}")
    if mean_delta > STOP_TIME_EPSILON_S:
        failures.append(
            f"FAIL (mean test): the pairs' mean difference is {mean_delta:+.3f}s, past {STOP_TIME_EPSILON_S}s"
        )
    if worst_delta > STOP_TIME_WORST_PAIR_S:
        failures.append(
            f"FAIL (worst-pair test): the worst same-offset pair regressed by {worst_delta:.3f}s, "
            f"past {STOP_TIME_WORST_PAIR_S}s"
        )
    if excess >= limit:
        failures.append(f"FAIL (excess-count test): {excess:+d} pairs, limit {limit}")
        if len(failures) == 1:
            notes.append("the excess count failed alone: re-run before concluding, it is the most sensitive test")
    return failures, notes


def build_label(role, binary):
    """`role`, the version and the commit the binary reports, so the tables say
    what actually ran: `baseline 0.42.0 2968407d04c2`. `kimmyd --version`
    prints `kimmyd 0.42.0 (2968407d04c2 2026-09-29)`; a binary that does not
    answer that way is labelled by its role alone."""
    try:
        out = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return role
    match = re.match(r"kimmyd (\S+) \((\S+) ", out)
    return f"{role} {match.group(1)} {match.group(2)}" if match else role


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--baseline-bin", required=True, help="release kimmyd built from the version to compare against")
    p.add_argument("--head-bin", required=True, help="release kimmyd built from this branch's head")
    p.add_argument("--trials", type=int, default=24, help="total trials per build, split across targets")
    p.add_argument("--walk-row-ms", type=int, default=25)
    p.add_argument("--serve-walk-ms", type=int, default=700)
    p.add_argument("--seed-batches", type=int, default=60, help="50 docs each")
    p.add_argument(
        "--seed", type=int, default=None, help="offsets' random seed; random and printed if omitted"
    )
    p.add_argument(
        "--yield-half",
        choices=["target", "peer"],
        default=None,
        help="in the odd-indexed trials of the head build, wedge one member's expiry task so it "
        "yields (ADR-213) and wait for the yield to be effective before the load starts: "
        "`target` yields the member that is then signalled, `peer` the third member. Head "
        "only, since the baseline has no yielding",
    )
    p.add_argument("--json-out", help="write the raw per-trial results here as JSON")
    args = p.parse_args()
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(2**32)
    print(f"seed: {args.seed}", file=sys.stderr)

    baseline_label = build_label("baseline", args.baseline_bin)
    head_label = build_label("head", args.head_bin)
    print(f"baseline: {baseline_label}\nhead: {head_label}", file=sys.stderr)

    print(f"\n### {args.trials} trials per build, interleaved ###", file=sys.stderr)
    all_results = run_paired_matrix(
        [(baseline_label, args.baseline_bin), (head_label, args.head_bin)],
        args.trials,
        args.walk_row_ms,
        args.serve_walk_ms,
        args.seed_batches,
        args.seed,
        args.yield_half,
    )

    worsts = {}
    failed = False
    for label, results in all_results.items():
        worst, bad_exits, aborted, dirty_restarts = summarize(label, results)
        worsts[label] = worst
        if bad_exits or aborted or dirty_restarts:
            failed = True

    # A run that was asked to yield a member and never did has measured nothing of
    # the yielded half: with `--trials` below 4 the odd-indexed trials do not exist.
    if args.yield_half and not any(r.get("yielded") for r in all_results.get(head_label, [])):
        print(
            "FAIL (no yielded trial): --yield-half was given but no head trial yielded a "
            "member (the odd-indexed ones do; give --trials of at least 4)"
        )
        failed = True

    baseline_worst = worsts.get(baseline_label, 0.0)
    head_worst = worsts.get(head_label, 0.0)
    print(
        f"\n== worst stop time: {baseline_label} {baseline_worst:.3f}s, {head_label} {head_worst:.3f}s "
        f"(epsilon {STOP_TIME_EPSILON_S}s) =="
    )

    # The regression check is on the paired deltas, not the group worsts
    # above: a trial a busy host slowed moves a group's worst without either
    # build being slower to stop. The builds' trials are interleaved on the same
    # offsets, so the pairs' differences are what a slower stop looks like:
    # their mean, one pair against a wider bound, and the excess of clearly
    # slower pairs over clearly faster ones.
    worst_pair_delta, mean_pair_delta, pair_deltas = pair_and_compare(
        all_results.get(baseline_label, []), all_results.get(head_label, [])
    )
    print(
        f"   excess count (pairs worse than +{STOP_TIME_PAIR_NOISE_S}s minus pairs better than "
        f"-{STOP_TIME_PAIR_NOISE_S}s): {excess_count(pair_deltas):+d}, "
        f"fails at {excess_limit(len(pair_deltas))}"
    )
    failures, notes = judge_pairs(worst_pair_delta, mean_pair_delta, pair_deltas)
    for note in notes:
        print(f"   note: {note}")
    for failure in failures:
        print(failure)
    failed = failed or bool(failures)

    # The two halves, each against the same bars as the whole: faster pairs in one
    # half net against slower pairs in the other in the overall mean and excess count,
    # so a regression that only the yielded trials (or only the others) show can hide.
    if args.yield_half:
        head_results = all_results.get(head_label, [])
        for name, half in (
            ("yielded", [r for r in head_results if r.get("yielded")]),
            ("un-yielded", [r for r in head_results if not r.get("yielded")]),
        ):
            print(f"\n-- the {name} half of the head's trials, against the baseline's on the same offsets --")
            if not half:
                print(f"   no {name} trials; not judged")
                continue
            half_worst, half_mean, half_deltas = pair_and_compare(
                all_results.get(baseline_label, []), half
            )
            print(
                f"   excess count: {excess_count(half_deltas):+d}, "
                f"fails at {excess_limit(len(half_deltas))}"
            )
            half_failures, half_notes = judge_pairs(half_worst, half_mean, half_deltas)
            for note in half_notes:
                print(f"   note: {note}")
            for failure in half_failures:
                print(f"{failure} [{name} half]")
            failed = failed or bool(half_failures)

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(all_results, f, indent=2)

    if failed:
        sys.exit(1)
    print("\nPASS")


if __name__ == "__main__":
    main()
