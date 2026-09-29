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
found something to repair, and the head build's worst stop time no worse
than the baseline's worst plus STOP_TIME_EPSILON. `--seed` is random unless
given, and always printed, so a failing run can be reproduced exactly.
"""

import argparse
import json
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

# How much worse the head build's worst stop may be than the baseline's,
# before the matrix reports a regression rather than ordinary run-to-run
# jitter on this machine.
STOP_TIME_EPSILON_S = 0.25

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
                http("GET", self.url("/healthz"), timeout=1)
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


def run_trial(binary, target, index, offset_s, walk_row_ms, serve_walk_ms, seed_batches, trial_label):
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
    }
    try:
        a_env = {
            "KIMMY_TEST_WALK_ROW_MS": str(walk_row_ms),
            "KIMMY_TEST_SERVE_WALK_MS": str(serve_walk_ms),
        }
        for i in range(3):
            seeds = [cluster_ports[j] for j in range(3) if j != i]
            extra = a_env if names[i] == "a" else None
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


def run_matrix(binary, label, trials, walk_row_ms, serve_walk_ms, seed_batches, seed):
    rng = random.Random(seed)
    results = []
    targets = ["requester", "server"]
    per_target = max(1, trials // len(targets))
    n = 0
    for target in targets:
        for i in range(per_target):
            offset = rng.uniform(2.0, 30.0)
            n += 1
            label_i = f"{label}/{target}/{i}"
            print(f"  [{label_i}] offset={offset:.2f}s ...", file=sys.stderr, flush=True)
            r = run_trial(binary, target, i, offset, walk_row_ms, serve_walk_ms, seed_batches, label_i)
            results.append(r)
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
    baseline) and returns the worst."""
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
        return 0.0
    deltas.sort(key=lambda d: d[1], reverse=True)
    worst_key, worst_delta, worst_base, worst_head = deltas[0]
    mean_delta = sum(d[1] for d in deltas) / len(deltas)
    print(
        f"\n== paired comparison ({len(deltas)} pairs, same offset each): "
        f"worst delta {worst_delta:+.3f}s at {worst_key} (baseline {worst_base:.3f}s, "
        f"head {worst_head:.3f}s), mean delta {mean_delta:+.3f}s =="
    )
    return worst_delta


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
    p.add_argument("--json-out", help="write the raw per-trial results here as JSON")
    args = p.parse_args()
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(2**32)
    print(f"seed: {args.seed}", file=sys.stderr)

    baseline_label = build_label("baseline", args.baseline_bin)
    head_label = build_label("head", args.head_bin)
    print(f"baseline: {baseline_label}\nhead: {head_label}", file=sys.stderr)

    all_results = {}
    for label, binary in [(baseline_label, args.baseline_bin), (head_label, args.head_bin)]:
        print(f"\n### {label}: {args.trials} trials ###", file=sys.stderr)
        results = run_matrix(
            binary, label, args.trials, args.walk_row_ms, args.serve_walk_ms, args.seed_batches, args.seed
        )
        all_results[label] = results

    worsts = {}
    failed = False
    for label, results in all_results.items():
        worst, bad_exits, aborted, dirty_restarts = summarize(label, results)
        worsts[label] = worst
        if bad_exits or aborted or dirty_restarts:
            failed = True

    baseline_worst = worsts.get(baseline_label, 0.0)
    head_worst = worsts.get(head_label, 0.0)
    print(
        f"\n== worst stop time: {baseline_label} {baseline_worst:.3f}s, {head_label} {head_worst:.3f}s "
        f"(epsilon {STOP_TIME_EPSILON_S}s) =="
    )

    # The regression check itself is on the paired deltas, not the group
    # worsts above: a handful of harder offsets landing in one build's run
    # and not the other's can move a group's worst or its average without
    # either build actually being slower to stop, and a shared seed makes
    # comparing like-for-like free.
    worst_pair_delta = pair_and_compare(all_results.get(baseline_label, []), all_results.get(head_label, []))
    if worst_pair_delta > STOP_TIME_EPSILON_S:
        print(f"FAIL: the worst same-offset pair regressed by {worst_pair_delta:.3f}s, past epsilon")
        failed = True

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(all_results, f, indent=2)

    if failed:
        sys.exit(1)
    print("\nPASS")


if __name__ == "__main__":
    main()
