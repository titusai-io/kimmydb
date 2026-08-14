#!/usr/bin/env python3
"""Run one set of scenarios three ways, and compare the answers.

Three clients exist. They passed matching scenarios only because they were
written to match, and nothing enforced it — which is the drift this suite
closes, and the kind a user finds first.

# How it works

`scenarios.json` declares every scenario and the observations a correct client
must produce. Each client ships a small **driver** that runs a named scenario
and prints its observations as JSON. This runner:

1. asks every driver which scenarios it implements, and fails on any that the
   declared list has and a driver does not — coverage drift;
2. starts a fresh node per scenario, so no scenario inherits another's data;
3. runs the scenario against every driver and compares its observations to the
   expectations — behavioural drift.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions. There is one oracle here and three
answers to it.

    ./run.py                       # everything
    ./run.py --client go           # one client
    ./run.py --scenario paging_walks_everything
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SPEC = json.loads((HERE / "scenarios.json").read_text())
PASSWORD = SPEC["password"]
DEAD = "http://127.0.0.1:1"  # reserved; nothing listens there

# Each driver is "how to invoke this client", and nothing else. Adding a fourth
# language is a line here plus a driver that speaks the same two commands.
def python_driver() -> list[str]:
    """How to run the Python driver with its dependencies present.

    Through `uv` when it is available, which is how the package's own tests and
    CI run: the driver imports `httpx`, and the ambient interpreter has no
    reason to have it. Falling back to this interpreter is for an environment
    where the package is already installed.
    """
    driver = str(ROOT / "clients" / "python" / "conformance_driver.py")
    if shutil.which("uv"):
        return ["uv", "run", "--quiet", "--project", str(ROOT / "clients" / "python"),
                "python", driver]
    return [sys.executable, driver]


DRIVERS = {
    "rust": [str(ROOT / "target" / "release" / "examples" / "conformance")],
    "python": python_driver(),
    "go": [str(ROOT / "clients" / "go" / "conformance-driver")],
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """A real kimmyd, started fresh for one scenario."""

    def __init__(self, directory: pathlib.Path, token_ttl: int = 3600) -> None:
        binary = os.environ.get("KIMMYD_BINARY", str(ROOT / "target" / "release" / "kimmyd"))
        if not pathlib.Path(binary).exists():
            sys.exit(f"no kimmyd at {binary}; run `cargo build --release` first")

        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        directory.mkdir(parents=True, exist_ok=True)
        config = directory / "kimmy.toml"
        config.write_text(f"""
[server]
bind = "127.0.0.1:{self.port}"
mcp = false

[storage]
data_dir = "{directory / 'data'}"

[auth]
jwt_secret = "a-secret-long-enough-for-the-conformance-suite"
token_ttl_secs = {token_ttl}
""")
        self.log = open(directory / "node.log", "w")
        self.process = subprocess.Popen(
            [binary, "--config", str(config)],
            env={**os.environ, "KIMMY_ROOT_PASSWORD": PASSWORD},
            stdout=self.log,
            stderr=self.log,
        )
        self._wait_ready(directory)

    def _wait_ready(self, directory: pathlib.Path) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{self.base}/healthz", timeout=1) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, OSError):
                pass
            if self.process.poll() is not None:
                sys.exit(f"kimmyd exited; log:\n{(directory / 'node.log').read_text()}")
            time.sleep(0.05)
        sys.exit("kimmyd never became healthy")

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.log.close()


def driver_scenarios(client: str) -> list[str]:
    result = subprocess.run(
        DRIVERS[client] + ["list"], capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        sys.exit(f"{client}: `list` failed:\n{result.stderr}")
    return json.loads(result.stdout)


def run_scenario(client: str, scenario: str, base: str) -> dict:
    result = subprocess.run(
        DRIVERS[client] + ["run", scenario, base, DEAD],
        capture_output=True,
        text=True,
        timeout=180,
        env={**os.environ, "KIMMY_ROOT_PASSWORD": PASSWORD},
    )
    stdout = result.stdout.strip()
    if not stdout:
        return {"error": f"no output (exit {result.returncode})\n{result.stderr[-800:]}"}
    try:
        return json.loads(stdout.splitlines()[-1])
    except json.JSONDecodeError as e:
        return {"error": f"unparseable output: {e}\n{stdout[-800:]}"}


def compare(expected: dict, observed: dict) -> list[str]:
    """Every expectation, against what the driver saw.

    Extra observations are allowed — a client may report more than it was asked
    about. Missing or different ones are not.
    """
    if "error" in observed:
        return [f"driver failed: {observed['error']}"]
    problems = []
    for key, want in expected.items():
        got = observed.get(key, "<missing>")
        if got != want:
            problems.append(f"{key}: expected {want!r}, observed {got!r}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", action="append", choices=sorted(DRIVERS))
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--work-dir", default=None)
    args = parser.parse_args()

    clients = args.client or sorted(DRIVERS)
    scenarios = [s for s in SPEC["scenarios"] if not args.scenario or s["id"] in args.scenario]
    if not scenarios:
        sys.exit("no scenarios matched")

    # A fresh tree per run. Reusing it meant a second run of a scenario started
    # a node on the first run's data — which showed up as every client failing
    # to create a collection that already existed, and looked exactly like a
    # client defect until the runner was suspected.
    work = pathlib.Path(args.work_dir or "/tmp/kimmy-conformance")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    # Coverage first: a client that has quietly stopped implementing a scenario
    # is a failure, not a silence.
    declared = {s["id"] for s in SPEC["scenarios"]}
    failures: list[str] = []
    for client in clients:
        implemented = set(driver_scenarios(client))
        if missing := declared - implemented:
            failures.append(f"{client}: does not implement {sorted(missing)}")
        if extra := implemented - declared:
            failures.append(f"{client}: implements undeclared scenarios {sorted(extra)}")
    if failures:
        for failure in failures:
            print(f"COVERAGE  {failure}")
        return 1

    print(f"{len(scenarios)} scenarios x {len(clients)} clients: {', '.join(clients)}\n")
    results: dict[str, dict[str, list[str]]] = {}

    for scenario in scenarios:
        node_settings = scenario.get("node", {})
        results[scenario["id"]] = {}
        for client in clients:
            directory = work / scenario["id"] / client
            node = Node(directory, token_ttl=node_settings.get("token_ttl_secs", 3600))
            try:
                observed = run_scenario(client, scenario["id"], node.base)
            finally:
                node.stop()
            problems = compare(scenario["expect"], observed)
            results[scenario["id"]][client] = problems

        row = "  ".join(
            f"{client}:{'ok' if not results[scenario['id']][client] else 'FAIL'}"
            for client in clients
        )
        print(f"{scenario['id']:<46} {row}")

    print()
    failed = 0
    for scenario_id, per_client in results.items():
        for client, problems in per_client.items():
            if problems:
                failed += 1
                print(f"FAIL {scenario_id} [{client}]")
                for problem in problems:
                    print(f"       {problem}")

    total = len(scenarios) * len(clients)
    if failed:
        print(f"\n{failed} of {total} runs disagreed with the declared behaviour")
        return 1
    print(f"{total} runs, three clients, one set of scenarios: no disagreements")
    return 0


if __name__ == "__main__":
    sys.exit(main())
