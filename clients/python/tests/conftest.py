"""A real node to talk to.

The Python client is tested the same way the Rust one is: against a spawned
`kimmyd`, over a socket. Nothing here mocks a response — a client's whole job
is to be right about what comes back from a server, and a fake server is a
statement of what this client already believes.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT_PASSWORD = "python-client-password"
JWT_SECRET = "a-secret-long-enough-for-the-python-client-tests"


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[3]


def _binary() -> pathlib.Path:
    """The `kimmyd` to drive.

    Release first, then debug: either is a real node, and preferring the fast
    one keeps the suite quick when both exist.
    """
    override = os.environ.get("KIMMYD_BINARY")
    if override:
        return pathlib.Path(override)
    for profile in ("release", "debug"):
        candidate = _repo_root() / "target" / profile / "kimmyd"
        if candidate.exists():
            return candidate
    pytest.skip(
        "no kimmyd binary found; run `cargo build` first, or set KIMMYD_BINARY",
        allow_module_level=True,
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    def __init__(self, tmp_path: pathlib.Path, token_ttl: int = 3600) -> None:
        self.port = _free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.dir = tmp_path
        config = f"""
[server]
bind = "127.0.0.1:{self.port}"
mcp = false

[storage]
data_dir = "{tmp_path / 'data'}"

[auth]
jwt_secret = "{JWT_SECRET}"
token_ttl_secs = {token_ttl}
"""
        config_path = tmp_path / "kimmy.toml"
        config_path.write_text(config)
        self.log = open(tmp_path / "node.log", "w")
        self.process = subprocess.Popen(
            [str(_binary()), "--config", str(config_path)],
            env={**os.environ, "KIMMY_ROOT_PASSWORD": ROOT_PASSWORD},
            stdout=self.log,
            stderr=self.log,
        )
        self._wait_ready()

    def _wait_ready(self) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(f"{self.base}/healthz", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"kimmyd exited with {self.process.returncode}; log:\n"
                    + (self.dir / "node.log").read_text()
                )
            time.sleep(0.05)
        raise RuntimeError("kimmyd never became healthy")

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            self.process.kill()
        self.log.close()


@pytest.fixture
def node(tmp_path):
    node = Node(tmp_path)
    yield node
    node.stop()


@pytest.fixture
def short_lived_node(tmp_path):
    """A node whose tokens expire after a second, for the renewal test."""
    node = Node(tmp_path, token_ttl=1)
    yield node
    node.stop()


@pytest.fixture
def db(node):
    from kimmydb import Client

    with Client(node.base, user="root", password=ROOT_PASSWORD) as client:
        yield client
