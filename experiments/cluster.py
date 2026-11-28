"""Lanceur de cluster local : démarre, tue, relance et partitionne des nœuds.

Chaque nœud est un processus `python -m src.main` séparé, ce qui simule
une machine. Fonctionne sous Windows, macOS et Linux.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Node:
    node_id: str
    port: int
    process: subprocess.Popen | None = None
    log_path: Path | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@dataclass
class Cluster:
    """Cluster de N nœuds locaux, avec un facteur de réplication optionnel."""

    size: int = 3
    rf: int | None = None
    base_port: int = 9001
    # Hors du dépôt (donc hors OneDrive) : les fichiers Raft sont réécrits en continu.
    workdir: Path = field(default_factory=lambda: Path(tempfile.gettempdir()) / "sgbd-runtime")
    election_min_ms: int = 300
    election_max_ms: int = 600
    nodes: dict[str, Node] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for i in range(1, self.size + 1):
            node_id = f"node-{i}"
            self.nodes[node_id] = Node(node_id, self.base_port + i - 1)
        self._client = httpx.Client(timeout=1.0, trust_env=False)

    # ------------------------------------------------------------------ cycle de vie
    def peers_arg(self) -> str:
        return ",".join(f"{n.node_id}={n.url}" for n in self.nodes.values())

    def start(self, clean: bool = True, timeout: float = 20.0) -> str:
        """Démarre tous les nœuds et attend qu'un leader soit élu. Retourne son id."""
        if clean and self.workdir.exists():
            shutil.rmtree(self.workdir, ignore_errors=True)
        self.workdir.mkdir(parents=True, exist_ok=True)
        for node in self.nodes.values():
            self._spawn(node)
        return self.wait_for_leader(timeout=timeout)

    def _spawn(self, node: Node) -> None:
        node.log_path = self.workdir / f"{node.node_id}.log"
        env = dict(os.environ)
        env["RAFT_ELECTION_MIN_MS"] = str(self.election_min_ms)
        env["RAFT_ELECTION_MAX_MS"] = str(self.election_max_ms)
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [
            sys.executable, "-m", "src.main",
            "--node-id", node.node_id,
            "--port", str(node.port),
            "--peers", self.peers_arg(),
            "--data-file", str(self.workdir / f"{node.node_id}.json"),
        ]
        if self.rf is not None:
            cmd += ["--rf", str(self.rf)]
        log = open(node.log_path, "ab")
        node.process = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)

    def kill(self, node_id: str) -> float:
        """Arrêt brutal (équivalent de kill -9). Retourne l'instant de l'arrêt."""
        node = self.nodes[node_id]
        t = time.perf_counter()
        if node.process and node.process.poll() is None:
            node.process.kill()
            node.process.wait(timeout=10)
        node.process = None
        return t

    def restart(self, node_id: str) -> None:
        """Relance un nœud arrêté en gardant ses fichiers (log Raft et données)."""
        self._spawn(self.nodes[node_id])

    def stop(self) -> None:
        for node in self.nodes.values():
            if node.process and node.process.poll() is None:
                node.process.kill()
                try:
                    node.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
            node.process = None
        self._client.close()

    def __enter__(self) -> "Cluster":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    # ------------------------------------------------------------------ observation
    def alive(self) -> list[str]:
        return [n.node_id for n in self.nodes.values() if n.process and n.process.poll() is None]

    def status(self, node_id: str) -> dict | None:
        try:
            r = self._client.get(f"{self.nodes[node_id].url}/raft/status")
            return r.json() if r.status_code == 200 else None
        except httpx.HTTPError:
            return None

    def leader(self, among: list[str] | None = None) -> str | None:
        """Retourne le leader reconnu (celui qui se dit leader avec le plus grand term)."""
        best: tuple[int, str] | None = None
        for node_id in among or self.alive():
            st = self.status(node_id)
            if st and st.get("role") == "leader":
                if best is None or st["term"] > best[0]:
                    best = (st["term"], node_id)
        return best[1] if best else None

    def wait_for_leader(self, timeout: float = 20.0, among: list[str] | None = None,
                        exclude: str | None = None, poll: float = 0.02) -> str:
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            candidates = [n for n in (among or self.alive()) if n != exclude]
            lead = self.leader(candidates)
            if lead:
                return lead
            time.sleep(poll)
        raise TimeoutError("Aucun leader élu dans le délai imparti.")

    # ------------------------------------------------------------------ pannes réseau
    def partition(self, group_a: list[str], group_b: list[str]) -> None:
        """Coupe toute communication Raft entre group_a et group_b (dans les deux sens)."""
        for node_id in group_a:
            self._client.post(f"{self.nodes[node_id].url}/admin/partition", json={"blocked": group_b})
        for node_id in group_b:
            self._client.post(f"{self.nodes[node_id].url}/admin/partition", json={"blocked": group_a})

    def heal(self) -> None:
        for node_id in self.alive():
            try:
                self._client.delete(f"{self.nodes[node_id].url}/admin/partition")
            except httpx.HTTPError:
                pass

    def url(self, node_id: str) -> str:
        return self.nodes[node_id].url
