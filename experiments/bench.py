"""Générateur de charge : envoie des put/get en parallèle et enregistre chaque opération."""
from __future__ import annotations

import random
import statistics
import threading
import time
from dataclasses import dataclass
from typing import Callable

import httpx


@dataclass
class Op:
    worker: int
    kind: str          # "put" ou "get"
    key: str
    value: int | None  # valeur écrite, ou valeur lue
    t_start: float     # secondes depuis le début du test
    t_end: float
    ok: bool           # True = acquitté (put 200) ou lu avec la bonne valeur (get)
    status: str        # code HTTP, "timeout" ou "error"
    node: str          # URL du nœud contacté

    @property
    def latency_ms(self) -> float:
        return (self.t_end - self.t_start) * 1000


class Workload:
    """Charge mixte lectures/écritures.

    Chaque écriture utilise une clé unique (k-<worker>-<n>) : on peut ainsi vérifier
    après coup qu'aucune écriture acquittée n'a été perdue. Chaque lecture relit une
    clé déjà acquittée par le même worker : une réponse 404 ou une valeur différente
    est une lecture périmée (stale read).
    """

    def __init__(self, targets: Callable[[int], list[str]], threads: int = 8,
                 read_ratio: float = 0.5, timeout: float = 2.0, seed: int = 42,
                 read_router: Callable[[str], str | None] | None = None):
        self.targets = targets          # fonction worker -> liste d'URLs à contacter
        # Optionnel : envoie chaque lecture au nœud qui stocke la clé (anneau de hachage).
        # Utile quand RF < N, car un nœud ne répond qu'avec ses données locales.
        self.read_router = read_router
        self.threads = threads
        self.read_ratio = read_ratio
        self.timeout = timeout
        self.seed = seed
        self.ops: list[Op] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.t0 = 0.0

    def now(self) -> float:
        return time.perf_counter() - self.t0

    def _worker(self, wid: int) -> None:
        rng = random.Random(self.seed * 1000 + wid)
        client = httpx.Client(timeout=self.timeout, trust_env=False)
        acked: list[tuple[str, int]] = []
        seq = 0
        local: list[Op] = []
        while not self._stop.is_set():
            urls = self.targets(wid)
            if not urls:
                time.sleep(0.01)
                continue
            url = rng.choice(urls)
            if acked and rng.random() < self.read_ratio:
                key, expected = rng.choice(acked)
                if self.read_router is not None:
                    url = self.read_router(key) or url
                t1 = self.now()
                try:
                    r = client.get(f"{url}/kv/{key}")
                    t2 = self.now()
                    value = r.json().get("value") if r.status_code == 200 else None
                    local.append(Op(wid, "get", key, value, t1, t2,
                                    r.status_code == 200 and value == expected, str(r.status_code), url))
                except httpx.TimeoutException:
                    local.append(Op(wid, "get", key, None, t1, self.now(), False, "timeout", url))
                except httpx.HTTPError:
                    local.append(Op(wid, "get", key, None, t1, self.now(), False, "error", url))
                    time.sleep(0.01)
            else:
                seq += 1
                key = f"k-{wid}-{seq}"
                t1 = self.now()
                try:
                    r = client.put(f"{url}/kv", json={"key": key, "value": seq})
                    t2 = self.now()
                    ok = r.status_code == 200
                    local.append(Op(wid, "put", key, seq, t1, t2, ok, str(r.status_code), url))
                    if ok:
                        acked.append((key, seq))
                except httpx.TimeoutException:
                    local.append(Op(wid, "put", key, seq, t1, self.now(), False, "timeout", url))
                except httpx.HTTPError:
                    local.append(Op(wid, "put", key, seq, t1, self.now(), False, "error", url))
                    time.sleep(0.01)
        client.close()
        with self._lock:
            self.ops.extend(local)

    def start(self) -> None:
        self.t0 = time.perf_counter()
        self._threads = [threading.Thread(target=self._worker, args=(i,), daemon=True)
                         for i in range(self.threads)]
        for t in self._threads:
            t.start()

    def stop(self) -> list[Op]:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=self.timeout + 5)
        self.ops.sort(key=lambda o: o.t_start)
        return self.ops

    def run_for(self, seconds: float) -> list[Op]:
        self.start()
        time.sleep(seconds)
        return self.stop()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    return values[f] + (values[c] - values[f]) * (k - f)


def summarize(ops: list[Op], t_from: float = 0.0, t_to: float | None = None) -> dict:
    """Débit et latences sur la fenêtre [t_from, t_to] (la phase de préchauffage est exclue)."""
    window = [o for o in ops if o.t_start >= t_from and (t_to is None or o.t_end <= t_to)]
    if not window:
        return {}
    duration = (t_to or max(o.t_end for o in window)) - t_from
    res: dict = {"duration_s": round(duration, 2), "ops_total": len(window)}
    ok = [o for o in window if o.ok]
    res["throughput_ops_s"] = round(len(ok) / duration, 1)
    res["error_rate"] = round(1 - len(ok) / len(window), 4)
    for kind in ("put", "get"):
        lat = [o.latency_ms for o in window if o.kind == kind and o.ok]
        res[f"{kind}_count"] = len(lat)
        res[f"{kind}_p50_ms"] = round(percentile(lat, 50), 2)
        res[f"{kind}_p95_ms"] = round(percentile(lat, 95), 2)
        res[f"{kind}_p99_ms"] = round(percentile(lat, 99), 2)
        res[f"{kind}_mean_ms"] = round(statistics.fmean(lat), 2) if lat else float("nan")
    return res
