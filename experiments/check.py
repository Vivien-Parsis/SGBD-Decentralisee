"""Vérificateur : écritures perdues, lectures périmées et divergence entre réplicas."""
from __future__ import annotations

import hashlib
import json

import httpx

from experiments.bench import Op


def acked_writes(ops: list[Op]) -> dict[str, int]:
    return {o.key: o.value for o in ops if o.kind == "put" and o.ok}


def lost_writes(ops: list[Op], read_url_for_key) -> dict:
    """Relit chaque écriture acquittée. Une clé absente ou fausse = écriture perdue."""
    acked = acked_writes(ops)
    lost, wrong = [], []
    with httpx.Client(timeout=2.0, trust_env=False) as client:
        for key, value in acked.items():
            r = client.get(f"{read_url_for_key(key)}/kv/{key}")
            if r.status_code == 404:
                lost.append(key)
            elif r.status_code == 200 and r.json().get("value") != value:
                wrong.append(key)
    return {"acked_writes": len(acked), "lost_writes": len(lost), "wrong_values": len(wrong),
            "lost_examples": lost[:5]}


def stale_reads(ops: list[Op]) -> dict:
    """Lectures d'une clé déjà acquittée qui renvoient 404 ou une autre valeur."""
    reads = [o for o in ops if o.kind == "get" and o.status in ("200", "404")]
    stale = [o for o in reads if not o.ok]
    return {"reads_answered": len(reads), "stale_reads": len(stale)}


def replica_fingerprints(node_urls: dict[str, str], keys: list[str]) -> dict[str, str]:
    """Empreinte (hash) du contenu de chaque nœud sur l'ensemble de clés donné."""
    prints: dict[str, str] = {}
    with httpx.Client(timeout=2.0, trust_env=False) as client:
        for node_id, url in node_urls.items():
            content = {}
            for key in sorted(keys):
                try:
                    r = client.get(f"{url}/kv/{key}")
                except httpx.HTTPError:
                    content = None
                    break
                content[key] = r.json().get("value") if r.status_code == 200 else None
            prints[node_id] = "injoignable" if content is None else hashlib.sha256(
                json.dumps(content, sort_keys=True).encode()).hexdigest()[:12]
    return prints


def divergence(node_urls: dict[str, str], keys: list[str]) -> dict:
    """Compare les réplicas : avec RF = N, tous les nœuds doivent avoir la même empreinte."""
    prints = replica_fingerprints(node_urls, keys)
    distinct = {p for p in prints.values() if p != "injoignable"}
    return {"fingerprints": prints, "replicas_identical": len(distinct) <= 1}
