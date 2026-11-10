"""Client qui route les opérations vers le nœud responsable."""
from __future__ import annotations

from typing import Any
import httpx

from src.routing.consistent_hashing import ConsistentHashRing


class ClusterClient:
    def __init__(self, nodes: dict[str, str], virtual_nodes: int = 100, timeout: float = 3.0):
        """nodes associe un identifiant de nœud à son URL, ex. http://127.0.0.1:8001."""
        if not nodes:
            raise ValueError("Le cluster doit contenir au moins un nœud.")
        self.nodes = dict(nodes)
        self.ring = ConsistentHashRing(list(nodes), virtual_nodes)
        self.timeout = timeout

    def _url_for(self, key: str) -> str:
        node_id = self.ring.get_node(key)
        return self.nodes[node_id].rstrip("/")

    def put(self, key: str, value: Any) -> dict:
        response = httpx.put(
            f"{self._url_for(key)}/kv",
            json={"key": key, "value": value},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get(self, key: str) -> Any:
        response = httpx.get(f"{self._url_for(key)}/kv/{key}", timeout=self.timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["value"]

    def delete(self, key: str) -> bool:
        response = httpx.delete(f"{self._url_for(key)}/kv/{key}", timeout=self.timeout)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True
