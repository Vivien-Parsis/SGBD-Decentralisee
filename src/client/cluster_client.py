"""Client qui route les opérations vers le nœud responsable et gère la topologie."""
from __future__ import annotations

from typing import Any
import httpx

from src.routing.consistent_hashing import ConsistentHashRing


class ClusterClient:
    def __init__(self, nodes: dict[str, str], virtual_nodes: int = 100, timeout: float = 3.0):
        """nodes associe un identifiant de nœud à son URL, ex. http://127.0.0.1:8001."""
        if not nodes:
            raise ValueError("Le cluster doit contenir au moins un nœud.")
        self.nodes = {k: v.rstrip("/") for k, v in nodes.items()}
        self.ring = ConsistentHashRing(list(self.nodes), virtual_nodes)
        self.timeout = timeout

    def _url_for(self, key: str) -> str:
        node_id = self.ring.get_node(key)
        return self.nodes[node_id]

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

    def add_node(self, node_id: str, url: str, notify_cluster: bool = False) -> None:
        """Ajoute un nœud à chaud côté client (et optionnellement notifie le cluster Raft)."""
        if not node_id or not url:
            raise ValueError("node_id et url sont obligatoires.")
        clean_url = url.rstrip("/")
        if notify_cluster and self.nodes:
            target_url = next(iter(self.nodes.values()))
            response = httpx.post(
                f"{target_url}/cluster/nodes",
                json={"node_id": node_id, "url": clean_url},
                timeout=self.timeout,
            )
            response.raise_for_status()
        self.nodes[node_id] = clean_url
        self.ring.add_node(node_id)

    def remove_node(self, node_id: str, notify_cluster: bool = False) -> None:
        """Retire un nœud à chaud côté client (et optionnellement notifie le cluster Raft)."""
        if notify_cluster:
            remaining_urls = [u for nid, u in self.nodes.items() if nid != node_id]
            if remaining_urls:
                response = httpx.delete(
                    f"{remaining_urls[0]}/cluster/nodes/{node_id}",
                    timeout=self.timeout,
                )
                response.raise_for_status()
        self.nodes.pop(node_id, None)
        self.ring.remove_node(node_id)
