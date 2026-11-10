"""Anneau de hachage cohérent avec plusieurs points virtuels par nœud."""
from __future__ import annotations

import hashlib
import bisect


class ConsistentHashRing:
    def __init__(self, nodes: list[str] | None = None, virtual_nodes: int = 100):
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes doit être supérieur ou égal à 1.")
        self.virtual_nodes = virtual_nodes
        self._ring: list[int] = []
        self._owners: dict[int, str] = {}
        self._nodes: set[str] = set()
        for node in nodes or []:
            self.add_node(node)

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        if not node_id:
            raise ValueError("L'identifiant du nœud ne peut pas être vide.")
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for index in range(self.virtual_nodes):
            point = self._hash(f"{node_id}#vn{index}")
            self._owners[point] = node_id
            bisect.insort(self._ring, point)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        self._nodes.remove(node_id)
        points = [point for point, owner in self._owners.items() if owner == node_id]
        for point in points:
            del self._owners[point]
            self._ring.remove(point)

    def get_node(self, key: str) -> str:
        if not self._ring:
            raise RuntimeError("Aucun nœud n'est disponible dans l'anneau.")
        point = self._hash(key)
        index = bisect.bisect_left(self._ring, point)
        if index == len(self._ring):
            index = 0
        return self._owners[self._ring[index]]

    def get_nodes(self) -> list[str]:
        return sorted(self._nodes)

    def get_replicas(self, key: str, count: int) -> list[str]:
        """Retourne jusqu'à count nœuds distincts dans l'ordre de l'anneau."""
        if count < 1:
            raise ValueError("count doit être supérieur ou égal à 1.")
        if not self._ring:
            return []
        point = self._hash(key)
        index = bisect.bisect_left(self._ring, point)
        ordered = self._ring[index:] + self._ring[:index]
        result: list[str] = []
        for ring_point in ordered:
            owner = self._owners[ring_point]
            if owner not in result:
                result.append(owner)
                if len(result) == count:
                    break
        return result
