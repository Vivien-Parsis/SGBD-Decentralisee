"""Machine à états synchronisant le stockage clé-valeur et la topologie avec Raft."""
from __future__ import annotations

import threading
from typing import Any

from src.consensus.raft import LogEntry, NodeRole, RaftNode
from src.routing.consistent_hashing import ConsistentHashRing
from src.storage.kv_store import KeyValueStore


class QuorumNotReachedError(RuntimeError):
    """Erreur levée lorsqu'une opération n'a pas pu être validée par une majorité de nœuds."""


class NotLeaderError(RuntimeError):
    """Erreur levée lorsqu'une écriture est demandée à un nœud qui n'est pas leader."""

    def __init__(self, node_id: str, leader_id: str | None = None):
        self.node_id = node_id
        self.leader_id = leader_id
        super().__init__(
            f"Le nœud {node_id} n'est pas le leader (leader actuel : {leader_id})."
        )


class RaftStateMachine:
    """Synchronise un KeyValueStore local et la configuration du cluster avec RaftNode."""

    def __init__(
        self,
        raft_node: RaftNode,
        store: KeyValueStore,
        ring: ConsistentHashRing | None = None,
        replication_factor: int | None = None,
    ):
        self.raft_node = raft_node
        self.store = store
        self.ring = ring
        self.replication_factor = replication_factor
        self._lock = threading.RLock()
        self._applied_results: dict[int, Any] = {}

        # Branchement du callback de commit Raft sur la machine à états
        self.raft_node.on_commit_callback = self.apply_entry

    def _should_store_key(self, key: str) -> bool:
        """Vérifie si ce nœud doit stocker la clé selon l'anneau et le facteur de réplication."""
        if self.ring is None or self.replication_factor is None:
            return True
        nodes = self.ring.get_nodes()
        if not nodes or self.replication_factor >= len(nodes):
            return True
        replicas = self.ring.get_replicas(key, self.replication_factor)
        return self.raft_node.node_id in replicas

    def _rebalance_store_from_log(self, up_to_index: int) -> None:
        """Recalcule l'état des clés à partir du journal Raft après un changement de topologie."""
        expected_state: dict[str, Any] = {}
        for idx in range(1, up_to_index + 1):
            entry = self.raft_node.entry_at(idx)
            if entry is None:
                continue
            cmd = entry.command
            op = cmd.get("op")
            if op == "PUT":
                expected_state[cmd["key"]] = cmd["value"]
            elif op == "DELETE":
                expected_state.pop(cmd["key"], None)

        # Synchronise les clés dont ce nœud est désormais responsable
        for key, value in expected_state.items():
            if self._should_store_key(key):
                if self.store.get(key) != value or not self.store.contains(key):
                    self.store.put(key, value)
            elif self.ring is not None and self.replication_factor is not None:
                if self.store.contains(key):
                    self.store.delete(key)

    def apply_entry(self, entry: LogEntry) -> Any:
        """Applique une entrée commise du journal Raft sur le KeyValueStore ou la topologie."""
        with self._lock:
            command = entry.command
            op = command.get("op")
            result: Any = None

            if op == "PUT":
                key = command["key"]
                value = command["value"]
                if self._should_store_key(key):
                    self.store.put(key, value)
                    result = True
                else:
                    result = False
            elif op == "DELETE":
                key = command["key"]
                if self._should_store_key(key):
                    result = self.store.delete(key)
                else:
                    result = False
            elif op == "ADD_NODE":
                target_id = command["node_id"]
                url = command.get("url")
                members = command.get("members", [])
                self.raft_node.add_peer_local(target_id, url)
                for member_id in members:
                    self.raft_node.add_peer_local(member_id)
                if self.ring is not None:
                    self.ring.add_node(target_id)
                    for member_id in members:
                        self.ring.add_node(member_id)
                self._rebalance_store_from_log(entry.index)
                result = True
            elif op == "REMOVE_NODE":
                target_id = command["node_id"]
                self.raft_node.remove_peer_local(target_id)
                if self.ring is not None:
                    self.ring.remove_node(target_id)
                self._rebalance_store_from_log(entry.index)
                result = True

            self._applied_results[entry.index] = result
            return result

    def propose_put(self, key: str, value: Any) -> LogEntry:
        """Soumet une opération PUT au consensus Raft et vérifie qu'elle est commise."""
        KeyValueStore._validate_key(key)
        if self.raft_node.role != NodeRole.LEADER:
            raise NotLeaderError(self.raft_node.node_id, self.raft_node.leader_id)

        entry = self.raft_node.append_command(
            {"op": "PUT", "key": key, "value": value}
        )
        if self.raft_node.commit_index < entry.index:
            raise QuorumNotReachedError(
                f"Quorum non atteint pour l'entrée {entry.index} (commit_index={self.raft_node.commit_index})."
            )
        return entry

    def propose_delete(self, key: str) -> bool:
        """Soumet une opération DELETE au consensus Raft et retourne si la clé existait."""
        KeyValueStore._validate_key(key)
        if self.raft_node.role != NodeRole.LEADER:
            raise NotLeaderError(self.raft_node.node_id, self.raft_node.leader_id)

        existed_before = self.store.contains(key)
        entry = self.raft_node.append_command({"op": "DELETE", "key": key})
        if self.raft_node.commit_index < entry.index:
            raise QuorumNotReachedError(
                f"Quorum non atteint pour l'entrée {entry.index} (commit_index={self.raft_node.commit_index})."
            )
        with self._lock:
            return bool(self._applied_results.get(entry.index, existed_before))

    def propose_add_node(self, node_id: str, url: str | None = None) -> LogEntry:
        """Ajoute un nœud à chaud via le consensus Raft et synchronise son journal."""
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("L'identifiant du nœud doit être une chaîne non vide.")
        if self.raft_node.role != NodeRole.LEADER:
            raise NotLeaderError(self.raft_node.node_id, self.raft_node.leader_id)

        members = sorted(
            {self.raft_node.node_id, *self.raft_node.peer_ids, node_id}
        )
        entry = self.raft_node.append_command(
            {
                "op": "ADD_NODE",
                "node_id": node_id,
                "url": url,
                "members": members,
            }
        )
        if self.raft_node.commit_index < entry.index:
            raise QuorumNotReachedError(
                f"Quorum non atteint pour l'ajout du nœud {node_id}."
            )

        # Réplique immédiatement le journal complet vers le nouveau nœud et propage le commit
        self.raft_node.replicate_to_peers()
        self.raft_node.send_heartbeats()
        return entry

    def propose_remove_node(self, node_id: str) -> LogEntry:
        """Retire un nœud à chaud de la configuration du cluster via le consensus Raft."""
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("L'identifiant du nœud doit être une chaîne non vide.")
        if self.raft_node.role != NodeRole.LEADER:
            raise NotLeaderError(self.raft_node.node_id, self.raft_node.leader_id)

        entry = self.raft_node.append_command(
            {"op": "REMOVE_NODE", "node_id": node_id}
        )
        if self.raft_node.commit_index < entry.index:
            raise QuorumNotReachedError(
                f"Quorum non atteint pour le retrait du nœud {node_id}."
            )

        # Propage le commit aux pairs restants
        self.raft_node.send_heartbeats()
        if node_id == self.raft_node.node_id and self.raft_node.role == NodeRole.LEADER:
            with self.raft_node._lock:
                self.raft_node._step_down(self.raft_node.current_term)
        return entry

    def get(self, key: str) -> Any | None:
        return self.store.get(key)

    def contains(self, key: str) -> bool:
        return self.store.contains(key)
