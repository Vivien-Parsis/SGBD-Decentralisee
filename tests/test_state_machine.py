"""Tests unitaires pour T2.2 : Machine à états et synchronisation KeyValueStore <-> Raft."""
from pathlib import Path
from fastapi.testclient import TestClient
import pytest

from src.consensus.raft import InMemoryRaftTransport, RaftNode
from src.consensus.state_machine import (
    NotLeaderError,
    QuorumNotReachedError,
    RaftStateMachine,
)
from src.network.node_server import create_app
from src.routing.consistent_hashing import ConsistentHashRing
from src.storage.kv_store import KeyValueStore


def _build_replicated_stores(
    tmp_path: Path,
    node_ids: list[str],
    ring: ConsistentHashRing | None = None,
    replication_factor: int | None = None,
) -> tuple[dict[str, RaftNode], dict[str, RaftStateMachine], InMemoryRaftTransport]:
    transport = InMemoryRaftTransport()
    raft_nodes: dict[str, RaftNode] = {}
    machines: dict[str, RaftStateMachine] = {}

    for node_id in node_ids:
        peers = [p for p in node_ids if p != node_id]
        rnode = RaftNode(node_id=node_id, peer_ids=peers, transport=transport)
        store = KeyValueStore(tmp_path / f"{node_id}.json")
        sm = RaftStateMachine(
            raft_node=rnode,
            store=store,
            ring=ring,
            replication_factor=replication_factor,
        )
        raft_nodes[node_id] = rnode
        machines[node_id] = sm
        transport.register(rnode)

    return raft_nodes, machines, transport


def test_state_machine_replicates_put_and_delete_across_cluster(tmp_path):
    raft_nodes, machines, _ = _build_replicated_stores(
        tmp_path, ["node-1", "node-2", "node-3"]
    )
    leader_raft = raft_nodes["node-1"]
    leader_sm = machines["node-1"]
    leader_raft.start_election()

    # Écriture via le leader
    entry = leader_sm.propose_put("ville", "Lyon")
    assert entry.index == 1
    assert leader_sm.get("ville") == "Lyon"

    # Le heartbeat propage leader_commit aux followers qui appliquent l'entrée sur leur KeyValueStore
    leader_raft.send_heartbeats()
    assert machines["node-2"].get("ville") == "Lyon"
    assert machines["node-3"].get("ville") == "Lyon"

    # Suppression via le leader
    deleted = leader_sm.propose_delete("ville")
    assert deleted is True
    leader_raft.send_heartbeats()
    assert machines["node-1"].get("ville") is None
    assert machines["node-2"].get("ville") is None
    assert machines["node-3"].get("ville") is None


def test_follower_rejects_direct_write(tmp_path):
    raft_nodes, machines, _ = _build_replicated_stores(
        tmp_path, ["node-1", "node-2", "node-3"]
    )
    raft_nodes["node-1"].start_election()

    with pytest.raises(NotLeaderError) as exc_info:
        machines["node-2"].propose_put("k", "v")
    assert exc_info.value.leader_id == "node-1"


def test_isolated_leader_cannot_commit_to_store(tmp_path):
    raft_nodes, machines, transport = _build_replicated_stores(
        tmp_path, ["node-1", "node-2", "node-3"]
    )
    raft_nodes["node-1"].start_election()

    # Le leader node-1 est coupé des deux autres nœuds (pas de quorum)
    transport.isolated_nodes.add("node-1")

    with pytest.raises(QuorumNotReachedError):
        machines["node-1"].propose_put("secret", 42)

    # La donnée non commise ne doit pas être appliquée au KeyValueStore
    assert machines["node-1"].get("secret") is None


def test_state_machine_with_consistent_hash_ring_replication_factor(tmp_path):
    node_ids = ["node-1", "node-2", "node-3"]
    ring = ConsistentHashRing(node_ids, virtual_nodes=20)
    raft_nodes, machines, _ = _build_replicated_stores(
        tmp_path, node_ids, ring=ring, replication_factor=2
    )
    raft_nodes["node-1"].start_election()

    key = "client:99"
    expected_replicas = set(ring.get_replicas(key, 2))
    machines["node-1"].propose_put(key, {"solde": 500})
    raft_nodes["node-1"].send_heartbeats()

    stored_on = {nid for nid, sm in machines.items() if sm.contains(key)}
    assert stored_on == expected_replicas
    assert len(stored_on) == 2


def test_http_raft_endpoints_and_follower_conflict(tmp_path):
    transport = InMemoryRaftTransport()
    r1 = RaftNode("node-1", ["node-2", "node-3"], transport=transport)
    r2 = RaftNode("node-2", ["node-1", "node-3"], transport=transport)
    r3 = RaftNode("node-3", ["node-1", "node-2"], transport=transport)
    for r in (r1, r2, r3):
        transport.register(r)

    app1 = create_app("node-1", str(tmp_path / "n1.json"), raft_node=r1)
    app2 = create_app("node-2", str(tmp_path / "n2.json"), raft_node=r2)
    create_app("node-3", str(tmp_path / "n3.json"), raft_node=r3)

    r1.start_election()

    c1 = TestClient(app1)
    c2 = TestClient(app2)

    assert c1.get("/raft/status").json()["role"] == "leader"
    assert c2.get("/raft/status").json()["role"] == "follower"

    # Écriture sur le leader
    res = c1.put("/kv", json={"key": "lang", "value": "python"})
    assert res.status_code == 200
    r1.send_heartbeats()

    # La donnée est lisible sur le follower après réplication Raft
    assert c2.get("/kv/lang").json()["value"] == "python"

    # Écriture directe sur un follower sans URL de redirection configurée -> 409 Conflict
    conflict = c2.put("/kv", json={"key": "x", "value": 1})
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["leader_id"] == "node-1"

