"""Tests unitaires et HTTP pour T2.3 : Dynamisme du cluster (ajout et retrait à chaud)."""
from pathlib import Path
from fastapi.testclient import TestClient

from src.consensus.raft import InMemoryRaftTransport, NodeRole, RaftNode
from src.consensus.state_machine import RaftStateMachine
from src.network.node_server import create_app
from src.routing.consistent_hashing import ConsistentHashRing
from src.storage.kv_store import KeyValueStore


def _create_node(
    tmp_path: Path,
    node_id: str,
    peers: list[str],
    transport: InMemoryRaftTransport,
    replication_factor: int | None = None,
) -> tuple[RaftNode, RaftStateMachine, ConsistentHashRing]:
    ring = ConsistentHashRing(sorted({node_id, *peers}), virtual_nodes=20)
    rnode = RaftNode(node_id=node_id, peer_ids=peers, transport=transport)
    store = KeyValueStore(tmp_path / f"{node_id}.json")
    sm = RaftStateMachine(
        raft_node=rnode,
        store=store,
        ring=ring,
        replication_factor=replication_factor,
    )
    transport.register(rnode)
    return rnode, sm, ring


def test_hot_add_node_catches_up_log_and_updates_ring(tmp_path):
    transport = InMemoryRaftTransport()
    initial_ids = ["node-1", "node-2", "node-3"]
    raft_nodes: dict[str, RaftNode] = {}
    machines: dict[str, RaftStateMachine] = {}
    rings: dict[str, ConsistentHashRing] = {}

    for nid in initial_ids:
        peers = [p for p in initial_ids if p != nid]
        rnode, sm, ring = _create_node(tmp_path, nid, peers, transport)
        raft_nodes[nid], machines[nid], rings[nid] = rnode, sm, ring

    leader_raft = raft_nodes["node-1"]
    leader_sm = machines["node-1"]
    leader_raft.start_election()

    # Écriture de données avant l'arrivée de node-4
    leader_sm.propose_put("k1", "avant-ajout")
    leader_raft.send_heartbeats()

    # Création et enregistrement à chaud de node-4 (initialement vierge)
    r4, sm4, ring4 = _create_node(tmp_path, "node-4", [], transport)
    raft_nodes["node-4"], machines["node-4"], rings["node-4"] = r4, sm4, ring4

    leader_sm.propose_add_node("node-4", "http://127.0.0.1:8004")

    # Tous les nœuds (1 à 4) connaissent les 4 membres dans leur anneau et ont la clé k1
    expected_members = ["node-1", "node-2", "node-3", "node-4"]
    for nid in expected_members:
        assert rings[nid].get_nodes() == expected_members
        assert machines[nid].get("k1") == "avant-ajout"

    # Écriture après l'ajout sans coupure de service
    leader_sm.propose_put("k2", "apres-ajout")
    leader_raft.send_heartbeats()
    assert machines["node-4"].get("k2") == "apres-ajout"


def test_hot_remove_node_rebalances_keys(tmp_path):
    transport = InMemoryRaftTransport()
    node_ids = ["node-1", "node-2", "node-3"]
    raft_nodes: dict[str, RaftNode] = {}
    machines: dict[str, RaftStateMachine] = {}
    rings: dict[str, ConsistentHashRing] = {}

    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        rnode, sm, ring = _create_node(
            tmp_path, nid, peers, transport, replication_factor=2
        )
        raft_nodes[nid], machines[nid], rings[nid] = rnode, sm, ring

    raft_nodes["node-1"].start_election()
    leader_sm = machines["node-1"]

    # Insertion de plusieurs clés réparties avec facteur de réplication = 2 sur 3 nœuds
    for i in range(10):
        leader_sm.propose_put(f"item:{i}", i)
    raft_nodes["node-1"].send_heartbeats()

    # Retrait à chaud de node-3 : il ne reste que node-1 et node-2 (2 nœuds pour replication_factor=2)
    leader_sm.propose_remove_node("node-3")

    assert rings["node-1"].get_nodes() == ["node-1", "node-2"]
    assert rings["node-2"].get_nodes() == ["node-1", "node-2"]
    assert "node-3" not in raft_nodes["node-1"].peer_ids

    # Grâce au rééquilibrage depuis le journal Raft, node-1 et node-2 possèdent désormais toutes les clés
    for i in range(10):
        assert machines["node-1"].get(f"item:{i}") == i
        assert machines["node-2"].get(f"item:{i}") == i


def test_removing_leader_steps_down_and_allows_new_leader(tmp_path):
    transport = InMemoryRaftTransport()
    node_ids = ["node-1", "node-2", "node-3"]
    raft_nodes: dict[str, RaftNode] = {}
    machines: dict[str, RaftStateMachine] = {}

    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        rnode, sm, _ = _create_node(tmp_path, nid, peers, transport)
        raft_nodes[nid], machines[nid] = rnode, sm

    raft_nodes["node-1"].start_election()
    machines["node-1"].propose_remove_node("node-1")

    assert raft_nodes["node-1"].role == NodeRole.FOLLOWER
    assert "node-1" not in raft_nodes["node-2"].peer_ids
    assert "node-1" not in raft_nodes["node-3"].peer_ids

    # node-2 peut devenir leader du cluster restant (node-2, node-3)
    assert raft_nodes["node-2"].start_election() is True
    machines["node-2"].propose_put("survie", True)
    raft_nodes["node-2"].send_heartbeats()
    assert machines["node-3"].get("survie") is True


def test_http_cluster_membership_endpoints(tmp_path):
    transport = InMemoryRaftTransport()
    r1 = RaftNode("node-1", ["node-2"], transport=transport)
    r2 = RaftNode("node-2", ["node-1"], transport=transport)
    r3 = RaftNode("node-3", [], transport=transport)
    for r in (r1, r2, r3):
        transport.register(r)

    app1 = create_app("node-1", str(tmp_path / "n1.json"), raft_node=r1)
    app2 = create_app("node-2", str(tmp_path / "n2.json"), raft_node=r2)
    app3 = create_app("node-3", str(tmp_path / "n3.json"), raft_node=r3)

    r1.start_election()
    c1 = TestClient(app1)
    c2 = TestClient(app2)
    c3 = TestClient(app3)

    c1.put("/kv", json={"key": "init", "value": 100})

    # Ajout de node-3 via l'endpoint HTTP du leader
    add_res = c1.post(
        "/cluster/nodes",
        json={"node_id": "node-3", "url": "http://127.0.0.1:8003"},
    )
    assert add_res.status_code == 200
    assert add_res.json()["nodes"] == ["node-1", "node-2", "node-3"]
    assert c3.get("/kv/init").json()["value"] == 100

    # Retrait de node-2 via l'endpoint HTTP
    del_res = c1.delete("/cluster/nodes/node-2")
    assert del_res.status_code == 200
    assert del_res.json()["nodes"] == ["node-1", "node-3"]
    assert c2.get("/cluster/nodes").status_code == 200

