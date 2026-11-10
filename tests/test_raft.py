"""Tests unitaires pour T2.1 : Cœur de l'algorithme Raft."""
from src.consensus.raft import (
    InMemoryRaftTransport,
    LogEntry,
    NodeRole,
    RaftNode,
    RequestVoteRequest,
)


def _build_cluster(node_ids: list[str]) -> tuple[dict[str, RaftNode], InMemoryRaftTransport]:
    transport = InMemoryRaftTransport()
    nodes: dict[str, RaftNode] = {}
    for node_id in node_ids:
        peers = [peer for peer in node_ids if peer != node_id]
        node = RaftNode(node_id=node_id, peer_ids=peers, transport=transport)
        nodes[node_id] = node
        transport.register(node)
    return nodes, transport


def test_leader_election_three_nodes():
    nodes, _ = _build_cluster(["node-1", "node-2", "node-3"])

    won = nodes["node-1"].start_election()
    assert won is True
    assert nodes["node-1"].role == NodeRole.LEADER
    assert nodes["node-1"].current_term == 1
    assert nodes["node-2"].role == NodeRole.FOLLOWER
    assert nodes["node-3"].role == NodeRole.FOLLOWER
    assert nodes["node-2"].leader_id == "node-1"
    assert nodes["node-3"].leader_id == "node-1"


def test_log_replication_and_commit_index():
    nodes, _ = _build_cluster(["node-1", "node-2", "node-3"])
    leader = nodes["node-1"]
    leader.start_election()

    entry = leader.append_command({"op": "PUT", "key": "user:1", "value": "Alice"})
    assert entry.index == 1
    assert entry.term == 1

    # Le leader a répliqué sur la majorité : commit_index vaut 1
    assert leader.commit_index == 1
    assert len(nodes["node-2"].log) == 1
    assert len(nodes["node-3"].log) == 1

    # Un heartbeat propage leader_commit aux followers
    leader.send_heartbeats()
    assert nodes["node-2"].commit_index == 1
    assert nodes["node-3"].commit_index == 1
    assert nodes["node-2"].log[0].command == {"op": "PUT", "key": "user:1", "value": "Alice"}


def test_quorum_maintained_with_one_node_down_and_catchup():
    nodes, transport = _build_cluster(["node-1", "node-2", "node-3"])
    leader = nodes["node-1"]
    leader.start_election()

    # Isolation de node-3 (simulation de panne)
    transport.isolated_nodes.add("node-3")

    # Le cluster (node-1 + node-2) a toujours le quorum (2/3)
    leader.append_command({"op": "PUT", "key": "k1", "value": "v1"})
    leader.append_command({"op": "PUT", "key": "k2", "value": "v2"})
    assert leader.commit_index == 2
    assert nodes["node-2"].last_log_index() == 2
    assert nodes["node-3"].last_log_index() == 0

    # Retour de node-3 : le prochain heartbeat rattrape automatiquement le retard
    transport.isolated_nodes.remove("node-3")
    leader.send_heartbeats()
    assert nodes["node-3"].last_log_index() == 2
    assert nodes["node-3"].commit_index == 2


def test_leader_failure_triggers_new_election():
    nodes, transport = _build_cluster(["node-1", "node-2", "node-3"])
    nodes["node-1"].start_election()
    nodes["node-1"].append_command({"op": "PUT", "key": "a", "value": 1})

    # Panne du leader node-1
    transport.isolated_nodes.add("node-1")

    # node-2 déclenche une nouvelle élection au terme 2
    assert nodes["node-2"].start_election() is True
    assert nodes["node-2"].role == NodeRole.LEADER
    assert nodes["node-2"].current_term == 2
    assert nodes["node-3"].leader_id == "node-2"

    # Quand node-1 revient et reçoit un heartbeat du terme 2, il redevient follower
    transport.isolated_nodes.remove("node-1")
    nodes["node-2"].send_heartbeats()
    assert nodes["node-1"].role == NodeRole.FOLLOWER
    assert nodes["node-1"].current_term == 2
    assert nodes["node-1"].leader_id == "node-2"


def test_candidate_with_outdated_log_is_rejected():
    nodes, _ = _build_cluster(["node-1", "node-2", "node-3"])
    # node-2 a une entrée au terme 2
    nodes["node-2"].current_term = 2
    nodes["node-2"].log.append(LogEntry(term=2, index=1, command={"op": "PUT", "key": "x", "value": 1}))

    # node-1 (log vide) demande le vote au terme 2
    response = nodes["node-2"].handle_request_vote(
        RequestVoteRequest(term=2, candidate_id="node-1", last_log_index=0, last_log_term=0)
    )
    assert response.vote_granted is False


def test_tick_triggers_election_and_persistence(tmp_path):
    state_path = tmp_path / "raft_state.json"
    node = RaftNode(
        node_id="solo-node",
        peer_ids=[],
        state_file=state_path,
        election_timeout_range=(0.05, 0.05),
    )
    # Simule l'expiration du timeout d'élection via tick()
    node.tick(now=node._last_contact + 0.10)
    assert node.role == NodeRole.LEADER
    node.append_command({"op": "PUT", "key": "persist", "value": True})

    # Rechargement depuis le disque
    reloaded = RaftNode(node_id="solo-node", peer_ids=[], state_file=state_path)
    assert reloaded.current_term == node.current_term
    assert reloaded.voted_for == "solo-node"
    assert len(reloaded.log) == 1
    assert reloaded.log[0].command["key"] == "persist"

