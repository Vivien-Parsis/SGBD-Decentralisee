from src.routing.consistent_hashing import ConsistentHashRing


def test_routes_keys_and_returns_replicas():
    ring = ConsistentHashRing(["node-1", "node-2", "node-3"], virtual_nodes=20)
    assert ring.get_node("customer:42") in ring.get_nodes()
    replicas = ring.get_replicas("customer:42", 2)
    assert len(replicas) == 2
    assert len(set(replicas)) == 2


def test_add_and_remove_node():
    ring = ConsistentHashRing(["node-1", "node-2"])
    ring.add_node("node-3")
    assert "node-3" in ring.get_nodes()
    ring.remove_node("node-3")
    assert "node-3" not in ring.get_nodes()
