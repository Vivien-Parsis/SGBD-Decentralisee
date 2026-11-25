from src.routing.consistent_hashing import ConsistentHashRing


def test_consistent_hashing_basic_and_replicas():
    ring = ConsistentHashRing(nodes=["node-1", "node-2", "node-3"], virtual_nodes=50)

    owner = ring.get_node("session:42")
    assert owner in {"node-1", "node-2", "node-3"}

    replicas = ring.get_replicas("session:42", count=2)
    assert len(replicas) == 2
    assert len(set(replicas)) == 2
    assert replicas[0] == owner


def test_add_and_remove_node_keeps_stability():
    ring = ConsistentHashRing(nodes=["node-1", "node-2"], virtual_nodes=50)
    keys = [f"key-{i}" for i in range(100)]
    before = {k: ring.get_node(k) for k in keys}

    ring.add_node("node-3")
    assert ring.get_nodes() == ["node-1", "node-2", "node-3"]

    ring.remove_node("node-3")
    assert ring.get_nodes() == ["node-1", "node-2"]
    after = {k: ring.get_node(k) for k in keys}
    assert before == after