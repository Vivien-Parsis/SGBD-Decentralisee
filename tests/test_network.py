from pathlib import Path

from fastapi.testclient import TestClient

from src.network.node_server import create_app


def test_node_endpoints_and_membership(tmp_path: Path):
    app = create_app(
        node_id="node-1",
        data_file=str(tmp_path / "node-1.json"),
        peers={"node-1": "http://127.0.0.1:8001"},
        virtual_nodes=20,
    )
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["node_id"] == "node-1"

    put_resp = client.put("/kv", json={"key": "k1", "value": "v1"})
    assert put_resp.status_code == 200

    get_resp = client.get("/kv/k1")
    assert get_resp.status_code == 200
    assert get_resp.json()["value"] == "v1"

    add_resp = client.post("/cluster/nodes", json={"node_id": "node-2", "url": "http://127.0.0.1:8002"})
    assert add_resp.status_code == 200
    assert "node-2" in add_resp.json()["nodes"]

    del_resp = client.delete("/kv/k1", params={"forward": "false"})
    assert del_resp.status_code == 200
    assert client.get("/kv/k1", params={"forward": "false"}).status_code == 404