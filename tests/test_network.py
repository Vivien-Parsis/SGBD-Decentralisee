from fastapi.testclient import TestClient
from src.network.node_server import create_app


def test_node_http_api(tmp_path):
    client = TestClient(create_app("test-node", str(tmp_path / "store.json")))
    assert client.get("/health").status_code == 200
    assert client.put("/kv", json={"key": "x", "value": 123}).status_code == 200
    assert client.get("/kv/x").json()["value"] == 123
    assert client.delete("/kv/x").status_code == 200
    assert client.get("/kv/x").status_code == 404
