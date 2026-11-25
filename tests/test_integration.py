import socket
import threading
import time
from pathlib import Path

import httpx
import uvicorn

from src.client.cluster_client import ClusterClient
from src.network.node_server import create_app


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_server(app, host: str, port: int):
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            resp = httpx.get(f"http://{host}:{port}/health", timeout=0.5)
            if resp.status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.05)
    return server, thread


def test_forwarding_and_client_failover(tmp_path: Path):
    host = "127.0.0.1"
    port_1 = _get_free_port()
    port_2 = _get_free_port()
    url_1 = f"http://{host}:{port_1}"
    url_2 = f"http://{host}:{port_2}"
    peers = {"node-1": url_1, "node-2": url_2}

    app_1 = create_app(
        node_id="node-1",
        data_file=str(tmp_path / "node-1.json"),
        peers=peers,
        virtual_nodes=30,
    )
    app_2 = create_app(
        node_id="node-2",
        data_file=str(tmp_path / "node-2.json"),
        peers=peers,
        virtual_nodes=30,
    )

    server_1, thread_1 = _start_server(app_1, host, port_1)
    server_2, thread_2 = _start_server(app_2, host, port_2)

    try:
        client = ClusterClient(peers, virtual_nodes=30, timeout=2.0)

        for i in range(10):
            key = f"item-{i}"
            client.put(key, {"index": i})
            assert client.get(key) == {"index": i}

        # Arrêt brutal simulé du nœud 1 puis retrait de node-1 sur node-2
        server_1.should_exit = True
        thread_1.join(timeout=3.0)
        httpx.delete(f"{url_2}/cluster/nodes/node-1", timeout=2.0).raise_for_status()

        surviving_client = ClusterClient({"node-2": url_2}, virtual_nodes=30, timeout=2.0)
        assert surviving_client.put("after-down", "ok")["status"] == "stored"
        assert surviving_client.get("after-down") == "ok"
        assert surviving_client.delete("after-down") is True
    finally:
        server_1.should_exit = True
        server_2.should_exit = True
        thread_1.join(timeout=3.0)
        thread_2.join(timeout=3.0)