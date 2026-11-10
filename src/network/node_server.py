"""Serveur HTTP d'un nœud de stockage synchronisé par Raft."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
import httpx
from pydantic import BaseModel

from src.consensus.raft import (
    AppendEntriesRequest,
    HttpRaftTransport,
    NodeRole,
    RaftNode,
    RequestVoteRequest,
)
from src.consensus.state_machine import (
    NotLeaderError,
    QuorumNotReachedError,
    RaftStateMachine,
)
from src.routing.consistent_hashing import ConsistentHashRing
from src.storage.kv_store import KeyValueStore


class PutRequest(BaseModel):
    key: str
    value: Any


class AddNodeRequest(BaseModel):
    node_id: str
    url: str | None = None


def _parse_peers_env(raw_peers: str | None, current_node_id: str) -> dict[str, str]:
    """Parse la variable PEERS au format 'node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003'."""
    if not raw_peers:
        return {}
    peers: dict[str, str] = {}
    for item in raw_peers.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        peer_id, url = item.split("=", 1)
        peer_id = peer_id.strip()
        url = url.strip().rstrip("/")
        if peer_id and peer_id != current_node_id and url:
            peers[peer_id] = url
    return peers


def create_app(
    node_id: str | None = None,
    data_file: str | None = None,
    peers: dict[str, str] | None = None,
    raft_node: RaftNode | None = None,
    replication_factor: int | None = None,
) -> FastAPI:
    node_id = node_id or os.getenv("NODE_ID", "node-1")
    data_file = data_file or os.getenv("DATA_FILE", f"data/{node_id}.json")
    store = KeyValueStore(Path(data_file))

    peer_map = (
        {k: v.rstrip("/") for k, v in peers.items() if k != node_id}
        if peers is not None
        else _parse_peers_env(os.getenv("PEERS"), node_id)
    )

    if raft_node is None:
        transport = HttpRaftTransport(peer_map) if peer_map else None
        raft_node = RaftNode(
            node_id=node_id,
            peer_ids=list(peer_map.keys()),
            transport=transport,
        )
        if not peer_map:
            raft_node.start_election()

    initial_nodes = sorted({node_id, *raft_node.peer_ids})
    ring = ConsistentHashRing(initial_nodes)
    state_machine = RaftStateMachine(
        raft_node=raft_node,
        store=store,
        ring=ring,
        replication_factor=replication_factor,
    )

    app = FastAPI(title=f"SGBD décentralisé - {node_id}", version="0.3.0")
    app.state.node_id = node_id
    app.state.store = store
    app.state.peers = peer_map
    app.state.ring = ring
    app.state.raft_node = raft_node
    app.state.state_machine = state_machine

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "node_id": node_id,
            "role": raft_node.role.value,
            "term": raft_node.current_term,
            "leader_id": raft_node.leader_id,
        }

    @app.get("/raft/status")
    def raft_status():
        return raft_node.status()

    @app.post("/raft/request_vote")
    def raft_request_vote(payload: dict[str, Any]):
        request = RequestVoteRequest.from_dict(payload)
        response = raft_node.handle_request_vote(request)
        return response.to_dict()

    @app.post("/raft/append_entries")
    def raft_append_entries(payload: dict[str, Any]):
        request = AppendEntriesRequest.from_dict(payload)
        response = raft_node.handle_append_entries(request)
        return response.to_dict()

    @app.get("/cluster/nodes")
    def get_cluster_nodes():
        return {
            "node_id": node_id,
            "leader_id": raft_node.leader_id,
            "nodes": ring.get_nodes(),
            "peers": list(raft_node.peer_ids),
        }

    @app.post("/cluster/nodes")
    def add_cluster_node(request: AddNodeRequest):
        try:
            if request.url and request.node_id != node_id:
                app.state.peers[request.node_id] = request.url.rstrip("/")
            entry = state_machine.propose_add_node(request.node_id, request.url)
            return {
                "status": "node_added",
                "node_id": request.node_id,
                "nodes": ring.get_nodes(),
                "term": entry.term,
                "index": entry.index,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotLeaderError as exc:
            leader_url = app.state.peers.get(exc.leader_id or "")
            if leader_url:
                try:
                    resp = httpx.post(
                        f"{leader_url}/cluster/nodes",
                        json={"node_id": request.node_id, "url": request.url},
                        timeout=2.0,
                    )
                    if resp.status_code >= 400:
                        raise HTTPException(status_code=resp.status_code, detail=resp.text)
                    return resp.json()
                except httpx.HTTPError as http_exc:
                    raise HTTPException(status_code=503, detail=str(http_exc)) from http_exc
            raise HTTPException(
                status_code=409,
                detail={"message": str(exc), "leader_id": exc.leader_id},
            ) from exc
        except QuorumNotReachedError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.delete("/cluster/nodes/{target_node_id}")
    def remove_cluster_node(target_node_id: str):
        try:
            entry = state_machine.propose_remove_node(target_node_id)
            app.state.peers.pop(target_node_id, None)
            return {
                "status": "node_removed",
                "node_id": target_node_id,
                "nodes": ring.get_nodes(),
                "term": entry.term,
                "index": entry.index,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotLeaderError as exc:
            leader_url = app.state.peers.get(exc.leader_id or "")
            if leader_url:
                try:
                    resp = httpx.delete(
                        f"{leader_url}/cluster/nodes/{target_node_id}",
                        timeout=2.0,
                    )
                    if resp.status_code >= 400:
                        raise HTTPException(status_code=resp.status_code, detail=resp.text)
                    return resp.json()
                except httpx.HTTPError as http_exc:
                    raise HTTPException(status_code=503, detail=str(http_exc)) from http_exc
            raise HTTPException(
                status_code=409,
                detail={"message": str(exc), "leader_id": exc.leader_id},
            ) from exc
        except QuorumNotReachedError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.put("/kv")
    def put_value(request: PutRequest):
        try:
            entry = state_machine.propose_put(request.key, request.value)
            return {
                "status": "stored",
                "node_id": node_id,
                "key": request.key,
                "term": entry.term,
                "index": entry.index,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotLeaderError as exc:
            leader_url = app.state.peers.get(exc.leader_id or "")
            if leader_url:
                try:
                    resp = httpx.put(
                        f"{leader_url}/kv",
                        json={"key": request.key, "value": request.value},
                        timeout=2.0,
                    )
                    if resp.status_code >= 400:
                        raise HTTPException(status_code=resp.status_code, detail=resp.text)
                    return resp.json()
                except httpx.HTTPError as http_exc:
                    raise HTTPException(status_code=503, detail=str(http_exc)) from http_exc
            raise HTTPException(
                status_code=409,
                detail={"message": str(exc), "leader_id": exc.leader_id},
            ) from exc
        except QuorumNotReachedError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/kv/{key}")
    def get_value(key: str):
        try:
            if not state_machine.contains(key):
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"key": key, "value": state_machine.get(key), "node_id": node_id}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.delete("/kv/{key}")
    def delete_value(key: str):
        try:
            if not state_machine.contains(key) and raft_node.role == NodeRole.LEADER:
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            deleted = state_machine.propose_delete(key)
            if not deleted:
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"status": "deleted", "node_id": node_id, "key": key}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotLeaderError as exc:
            leader_url = app.state.peers.get(exc.leader_id or "")
            if leader_url:
                try:
                    resp = httpx.delete(f"{leader_url}/kv/{key}", timeout=2.0)
                    if resp.status_code >= 400:
                        raise HTTPException(status_code=resp.status_code, detail=resp.text)
                    return resp.json()
                except httpx.HTTPError as http_exc:
                    raise HTTPException(status_code=503, detail=str(http_exc)) from http_exc
            raise HTTPException(
                status_code=409,
                detail={"message": str(exc), "leader_id": exc.leader_id},
            ) from exc
        except QuorumNotReachedError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    return app


app = create_app()
