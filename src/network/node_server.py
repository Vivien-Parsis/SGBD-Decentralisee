"""Serveur HTTP d'un nœud de stockage et de routage."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.routing.consistent_hashing import ConsistentHashRing
from src.storage.kv_store import KeyValueStore


class PutRequest(BaseModel):
    key: str
    value: Any


class NodeConfigRequest(BaseModel):
    node_id: str
    url: str


def _parse_peers(raw_peers: str | None) -> dict[str, str]:
    """Parse une chaîne du type 'node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002'."""
    peers: dict[str, str] = {}
    if not raw_peers:
        return peers
    for item in raw_peers.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        nid, url = item.split("=", 1)
        peers[nid.strip()] = url.strip().rstrip("/")
    return peers


def create_app(
        node_id: str | None = None,
        data_file: str | None = None,
        peers: dict[str, str] | None = None,
        virtual_nodes: int = 100,
        state_machine: Any | None = None,
) -> FastAPI:
    node_id = node_id or os.getenv("NODE_ID", "node-1")
    data_file = data_file or os.getenv("DATA_FILE", f"data/{node_id}.json")
    sync_on_write = os.getenv("SYNC_ON_WRITE", "true").lower() in ("1", "true", "yes")

    resolved_peers = dict(peers) if peers is not None else _parse_peers(os.getenv("CLUSTER_PEERS"))
    if node_id not in resolved_peers:
        self_url = os.getenv("NODE_URL", "http://127.0.0.1:8000").rstrip("/")
        resolved_peers[node_id] = self_url

    store = KeyValueStore(Path(data_file), sync_on_write=sync_on_write)
    ring = ConsistentHashRing(nodes=list(resolved_peers.keys()), virtual_nodes=virtual_nodes)

    app = FastAPI(title=f"SGBD décentralisé - {node_id}", version="0.2.0")
    app.state.node_id = node_id
    app.state.store = store
    app.state.ring = ring
    app.state.peers = resolved_peers
    app.state.state_machine = state_machine

    def _forward_put(target_url: str, key: str, value: Any) -> dict[str, Any]:
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.put(f"{target_url}/kv", json={"key": key, "value": value}, params={"forward": "false"})
            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail=f"Nœud cible indisponible ({target_url}): {exc}") from exc

    def _forward_get(target_url: str, key: str) -> dict[str, Any]:
        encoded_key = quote(key, safe="")
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{target_url}/kv/{encoded_key}", params={"forward": "false"})
            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail=f"Nœud cible indisponible ({target_url}): {exc}") from exc

    def _forward_delete(target_url: str, key: str) -> dict[str, Any]:
        encoded_key = quote(key, safe="")
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.delete(f"{target_url}/kv/{encoded_key}", params={"forward": "false"})
            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail=f"Nœud cible indisponible ({target_url}): {exc}") from exc

    def _apply_put_local(key: str, value: Any) -> None:
        if app.state.state_machine is not None and hasattr(app.state.state_machine, "apply_put"):
            app.state.state_machine.apply_put(key, value)
        else:
            store.put(key, value)

    def _apply_delete_local(key: str) -> bool:
        if app.state.state_machine is not None and hasattr(app.state.state_machine, "apply_delete"):
            return bool(app.state.state_machine.apply_delete(key))
        return store.delete(key)

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "node_id": node_id,
            "nodes": ring.get_nodes(),
        }

    @app.get("/cluster/route/{key}")
    def route_key(key: str, replicas: int = Query(default=1, ge=1)):
        try:
            owner = ring.get_node(key)
            return {
                "key": key,
                "owner": owner,
                "owner_url": resolved_peers.get(owner),
                "replicas": ring.get_replicas(key, replicas),
            }
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.post("/cluster/nodes")
    def add_cluster_node(payload: NodeConfigRequest):
        resolved_peers[payload.node_id] = payload.url.rstrip("/")
        ring.add_node(payload.node_id)
        return {"status": "added", "node_id": payload.node_id, "nodes": ring.get_nodes()}

    @app.delete("/cluster/nodes/{target_node_id}")
    def remove_cluster_node(target_node_id: str):
        if target_node_id == node_id:
            raise HTTPException(status_code=400, detail="Impossible de retirer le nœud courant lui-même.")
        resolved_peers.pop(target_node_id, None)
        ring.remove_node(target_node_id)
        return {"status": "removed", "node_id": target_node_id, "nodes": ring.get_nodes()}

    @app.put("/kv")
    def put_value(request: PutRequest, forward: bool = True):
        try:
            KeyValueStore._validate_key(request.key)
            owner = ring.get_node(request.key)
            if owner != node_id and forward and owner in resolved_peers:
                return _forward_put(resolved_peers[owner], request.key, request.value)
            _apply_put_local(request.key, request.value)
            return {"status": "stored", "node_id": node_id, "owner": owner, "key": request.key}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/kv/{key}")
    def get_value(key: str, forward: bool = True):
        try:
            KeyValueStore._validate_key(key)
            owner = ring.get_node(key)
            if owner != node_id and forward and owner in resolved_peers:
                return _forward_get(resolved_peers[owner], key)
            if not store.contains(key):
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"key": key, "value": store.get(key), "node_id": node_id, "owner": owner}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.delete("/kv/{key}")
    def delete_value(key: str, forward: bool = True):
        try:
            KeyValueStore._validate_key(key)
            owner = ring.get_node(key)
            if owner != node_id and forward and owner in resolved_peers:
                return _forward_delete(resolved_peers[owner], key)
            deleted = _apply_delete_local(key)
            if not deleted:
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"status": "deleted", "node_id": node_id, "owner": owner, "key": key}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    return app


app = create_app()