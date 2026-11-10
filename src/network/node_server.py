"""Serveur HTTP d'un nœud de stockage."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.storage.kv_store import KeyValueStore


class PutRequest(BaseModel):
    key: str
    value: Any


def create_app(node_id: str | None = None, data_file: str | None = None) -> FastAPI:
    node_id = node_id or os.getenv("NODE_ID", "node-1")
    data_file = data_file or os.getenv("DATA_FILE", f"data/{node_id}.json")
    store = KeyValueStore(Path(data_file))

    app = FastAPI(title=f"SGBD décentralisé - {node_id}", version="0.1.0")
    app.state.node_id = node_id
    app.state.store = store

    @app.get("/health")
    def health():
        return {"status": "ok", "node_id": node_id}

    @app.put("/kv")
    def put_value(request: PutRequest):
        try:
            store.put(request.key, request.value)
            return {"status": "stored", "node_id": node_id, "key": request.key}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/kv/{key}")
    def get_value(key: str):
        try:
            if not store.contains(key):
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"key": key, "value": store.get(key), "node_id": node_id}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.delete("/kv/{key}")
    def delete_value(key: str):
        try:
            deleted = store.delete(key)
            if not deleted:
                raise HTTPException(status_code=404, detail="Clé introuvable.")
            return {"status": "deleted", "node_id": node_id, "key": key}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


app = create_app()
