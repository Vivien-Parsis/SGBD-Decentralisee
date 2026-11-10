"""Persistance locale clé-valeur pour un nœud.

Le fichier JSON est écrit de manière atomique afin d'éviter de laisser un fichier
partiellement écrit si le processus s'arrête pendant une sauvegarde.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any


class KeyValueStore:
    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.file_path.exists():
            self._data = {}
            return
        try:
            with self.file_path.open("r", encoding="utf-8") as file:
                loaded = json.load(file)
            if not isinstance(loaded, dict):
                raise ValueError("Le fichier de stockage doit contenir un objet JSON.")
            self._data = loaded
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Fichier de stockage JSON invalide : {self.file_path}") from exc

    def _save(self) -> None:
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{self.file_path.name}.", suffix=".tmp", dir=self.file_path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(self._data, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_name, self.file_path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def put(self, key: str, value: Any) -> None:
        self._validate_key(key)
        with self._lock:
            self._data[key] = value
            self._save()

    def get(self, key: str) -> Any | None:
        self._validate_key(key)
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> bool:
        self._validate_key(key)
        with self._lock:
            if key not in self._data:
                return False
            del self._data[key]
            self._save()
            return True

    def contains(self, key: str) -> bool:
        self._validate_key(key)
        with self._lock:
            return key in self._data

    def all_items(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._data)

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("La clé doit être une chaîne non vide.")
