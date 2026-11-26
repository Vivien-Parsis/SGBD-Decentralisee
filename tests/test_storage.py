from pathlib import Path

import pytest

from src.storage.kv_store import KeyValueStore


def test_put_get_delete_and_persistence(tmp_path: Path):
    db_path = tmp_path / "node-1.json"
    store = KeyValueStore(db_path)

    assert store.get("missing") is None
    assert store.delete("missing") is False

    store.put("user:1", {"name": "Alice", "role": "admin"})
    store.put("nullable", None)

    assert store.contains("user:1") is True
    assert store.get("user:1") == {"name": "Alice", "role": "admin"}
    assert store.contains("nullable") is True
    assert store.get("nullable") is None

    reloaded = KeyValueStore(db_path)
    assert reloaded.get("user:1") == {"name": "Alice", "role": "admin"}
    assert reloaded.contains("nullable") is True

    assert reloaded.delete("user:1") is True
    assert reloaded.get("user:1") is None


def test_invalid_key_raises(tmp_path: Path):
    store = KeyValueStore(tmp_path / "node-1.json")
    with pytest.raises(ValueError):
        store.put("", "value")
    with pytest.raises(ValueError):
        store.get("")
    with pytest.raises(ValueError):
        store.delete("")