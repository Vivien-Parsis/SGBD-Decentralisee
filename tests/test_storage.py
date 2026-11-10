import json
from src.storage.kv_store import KeyValueStore


def test_put_get_delete_and_persistence(tmp_path):
    file_path = tmp_path / "node.json"
    store = KeyValueStore(file_path)
    store.put("city", "Paris")
    assert store.get("city") == "Paris"

    reloaded = KeyValueStore(file_path)
    assert reloaded.get("city") == "Paris"
    assert reloaded.delete("city") is True
    assert reloaded.delete("city") is False
    assert json.loads(file_path.read_text(encoding="utf-8")) == {}
