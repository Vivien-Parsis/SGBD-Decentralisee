"""Petit exemple d'utilisation du client du cluster."""
from src.client.cluster_client import ClusterClient


def main() -> None:
    nodes = {
        "node-1": "http://127.0.0.1:8001",
        "node-2": "http://127.0.0.1:8002",
        "node-3": "http://127.0.0.1:8003",
    }
    client = ClusterClient(nodes)

    client.put("utilisateur:1", {"nom": "Alice", "role": "admin"})
    print("Valeur lue :", client.get("utilisateur:1"))
    print("Suppression :", client.delete("utilisateur:1"))


if __name__ == "__main__":
    main()
