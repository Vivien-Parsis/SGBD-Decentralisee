"""Point d'entrée pour démarrer un nœud de la base de données décentralisée."""
from __future__ import annotations

import argparse
import os

import uvicorn

from src.network.node_server import _parse_peers_env, create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Démarre un nœud du SGBD décentralisé.")
    parser.add_argument("--node-id", default=os.getenv("NODE_ID", "node-1"))
    parser.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8001")))
    parser.add_argument("--data-file", default=None)
    parser.add_argument("--peers", default=os.getenv("CLUSTER_PEERS", ""))
    parser.add_argument("--virtual-nodes", type=int, default=100)
    parser.add_argument("--rf", type=int, default=None, help="facteur de réplication")
    args = parser.parse_args()

    peers = _parse_peers_env(args.peers, args.node_id)
    data_file = args.data_file or f"data/{args.node_id}.json"

    app = create_app(
        node_id=args.node_id,
        data_file=data_file,
        peers=peers,
        virtual_nodes=args.virtual_nodes,
        replication_factor=args.rf,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()