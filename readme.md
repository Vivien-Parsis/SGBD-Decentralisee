# SGBD décentralisé

Prototype Python de stockage clé-valeur distribué : stockage local JSON, API HTTP et routage par hachage cohérent.

## 1. Prérequis

- Python >=3.12
- Git

## 2. Installation sous Windows (PowerShell)

Depuis le dossier du projet :

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Si l'activation de l'environnement virtuel est bloquée par PowerShell, utilisez directement `.\.venv\Scripts\python.exe` comme dans les exemples.

## 3. Démarrer un cluster de trois nœuds

Ouvrez trois terminaux à la racine du projet et lancez un nœud par terminal. La liste `--peers` est la même pour tous les nœuds.

```bash
python -m src.main --node-id node-1 --port 8001 --peers node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003
python -m src.main --node-id node-2 --port 8002 --peers node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003
python -m src.main --node-id node-3 --port 8003 --peers node-1=http://127.0.0.1:8001,node-2=http://127.0.0.1:8002,node-3=http://127.0.0.1:8003
```

Options : `--rf N` (facteur de réplication, par défaut tous les nœuds stockent tout), `--data-file` (fichier de données). Les délais Raft se règlent avec les variables `RAFT_ELECTION_MIN_MS` (300), `RAFT_ELECTION_MAX_MS` (600), `RAFT_HEARTBEAT_MS` (75) et `RAFT_RPC_TIMEOUT_MS` (200).

Vérifier l'élection : `http://127.0.0.1:8001/health` (un seul nœud doit avoir `"role": "leader"`). État Raft détaillé : `/raft/status`.

## 4. API

| Méthode | Route | Rôle |
| --- | --- | --- |
| `PUT` | `/kv` (corps `{"key": ..., "value": ...}`) | écrit une valeur ; un suiveur transmet au leader ; 503 si pas de majorité |
| `GET` | `/kv/{key}` | lit la valeur stockée localement par le nœud |
| `DELETE` | `/kv/{key}` | supprime une clé (via le leader) |
| `GET` | `/health`, `/raft/status` | rôle, term, leader, commit index |
| `POST` / `DELETE` | `/cluster/nodes` | ajout et retrait d'un nœud à chaud |
| `POST` / `DELETE` | `/admin/partition` | simulation de partition réseau (tests) |

Exemple : `curl -X PUT http://127.0.0.1:8001/kv -H "Content-Type: application/json" -d '{"key":"a","value":1}'`

## 4 bis. Expériences (performance et pannes)

Voir [experiments/README.md](experiments/README.md) : `python -m experiments.run all` lance les mesures et produit les graphiques.

## 5. Lancer les tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## 6. Structure

```text
src/
  storage/kv_store.py               # stockage local JSON persistant
  network/node_server.py            # API HTTP d'un nœud (FastAPI)
  routing/consistent_hashing.py     # anneau SHA-256 et nœuds virtuels
  client/cluster_client.py          # routage des opérations vers un nœud
  main.py                           # exemple d'utilisation
tests/                              # tests unitaires et API
data/                               # fichiers JSON locaux (ignorés par Git)
```
