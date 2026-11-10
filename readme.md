# SGBD décentralisé

Prototype Python de stockage clé-valeur distribué : stockage local JSON, API HTTP et routage par hachage cohérent.

## 1. Prérequis

- Python 3.12 ou plus récent
- Git

## 2. Installation sous Windows (PowerShell)

Depuis le dossier du projet :

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Si l'activation de l'environnement virtuel est bloquée par PowerShell, utilisez directement `.\.venv\Scripts\python.exe` comme dans les exemples.

## 3. Démarrer trois nœuds

Ouvrez trois terminaux PowerShell, à la racine du projet.

Terminal 1 :

```powershell
$env:NODE_ID="node-1"
$env:DATA_FILE="data/node-1.json"
.\.venv\Scripts\python.exe -m uvicorn src.network.node_server:app --host 127.0.0.1 --port 8001
```

Terminal 2 :

```powershell
$env:NODE_ID="node-2"
$env:DATA_FILE="data/node-2.json"
.\.venv\Scripts\python.exe -m uvicorn src.network.node_server:app --host 127.0.0.1 --port 8002
```

Terminal 3 :

```powershell
$env:NODE_ID="node-3"
$env:DATA_FILE="data/node-3.json"
.\.venv\Scripts\python.exe -m uvicorn src.network.node_server:app --host 127.0.0.1 --port 8003
```

Chaque nœud expose `/health`, `PUT /kv`, `GET /kv/{key}` et `DELETE /kv/{key}`. Documentation interactive : `http://127.0.0.1:8001/docs` (adapter le port).

## 4. Utiliser le client du cluster

Dans un quatrième terminal, toujours à la racine :

```powershell
.\.venv\Scripts\python.exe -m src.main
```

Le client choisit le nœud responsable de la clé grâce à l'anneau de hachage cohérent.

## 5. Lancer les tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## 6. Structure

```text
src/
  storage/kv_store.py              # stockage local JSON persistant
  network/node_server.py           # API HTTP d'un nœud (FastAPI)
  routing/consistent_hashing.py    # anneau SHA-256 et nœuds virtuels
  client/cluster_client.py         # routage des opérations vers un nœud
  main.py                           # exemple d'utilisation
tests/                              # tests unitaires et API
data/                               # fichiers JSON locaux (ignorés par Git)
```
