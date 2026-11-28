# Expériences : performance et pannes

Scripts du Membre 3. Ils lancent de vrais clusters locaux (un processus par nœud), envoient une charge, injectent des pannes et vérifient les données.

## Installation

```bash
pip install -r requirements.txt -r experiments/requirements.txt
```

## Lancer

Depuis la racine du dépôt :

| Commande | Expérience | Ce qui est mesuré |
| --- | --- | --- |
| `python -m experiments.run nodes` | 1. Nombre de nœuds (N = 3, 5, 7) | latence p50/p95/p99 des écritures et lectures, débit |
| `python -m experiments.run rf` | 2. Facteur de réplication (RF = 1, 3, 5 avec N = 5) | idem |
| `python -m experiments.run crash` | 4. Arrêt brutal du leader (kill -9) sous charge | temps d'élection, indisponibilité, rattrapage du nœud relancé, écritures perdues |
| `python -m experiments.run partition` | 6. Partition : leader isolé avec une minorité (2 contre 3) | écritures acceptées de chaque côté, élection côté majorité, écritures perdues, réplicas identiques après réparation |
| `python -m experiments.run all` | les 4, puis les graphiques | |
| `python -m experiments.plots` | refait les graphiques | |

Options : `--reps 2` (répétitions), `--duration 15` (secondes mesurées par point, après 3 s de préchauffage), `--threads 8` (clients en parallèle), `--sizes 3 5 7`, `--rfs 1 3 5`.

Durée de `all` avec les options par défaut : environ 10 minutes.

Résultats : CSV dans `experiments/results/`, graphiques PNG dans `experiments/results/plots/`, journaux des nœuds dans le dossier temporaire du système (`%TEMP%\sgbd-runtime` sous Windows, `/tmp/sgbd-runtime` sous Linux), hors OneDrive.

## Fichiers

| Fichier | Rôle |
| --- | --- |
| `cluster.py` | démarre N nœuds, trouve le leader, tue ou relance un nœud, crée ou répare une partition |
| `bench.py` | charge mixte put/get en parallèle ; chaque opération est enregistrée (début, fin, résultat) |
| `check.py` | écritures acquittées perdues, lectures périmées, comparaison des réplicas |
| `run.py` | les 4 expériences |
| `plots.py` | graphiques |

## Méthode

- Chaque écriture utilise une clé unique : après une panne, on relit toutes les écritures acquittées (réponse 200). Une clé absente est une écriture perdue, ce qui ne doit jamais arriver avec Raft.
- Une lecture relit une clé déjà acquittée : une réponse 404 ou une autre valeur est une lecture périmée.
- La partition est simulée dans la couche réseau de chaque nœud (`/admin/partition`) : les messages Raft entre les deux groupes sont coupés dans les deux sens. Cela fonctionne sous Windows sans droits administrateur.

## Limites à citer dans le rapport

- Tous les nœuds tournent sur la même machine : les latences mesurent surtout le coût HTTP, JSON et disque, pas un vrai réseau.
- Un seul groupe Raft pour tout le cluster : chaque écriture est répliquée sur tous les nœuds. Le facteur de réplication ne change que l'endroit où les données sont stockées.
- L'état Raft (term, vote, journal complet) est réécrit sur disque à chaque modification : la latence d'écriture augmente avec la taille du journal.
- Les lectures sont locales (pas de passage par le leader) : un nœud isolé peut renvoyer une valeur périmée.
