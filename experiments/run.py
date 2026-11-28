"""Lance les expériences du projet et écrit les résultats dans experiments/results/.

Exemples (depuis la racine du dépôt) :
    python -m experiments.run nodes      # exp. 1 : latence et débit selon N
    python -m experiments.run rf         # exp. 2 : latence et débit selon RF
    python -m experiments.run crash      # exp. 4 : panne brutale du leader
    python -m experiments.run partition  # exp. 6 : partition réseau
    python -m experiments.run all        # tout, puis les graphiques
Options utiles : --duration 15 --reps 2 --threads 8
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import asdict
from pathlib import Path

from experiments.bench import Op, Workload, summarize
from experiments.check import divergence, lost_writes, stale_reads
from experiments.cluster import Cluster
from src.routing.consistent_hashing import ConsistentHashRing

RESULTS = Path(__file__).resolve().parent / "results"
WARMUP_S = 3.0


# ---------------------------------------------------------------- utilitaires
def save_rows(name: str, rows: list[dict]) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.csv"
    keys: list[str] = []
    for row in rows:
        keys += [k for k in row if k not in keys]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {path}")
    return path


def save_ops(name: str, ops: list[Op], events: dict) -> None:
    rows = []
    for o in ops:
        d = asdict(o)
        d["latency_ms"] = round(o.latency_ms, 3)
        rows.append(d)
    save_rows(name, rows)
    (RESULTS / f"{name}_events.json").write_text(json.dumps(events, indent=2), encoding="utf-8")


def read_router(cluster: Cluster, rf: int | None):
    """Envoie une lecture à un nœud vivant qui stocke la clé."""
    ring = ConsistentHashRing(sorted(cluster.nodes), virtual_nodes=100)

    def route(key: str) -> str | None:
        alive = set(cluster.alive())
        candidates = ring.get_replicas(key, rf) if rf else sorted(alive)
        for node_id in candidates:
            if node_id in alive:
                return cluster.url(node_id)
        return None
    return route


def perf_run(n: int, rf: int | None, duration: float, threads: int, read_ratio: float) -> dict:
    with Cluster(size=n, rf=rf) as cluster:
        cluster.start()
        urls = [cluster.url(i) for i in cluster.nodes]
        wl = Workload(lambda _w: urls, threads=threads, read_ratio=read_ratio,
                      read_router=read_router(cluster, rf))
        ops = wl.run_for(WARMUP_S + duration)
        res = summarize(ops, t_from=WARMUP_S, t_to=WARMUP_S + duration)
        res.update(stale_reads([o for o in ops if o.t_start >= WARMUP_S]))
        return res


# ---------------------------------------------------------------- expériences
def exp_nodes(args) -> None:
    print("Expérience 1 : effet du nombre de nœuds (RF = N)")
    rows = []
    for n in args.sizes:
        for rep in range(1, args.reps + 1):
            print(f"  N={n} répétition {rep}")
            res = perf_run(n, None, args.duration, args.threads, args.read_ratio)
            rows.append({"nodes": n, "rf": n, "rep": rep, **res})
    save_rows("exp1_nodes", rows)


def exp_rf(args) -> None:
    print(f"Expérience 2 : effet du facteur de réplication (N = {args.rf_nodes})")
    rows = []
    for rf in args.rfs:
        for rep in range(1, args.reps + 1):
            print(f"  RF={rf} répétition {rep}")
            res = perf_run(args.rf_nodes, rf, args.duration, args.threads, args.read_ratio)
            rows.append({"nodes": args.rf_nodes, "rf": rf, "rep": rep, **res})
    save_rows("exp2_rf", rows)


def exp_crash(args) -> None:
    print("Expérience 4 : arrêt brutal du leader pendant une charge d'écritures")
    rows = []
    for rep in range(1, args.reps + 1):
        with Cluster(size=args.crash_nodes) as cluster:
            cluster.start()
            old_leader = cluster.wait_for_leader()
            wl = Workload(lambda _w: [cluster.url(i) for i in cluster.alive()],
                          threads=4, read_ratio=0.0, timeout=1.0)
            wl.start()
            time.sleep(5)
            t_kill = wl.now()
            cluster.kill(old_leader)
            new_leader = cluster.wait_for_leader(exclude=old_leader, timeout=15)
            t_elected = wl.now()
            time.sleep(5)
            # Le nœud tué redémarre : combien de temps pour rattraper son journal ?
            leader_commit = cluster.status(new_leader)["commit_index"]
            t_restart = wl.now()
            cluster.restart(old_leader)
            catch_up = None
            deadline = time.perf_counter() + 20
            while time.perf_counter() < deadline:
                st = cluster.status(old_leader)
                if st and st["commit_index"] >= leader_commit:
                    catch_up = wl.now() - t_restart
                    break
                time.sleep(0.02)
            time.sleep(2)
            ops = wl.stop()

            after = [o for o in ops if o.kind == "put" and o.ok and o.t_start >= t_kill]
            first_ok_after = min((o.t_end for o in after), default=None)
            loss = lost_writes(ops, lambda _k: cluster.url(new_leader))
            time.sleep(1)
            div = divergence({i: cluster.url(i) for i in cluster.alive()},
                             list({o.key for o in ops if o.kind == "put" and o.ok}))
            row = {
                "rep": rep, "nodes": args.crash_nodes, "old_leader": old_leader, "new_leader": new_leader,
                "election_ms": round((t_elected - t_kill) * 1000, 1),
                "unavailability_ms": round((first_ok_after - t_kill) * 1000, 1) if first_ok_after else None,
                "restart_catch_up_ms": round(catch_up * 1000, 1) if catch_up is not None else None,
                "acked_writes": loss["acked_writes"], "lost_writes": loss["lost_writes"],
                "wrong_values": loss["wrong_values"], "replicas_identical": div["replicas_identical"],
                "lost_examples": " ".join(loss["lost_examples"]),
            }
            print("  ", row)
            rows.append(row)
            save_ops(f"exp4_crash_timeline_rep{rep}", ops,
                     {"t_kill": t_kill, "t_elected": t_elected, "t_restart": t_restart,
                      "old_leader": old_leader, "new_leader": new_leader})
    save_rows("exp4_crash", rows)


def exp_partition(args) -> None:
    print("Expérience 6 : partition réseau (leader isolé avec une minorité)")
    rows = []
    n = args.partition_nodes
    for rep in range(1, args.reps + 1):
        with Cluster(size=n) as cluster:
            cluster.start()
            old_leader = cluster.wait_for_leader()
            others = [i for i in cluster.nodes if i != old_leader]
            minority = [old_leader] + others[: (n - 1) // 2 - 1]   # 2 nœuds sur 5
            majority = [i for i in cluster.nodes if i not in minority]
            state = {"partitioned": False}

            # Workers 0-1 écrivent côté minorité, workers 2-5 côté majorité.
            def targets(wid: int) -> list[str]:
                if not state["partitioned"]:
                    return [cluster.url(i) for i in cluster.nodes]
                side = minority if wid < 2 else majority
                return [cluster.url(i) for i in side]

            wl = Workload(targets, threads=6, read_ratio=0.0, timeout=1.0)
            wl.start()
            time.sleep(3)
            t_part = wl.now()
            cluster.partition(minority, majority)
            state["partitioned"] = True
            new_leader = cluster.wait_for_leader(among=majority, timeout=15)
            t_elected = wl.now()
            time.sleep(6)
            t_heal = wl.now()
            cluster.heal()
            state["partitioned"] = False
            time.sleep(3)
            ops = wl.stop()

            during = [o for o in ops if o.kind == "put" and t_part + 0.1 <= o.t_start and o.t_end <= t_heal]
            minority_urls = {cluster.url(i) for i in minority}
            min_ops = [o for o in during if o.node in minority_urls]
            maj_ops = [o for o in during if o.node not in minority_urls]
            final_leader = cluster.wait_for_leader()
            loss = lost_writes(ops, lambda _k: cluster.url(final_leader))
            time.sleep(1)
            div = divergence({i: cluster.url(i) for i in cluster.nodes},
                             list({o.key for o in ops if o.kind == "put" and o.ok}))
            old_status = cluster.status(old_leader) or {}
            row = {
                "rep": rep, "nodes": n, "minority": "+".join(minority), "old_leader": old_leader,
                "new_leader_majority": new_leader,
                "election_majority_ms": round((t_elected - t_part) * 1000, 1),
                "minority_writes_tried": len(min_ops),
                "minority_writes_acked": sum(o.ok for o in min_ops),
                "majority_writes_tried": len(maj_ops),
                "majority_writes_acked": sum(o.ok for o in maj_ops),
                "old_leader_role_after_heal": old_status.get("role"),
                "acked_writes": loss["acked_writes"], "lost_writes": loss["lost_writes"],
                "wrong_values": loss["wrong_values"], "replicas_identical": div["replicas_identical"],
                "lost_examples": " ".join(loss["lost_examples"]),
            }
            print("  ", row)
            rows.append(row)
            save_ops(f"exp6_partition_timeline_rep{rep}", ops,
                     {"t_partition": t_part, "t_elected": t_elected, "t_heal": t_heal,
                      "minority": minority, "majority": majority,
                      "minority_urls": sorted(minority_urls)})
    save_rows("exp6_partition", rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("experiment", choices=["nodes", "rf", "crash", "partition", "all"])
    p.add_argument("--duration", type=float, default=15.0, help="durée mesurée par point (s)")
    p.add_argument("--reps", type=int, default=2, help="répétitions par point")
    p.add_argument("--threads", type=int, default=8, help="clients en parallèle")
    p.add_argument("--read-ratio", type=float, default=0.5)
    p.add_argument("--sizes", type=int, nargs="+", default=[3, 5, 7])
    p.add_argument("--rf-nodes", type=int, default=5)
    p.add_argument("--rfs", type=int, nargs="+", default=[1, 3, 5])
    p.add_argument("--crash-nodes", type=int, default=3)
    p.add_argument("--partition-nodes", type=int, default=5)
    args = p.parse_args()

    todo = ["nodes", "rf", "crash", "partition"] if args.experiment == "all" else [args.experiment]
    for name in todo:
        {"nodes": exp_nodes, "rf": exp_rf, "crash": exp_crash, "partition": exp_partition}[name](args)
    if args.experiment == "all":
        from experiments import plots
        plots.main()


if __name__ == "__main__":
    main()
