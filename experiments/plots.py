"""Graphiques à partir des CSV de experiments/results/ (PNG dans experiments/results/plots/).

    python -m experiments.plots
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
PLOTS = RESULTS / "plots"
BLUE, ORANGE, GREEN, RED, GREY = "#2b6cb0", "#dd6b20", "#2f855a", "#c53030", "#718096"


def read_csv(name: str) -> list[dict]:
    path = RESULTS / f"{name}.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _mean_by(rows: list[dict], x: str, y: str) -> tuple[list[int], list[float], list[float], list[float]]:
    groups: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        try:
            groups[int(r[x])].append(float(r[y]))
        except (ValueError, KeyError):
            pass
    xs = sorted(groups)
    means = [sum(groups[k]) / len(groups[k]) for k in xs]
    lows = [min(groups[k]) for k in xs]
    highs = [max(groups[k]) for k in xs]
    return xs, means, lows, highs


def _line(ax, rows, x, y, label, color):
    xs, m, lo, hi = _mean_by(rows, x, y)
    if not xs:
        return
    ax.errorbar(xs, m, yerr=[[a - b for a, b in zip(m, lo)], [b - a for a, b in zip(m, hi)]],
                marker="o", capsize=4, color=color, label=label)


def perf_figure(rows: list[dict], x: str, xlabel: str, title: str, out: str) -> None:
    if not rows:
        return
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    _line(ax1, rows, x, "put_p50_ms", "écriture p50", BLUE)
    _line(ax1, rows, x, "put_p99_ms", "écriture p99", BLUE)
    _line(ax1, rows, x, "get_p50_ms", "lecture p50", ORANGE)
    _line(ax1, rows, x, "get_p99_ms", "lecture p99", ORANGE)
    for line in ax1.get_lines()[1::2]:
        line.set_linestyle("--")
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel("latence (ms)")
    ax1.set_title("Latence")
    ax1.grid(alpha=0.3)
    ax1.legend()
    _line(ax2, rows, x, "throughput_ops_s", "opérations réussies / s", GREEN)
    ax2.set_xlabel(xlabel)
    ax2.set_ylabel("débit (ops/s)")
    ax2.set_title("Débit")
    ax2.set_ylim(bottom=0)
    ax2.grid(alpha=0.3)
    xs = sorted({int(r[x]) for r in rows})
    for ax in (ax1, ax2):
        ax.set_xticks(xs)
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(PLOTS / out, dpi=150)
    plt.close(fig)
    print(f"  -> {PLOTS / out}")


def timeline_figure(name: str, out: str, title: str) -> None:
    ops = read_csv(name)
    events_path = RESULTS / f"{name}_events.json"
    if not ops or not events_path.exists():
        return
    ev = json.loads(events_path.read_text(encoding="utf-8"))
    bucket = 0.25
    ok, ko = defaultdict(int), defaultdict(int)
    minority_urls = set(ev.get("minority_urls", []))
    ok_min = defaultdict(int)
    for o in ops:
        if o["kind"] != "put":
            continue
        b = int(float(o["t_end"]) / bucket)
        if o["ok"] == "True":
            if o["node"] in minority_urls:
                ok_min[b] += 1
            else:
                ok[b] += 1
        else:
            ko[b] += 1
    last = max(list(ok) + list(ko) + [0])
    xs = [i * bucket for i in range(last + 1)]
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.bar(xs, [ok[i] / bucket for i in range(last + 1)], width=bucket, align="edge",
           color=GREEN, label="écritures acquittées / s" + (" (majorité)" if minority_urls else ""))
    if minority_urls:
        ax.bar(xs, [ok_min[i] / bucket for i in range(last + 1)], width=bucket, align="edge",
               bottom=[ok[i] / bucket for i in range(last + 1)], color=BLUE,
               label="écritures acquittées / s (minorité)")
    ax.bar(xs, [-ko[i] / bucket for i in range(last + 1)], width=bucket, align="edge",
           color=RED, alpha=0.7, label="échecs / s (503, timeout)")
    labels = {"t_kill": "arrêt du leader", "t_elected": "nouveau leader", "t_restart": "redémarrage",
              "t_partition": "partition", "t_heal": "fin de partition"}
    y_low, y_high = ax.get_ylim()
    for i, (key, text) in enumerate((k, t) for k, t in labels.items() if k in ev):
        ax.axvline(ev[key], color=GREY, linestyle="--")
        # étiquettes décalées en hauteur pour ne pas se chevaucher
        ax.text(ev[key], y_low + (y_high - y_low) * (0.35 - 0.12 * (i % 2)), " " + text,
                color=GREY, fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xlabel("temps (s)")
    ax.set_ylabel("opérations / s")
    ax.set_title(title)
    ax.legend(loc="lower left", fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(PLOTS / out, dpi=150)
    plt.close(fig)
    print(f"  -> {PLOTS / out}")


def main() -> None:
    PLOTS.mkdir(parents=True, exist_ok=True)
    print("Graphiques :")
    perf_figure(read_csv("exp1_nodes"), "nodes", "nombre de nœuds N (RF = N)",
                "Expérience 1 : effet du nombre de nœuds", "exp1_nodes.png")
    perf_figure(read_csv("exp2_rf"), "rf", "facteur de réplication RF",
                "Expérience 2 : effet du facteur de réplication", "exp2_rf.png")
    timeline_figure("exp4_crash_timeline_rep1", "exp4_crash_timeline.png",
                    "Expérience 4 : écritures pendant l'arrêt brutal du leader")
    timeline_figure("exp6_partition_timeline_rep1", "exp6_partition_timeline.png",
                    "Expérience 6 : écritures pendant une partition (leader isolé avec une minorité)")


if __name__ == "__main__":
    main()
