from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms

log = logging.getLogger(__name__)


def _bucket_key(closed_ms: int, period: str) -> str:
    """Return ISO period label for a closed_date timestamp."""
    dt = datetime.fromtimestamp(closed_ms / 1000, tz=timezone.utc)
    if period == "week":
        # ISO week: 2026-W03
        return dt.strftime("%G-W%V")
    else:
        return dt.strftime("%Y-%m")


def _fmt_h(hours: float) -> str:
    if hours < 1:
        return f"{hours * 60:.0f}m"
    return f"{hours:.1f}h"


def cmd_plot(args: argparse.Namespace, cfg: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    repos = collect_repos_from_args(args, conn)
    if not repos:
        log.error("No repositories specified.")
        sys.exit(1)

    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", "MERGED")
    reviewer = getattr(args, "reviewer", None)
    output = getattr(args, "output", "output/chart.png")
    plot_type = getattr(args, "plot_type", "box")
    period = getattr(args, "period", "month")

    # repo_label -> list of (closed_ms, cycle_time_hours)
    raw_per_repo: dict[str, list[tuple[int, float]]] = {}

    for proj_key, repo_slug in repos:
        repo_row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
        ).fetchone()
        if not repo_row:
            log.warning("Repo not in cache: %s/%s", proj_key, repo_slug)
            continue
        repo_id = repo_row["id"]

        query = """
            SELECT created_date, closed_date, reviewers
            FROM pull_requests
            WHERE repo_id=? AND state=? AND closed_date IS NOT NULL
        """
        params: list[Any] = [repo_id, state]
        if since_ts:
            query += " AND created_date >= ?"
            params.append(since_ts)
        if until_ts:
            query += " AND created_date <= ?"
            params.append(until_ts)

        rows = conn.execute(query, params).fetchall()

        if reviewer:
            mode, username = reviewer.split(":", 1)
            rows = [
                r for r in rows
                if (mode == "include") == (username in json.loads(r["reviewers"] or "[]"))
            ]

        points = [
            (r["closed_date"], (r["closed_date"] - r["created_date"]) / 3_600_000)
            for r in rows
            if r["closed_date"] and r["created_date"]
        ]

        label = f"{proj_key}/{repo_slug}"
        if not points:
            log.warning("No data for %s in the specified range/state — skipping.", label)
            continue
        raw_per_repo[label] = points

    conn.close()

    if not raw_per_repo:
        log.error("No data to plot.")
        sys.exit(4)

    # ── points: print sorted list to stdout, no file ───────────────────────────
    if plot_type == "points":
        for label, points in raw_per_repo.items():
            times = sorted(t for _, t in points)
            med = statistics.median(times)
            print(f"\n{label}  (n={len(times)}, median={_fmt_h(med)})")
            for t in times:
                tag = " ← median" if t == med else ""
                print(f"  {_fmt_h(t):>8}{tag}")
        return

    # ── trend: median per period, line chart ──────────────────────────────────
    if plot_type == "trend":
        # collect all bucket keys across repos for consistent x-axis
        all_buckets: set[str] = set()
        trend_per_repo: dict[str, dict[str, list[float]]] = {}
        for label, points in raw_per_repo.items():
            buckets: dict[str, list[float]] = defaultdict(list)
            for closed_ms, ct in points:
                key = _bucket_key(closed_ms, period)
                buckets[key].append(ct)
                all_buckets.add(key)
            trend_per_repo[label] = dict(buckets)

        sorted_buckets = sorted(all_buckets)

        fig, ax = plt.subplots(figsize=(max(10, len(sorted_buckets) * 0.8), 6))
        colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

        for idx, (label, buckets) in enumerate(trend_per_repo.items()):
            color = colors[idx % len(colors)]
            x_pos, y_med, y_n = [], [], []
            for i, bk in enumerate(sorted_buckets):
                times = buckets.get(bk)
                if times:
                    x_pos.append(i)
                    y_med.append(statistics.median(times))
                    y_n.append(len(times))

            ax.plot(x_pos, y_med, marker="o", label=label, color=color, linewidth=1.5)
            for x, y, n in zip(x_pos, y_med, y_n):
                ax.annotate(f"{_fmt_h(y)}\nn={n}", xy=(x, y),
                            xytext=(0, 6), textcoords="offset points",
                            fontsize=6, ha="center", color=color)

        ax.set_xticks(range(len(sorted_buckets)))
        ax.set_xticklabels(sorted_buckets, rotation=45, ha="right")
        ax.set_ylabel("Median Cycle Time (hours)")
        period_label = "Week" if period == "week" else "Month"
        ax.set_title(f"Median Cycle Time by {period_label} ({state})")
        ax.legend(loc="upper right", fontsize=8)
        plt.tight_layout()

        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.suffix.lower() == ".html":
            try:
                import plotly.graph_objects as go
                fig_plotly = go.Figure()
                for label, buckets in trend_per_repo.items():
                    xs, ys, ns = [], [], []
                    for bk in sorted_buckets:
                        times = buckets.get(bk)
                        if times:
                            xs.append(bk)
                            ys.append(statistics.median(times))
                            ns.append(len(times))
                    fig_plotly.add_trace(go.Scatter(
                        x=xs, y=ys, mode="lines+markers", name=label,
                        text=[f"n={n}" for n in ns], hovertemplate="%{x}<br>%{y:.1f}h<br>%{text}",
                    ))
                fig_plotly.update_layout(
                    yaxis_title="Median Cycle Time (hours)",
                    xaxis_title=period_label,
                    title=f"Median Cycle Time by {period_label} ({state})",
                )
                fig_plotly.write_html(str(out_path))
                plt.close(fig)
            except ImportError:
                log.warning("plotly not installed, saving as PNG instead.")
                out_path = out_path.with_suffix(".png")
                fig.savefig(str(out_path), dpi=150)
                plt.close(fig)
        else:
            fig.savefig(str(out_path), dpi=150)
            plt.close(fig)
        print(f"Chart saved to {out_path}", flush=True)
        return

    # ── box: one box per repo ─────────────────────────────────────────────────
    labels = list(raw_per_repo.keys())
    values = [[t for _, t in raw_per_repo[l]] for l in labels]

    fig, ax = plt.subplots(figsize=(max(8, len(labels) * 1.5), 7))
    bp = ax.boxplot(values, labels=labels, patch_artist=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("#4A90D9")
        patch.set_alpha(0.7)

    y_min, y_max = ax.get_ylim()
    annotation_min_y = y_min + (y_max - y_min) * 0.04

    for i, (label, points) in enumerate(raw_per_repo.items(), 1):
        times = [t for _, t in points]
        median = statistics.median(times)
        ann_y = max(median, annotation_min_y)
        ax.annotate(
            f"med={_fmt_h(median)}\nn={len(times)}",
            xy=(i, median),
            xytext=(i, ann_y),
            textcoords="data",
            fontsize=7,
            color="darkred",
            ha="center",
            va="bottom",
            arrowprops=dict(arrowstyle="-", color="darkred", lw=0.5) if ann_y > median else None,
        )

    ax.set_ylabel("Cycle Time (hours)")
    ax.set_xlabel("Repository")
    ax.set_title(f"Cycle Time Distribution ({state})")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".html":
        try:
            import plotly.graph_objects as go
            fig_plotly = go.Figure()
            for label, points in raw_per_repo.items():
                fig_plotly.add_trace(go.Box(y=[t for _, t in points], name=label))
            fig_plotly.update_layout(
                yaxis_title="Cycle Time (hours)",
                xaxis_title="Repository",
                title=f"Cycle Time Distribution ({state})",
            )
            fig_plotly.write_html(str(out_path))
            plt.close(fig)
        except ImportError:
            log.warning("plotly not installed, saving as PNG instead.")
            out_path = out_path.with_suffix(".png")
            fig.savefig(str(out_path), dpi=150)
            plt.close(fig)
    else:
        fig.savefig(str(out_path), dpi=150)
        plt.close(fig)

    print(f"Chart saved to {out_path}", flush=True)
