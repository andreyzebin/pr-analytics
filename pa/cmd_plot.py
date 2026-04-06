from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms

log = logging.getLogger(__name__)


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
    output = getattr(args, "output", "chart.png")

    data_per_repo: dict[str, list[float]] = {}

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

        cycle_times = [
            (r["closed_date"] - r["created_date"]) / 3_600_000
            for r in rows
            if r["closed_date"] and r["created_date"]
        ]

        label = f"{proj_key}/{repo_slug}"
        if not cycle_times:
            log.warning("No data for %s in the specified range/state — skipping.", label)
            continue
        data_per_repo[label] = cycle_times

    conn.close()

    if not data_per_repo:
        log.error("No data to plot.")
        sys.exit(4)

    labels = list(data_per_repo.keys())
    values = [data_per_repo[l] for l in labels]

    fig, ax = plt.subplots(figsize=(max(8, len(labels) * 1.5), 6))
    bp = ax.boxplot(values, labels=labels, patch_artist=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("#4A90D9")
        patch.set_alpha(0.7)

    for i, (label, times) in enumerate(data_per_repo.items(), 1):
        median = sorted(times)[len(times) // 2]
        ax.annotate(f"{median:.1f}h", xy=(i, median), xytext=(4, 4),
                    textcoords="offset points", fontsize=8, color="darkred")

    ax.set_ylabel("Cycle Time (hours)")
    ax.set_xlabel("Repository")
    ax.set_title(f"Cycle Time Distribution ({state})")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    out_path = Path(output)
    if out_path.suffix.lower() == ".html":
        try:
            import plotly.graph_objects as go
            fig_plotly = go.Figure()
            for label, times in data_per_repo.items():
                fig_plotly.add_trace(go.Box(y=times, name=label))
            fig_plotly.update_layout(
                yaxis_title="Cycle Time (hours)",
                xaxis_title="Repository",
                title=f"Cycle Time Distribution ({state})",
            )
            fig_plotly.write_html(str(out_path))
        except ImportError:
            log.warning("plotly not installed, saving as PNG instead.")
            out_path = out_path.with_suffix(".png")
            fig.savefig(str(out_path), dpi=150)
    else:
        fig.savefig(str(out_path), dpi=150)

    print(f"Chart saved to {out_path}", flush=True)
    plt.close(fig)
