from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.metrics import METRICS, MetricDef, fmt_hours
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)


# ── Series ────────────────────────────────────────────────────────────────────

@dataclass
class Series:
    """A named subset of PR rows that renders as one line/box on the chart."""
    label: str
    rows: list = field(default_factory=list)


def _build_series(
    raw_per_repo: dict[str, list],
    split_arg: str | None,
    commenter_pr_set: set[tuple] | None = None,
) -> list[Series]:
    """
    Default: one Series per repo.
    --split reviewer:<slug>  — split by presence in PR reviewers list.
    --split commenter:<slug> — split by presence of at least one comment from slug.
    """
    if split_arg is None:
        return [Series(label=lbl, rows=rows) for lbl, rows in raw_per_repo.items()]

    kind, value = split_arg.split(":", 1)
    all_rows = [r for rows in raw_per_repo.values() for r in rows]

    if kind == "reviewer":
        slug = value
        with_rows    = [r for r in all_rows if slug in json.loads(r["reviewers"] or "[]")]
        without_rows = [r for r in all_rows if slug not in json.loads(r["reviewers"] or "[]")]
        return [
            Series(label=f"+ {slug}", rows=with_rows),
            Series(label=f"- {slug}", rows=without_rows),
        ]

    if kind == "commenter":
        slug = value
        ps = commenter_pr_set or set()
        with_rows    = [r for r in all_rows if (r["repo_id"], r["pr_id"]) in ps]
        without_rows = [r for r in all_rows if (r["repo_id"], r["pr_id"]) not in ps]
        return [
            Series(label=f"∈ {slug}", rows=with_rows),
            Series(label=f"∉ {slug}", rows=without_rows),
        ]

    log.error("Unsupported --split kind %r. Supported: reviewer:<slug>, commenter:<slug>", kind)
    sys.exit(1)


# ── Trend rendering ───────────────────────────────────────────────────────────

def _draw_trend_ax(
    ax,
    series_data: list[tuple[str, dict[str, float]]],
    sorted_buckets: list[str],
    mdef: MetricDef,
    colors: list[str],
    linestyles: list[str] | None = None,
) -> None:
    """Draw one metric on one axes. series_data = [(label, {bucket: value})]."""
    for idx, (label, buckets) in enumerate(series_data):
        color = colors[idx % len(colors)]
        ls = linestyles[idx % len(linestyles)] if linestyles else "-"

        x_pos = [i for i, bk in enumerate(sorted_buckets) if bk in buckets]
        y_vals = [buckets[bk] for bk in sorted_buckets if bk in buckets]
        if not x_pos:
            continue

        if mdef.plot_kind == "bar":
            n = len(series_data)
            w = 0.7 / max(n, 1)
            offset = (idx - n / 2 + 0.5) * w
            ax.bar([x + offset for x in x_pos], y_vals,
                   width=w, color=color, alpha=0.7, label=label)
        else:
            ax.plot(x_pos, y_vals, marker="o", label=label,
                    color=color, linewidth=1.5, linestyle=ls)
            for x, y in zip(x_pos, y_vals):
                ax.annotate(mdef.fmt(y), xy=(x, y),
                            xytext=(0, 6), textcoords="offset points",
                            fontsize=6, ha="center", color=color)

    ax.set_ylabel(f"{mdef.label} ({mdef.unit})", fontsize=9)
    if mdef.log_scale:
        ax.set_yscale("log")


def _save(fig, output: str) -> None:
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".html":
        log.warning("HTML output not supported for box/points; saving as PNG.")
        out_path = out_path.with_suffix(".png")
    fig.savefig(str(out_path), dpi=150)
    import matplotlib.pyplot as plt
    plt.close(fig)
    print(f"Chart saved to {out_path}", flush=True)


def _save_trend_html(
    out_path: Path,
    metric_results: dict[str, list[tuple[str, dict]]],
    sorted_buckets: list[str],
    requested_metrics: list[str],
    layout: str,
    period_label: str,
    state: str,
) -> bool:
    """Render trend chart as interactive HTML via plotly. Returns True on success."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return False

    n = len(requested_metrics)
    # Plotly colors (same order as matplotlib default cycle)
    COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
              "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

    if n == 1 or layout == "stack":
        specs = [[{"secondary_y": False}]] * n
        subplot_titles = [METRICS[m].label for m in requested_metrics]
        fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                            subplot_titles=subplot_titles, specs=specs,
                            vertical_spacing=0.08)
        for row_idx, mname in enumerate(requested_metrics, 1):
            mdef = METRICS[mname]
            show_legend = (row_idx == 1)
            for s_idx, (label, buckets) in enumerate(metric_results[mname]):
                xs = [bk for bk in sorted_buckets if bk in buckets]
                ys = [buckets[bk] for bk in xs]
                color = COLORS[s_idx % len(COLORS)]
                trace_label = label if show_legend else label
                fig.add_trace(
                    go.Scatter(
                        x=xs, y=ys, name=trace_label,
                        mode="lines+markers",
                        line=dict(color=color),
                        marker=dict(color=color),
                        text=[mdef.fmt(y) for y in ys],
                        hovertemplate="%{x}<br>%{text}<extra>" + label + "</extra>",
                        legendgroup=label,
                        showlegend=show_legend,
                    ),
                    row=row_idx, col=1,
                )
            fig.update_yaxes(
                title_text=f"{mdef.label} ({mdef.unit})",
                type="log" if mdef.log_scale else "-",
                row=row_idx, col=1,
            )

    else:  # overlay — dual y-axis
        assert n == 2
        mname0, mname1 = requested_metrics[0], requested_metrics[1]
        mdef0, mdef1 = METRICS[mname0], METRICS[mname1]
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        for s_idx, (label, buckets) in enumerate(metric_results[mname0]):
            xs = [bk for bk in sorted_buckets if bk in buckets]
            ys = [buckets[bk] for bk in xs]
            color = COLORS[s_idx % len(COLORS)]
            fig.add_trace(go.Scatter(
                x=xs, y=ys, name=f"{label}",
                mode="lines+markers", line=dict(color=color),
                text=[mdef0.fmt(y) for y in ys],
                hovertemplate="%{x}<br>%{text}<extra>" + label + " — " + mdef0.label + "</extra>",
                legendgroup=label,
            ), secondary_y=False)
        for s_idx, (label, buckets) in enumerate(metric_results[mname1]):
            xs = [bk for bk in sorted_buckets if bk in buckets]
            ys = [buckets[bk] for bk in xs]
            color = COLORS[s_idx % len(COLORS)]
            fig.add_trace(go.Scatter(
                x=xs, y=ys, name=f"{label} ({mdef1.label})",
                mode="lines+markers", line=dict(color=color, dash="dash"),
                text=[mdef1.fmt(y) for y in ys],
                hovertemplate="%{x}<br>%{text}<extra>" + label + " — " + mdef1.label + "</extra>",
                legendgroup=label, showlegend=True,
            ), secondary_y=True)
        fig.update_yaxes(title_text=f"{mdef0.label} ({mdef0.unit})",
                         type="log" if mdef0.log_scale else "-", secondary_y=False)
        fig.update_yaxes(title_text=f"{mdef1.label} ({mdef1.unit})",
                         type="log" if mdef1.log_scale else "-", secondary_y=True)

    title = " + ".join(METRICS[m].label for m in requested_metrics)
    fig.update_layout(
        title=f"{title} by {period_label} ({state})",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    # Force correct x-axis order — plotly otherwise sorts categories by first appearance
    fig.update_xaxes(categoryorder="array", categoryarray=sorted_buckets)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_path))
    return True


# ── Main command ──────────────────────────────────────────────────────────────

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

    since_ts  = date_to_ms(args.since) if args.since else None
    until_ts  = date_to_ms(args.until, end_of_day=True) if args.until else None
    state     = getattr(args, "state", "MERGED")
    output    = getattr(args, "output", "output/chart.png")
    plot_type = getattr(args, "plot_type", "box")
    period    = getattr(args, "period", "month")
    split_arg = getattr(args, "split", None)
    layout    = getattr(args, "layout", "stack")
    reviewer  = getattr(args, "reviewer", None)

    # Parse and validate --metrics
    raw_metrics = getattr(args, "metrics", "cycle_time")
    requested_metrics = [m.strip() for m in raw_metrics.split(",")]
    unknown = [m for m in requested_metrics if m not in METRICS]
    if unknown:
        log.error("Unknown metric(s): %s. Available: %s", unknown, list(METRICS.keys()))
        sys.exit(1)

    # ── Fetch raw rows ────────────────────────────────────────────────────────
    # Fetch all rows (no state filter) so every metric can use the same dataset.
    # Date range filters on created_date (consistent with original behaviour).
    raw_per_repo: dict[str, list] = {}

    for proj_key, repo_slug in repos:
        repo_row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
        ).fetchone()
        if not repo_row:
            log.warning("Repo not in cache: %s/%s", proj_key, repo_slug)
            continue
        repo_id = repo_row["id"]

        query = """
            SELECT repo_id, pr_id, created_date, closed_date, state, reviewers
            FROM pull_requests
            WHERE repo_id=? AND closed_date IS NOT NULL
        """
        params: list[Any] = [repo_id]
        if since_ts:
            query += " AND created_date >= ?"
            params.append(since_ts)
        if until_ts:
            query += " AND created_date <= ?"
            params.append(until_ts)

        rows = conn.execute(query, params).fetchall()

        # Apply --reviewer filter (keeps dataset focused; separate from --split)
        if reviewer:
            mode, username = reviewer.split(":", 1)
            rows = [
                r for r in rows
                if (mode == "include") == (username in json.loads(r["reviewers"] or "[]"))
            ]

        label = f"{proj_key}/{repo_slug}"
        if rows:
            raw_per_repo[label] = [dict(r) for r in rows]

    # ── Augment with first_comment_date (separate query, no JOIN) ─────────────
    if "time_to_first_comment" in requested_metrics and raw_per_repo:
        fcd_rows = conn.execute("""
            SELECT c.repo_id, c.pr_id, MIN(c.created_date) AS fcd
            FROM pr_comments c
            JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
            WHERE c.author != pr.author
            GROUP BY c.repo_id, c.pr_id
        """).fetchall()
        fcd_map = {(r["repo_id"], r["pr_id"]): r["fcd"] for r in fcd_rows}
        for rows_list in raw_per_repo.values():
            for d in rows_list:
                d["first_comment_date"] = fcd_map.get((d["repo_id"], d["pr_id"]))

    # ── Fetch commenter set (before conn.close) ───────────────────────────────
    commenter_pr_set: set[tuple] | None = None
    if split_arg and split_arg.startswith("commenter:"):
        slug = split_arg.split(":", 1)[1]
        commenter_pr_set = {
            (r["repo_id"], r["pr_id"])
            for r in conn.execute(
                "SELECT DISTINCT repo_id, pr_id FROM pr_comments WHERE author = ?", (slug,)
            ).fetchall()
        }

    conn.close()

    if not raw_per_repo:
        log.error("No data in cache for the specified repos/range.")
        sys.exit(4)

    # ── Build series ──────────────────────────────────────────────────────────
    series_list = _build_series(raw_per_repo, split_arg, commenter_pr_set)

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    # ── points ────────────────────────────────────────────────────────────────
    if plot_type == "points":
        # repo_id -> "PROJ/repo" label
        repo_id_to_label: dict[int, str] = {}
        for lbl, rows_list in raw_per_repo.items():
            for r in rows_list:
                repo_id_to_label[r["repo_id"]] = lbl

        per_pr_metrics  = [m for m in requested_metrics if METRICS[m].row_value is not None]
        aggregated_metrics = [m for m in requested_metrics if METRICS[m].row_value is None]

        for series in series_list:
            print(f"\n{'─' * 60}")
            print(f"{series.label}")

            # ── per-PR metrics: one block per metric ──────────────────────
            for mname in per_pr_metrics:
                mdef = METRICS[mname]
                pts = sorted(
                    (r["closed_date"], r["repo_id"], r["pr_id"], mdef.row_value(r, state))
                    for r in series.rows
                    if mdef.row_value(r, state) is not None
                )
                if not pts:
                    print(f"\n  [{mname}]  no data")
                    continue
                values = [v for _, _, _, v in pts]
                med = statistics.median(values)
                print(f"\n  [{mname}]  n={len(pts)}, median={mdef.fmt(med)}")
                col_w = max(len(f"{repo_id_to_label.get(rid, rid)}#{pid}") for _, rid, pid, _ in pts)
                for closed_ms, repo_id, pr_id, v in pts:
                    ref = f"{repo_id_to_label.get(repo_id, str(repo_id))}#{pr_id}"
                    tag = "  ← median" if v == med else ""
                    print(f"  {ms_to_date(closed_ms)}  {ref:<{col_w}}  {mdef.fmt(v):>8}{tag}")

            # ── aggregated metrics: one combined period table ─────────────
            if aggregated_metrics:
                all_buckets: set[str] = set()
                agg_data: dict[str, dict[str, float]] = {}
                for mname in aggregated_metrics:
                    buckets = METRICS[mname].compute(series.rows, period, state)
                    agg_data[mname] = buckets
                    all_buckets.update(buckets.keys())

                period_label = "week" if period == "week" else "month"
                print(f"\n  [{', '.join(aggregated_metrics)}]  by {period_label}")
                col_w2 = max((len(METRICS[m].label) for m in aggregated_metrics), default=8)
                header = f"  {'period':<12}" + "".join(f"  {METRICS[m].label:>{col_w2}}" for m in aggregated_metrics)
                print(header)
                for bk in sorted(all_buckets):
                    row_str = f"  {bk:<12}"
                    for mname in aggregated_metrics:
                        v = agg_data[mname].get(bk)
                        val_str = METRICS[mname].fmt(v) if v is not None else "-"
                        row_str += f"  {val_str:>{col_w2}}"
                    print(row_str)

        return

    # ── box ───────────────────────────────────────────────────────────────────
    if plot_type == "box":
        data = [
            (s.label, [
                (r["closed_date"] - r["created_date"]) / 3_600_000
                for r in s.rows
                if r["state"] == state and r["closed_date"] and r["created_date"]
            ])
            for s in series_list
        ]
        data = [(lbl, times) for lbl, times in data if times]
        if not data:
            log.error("No data to plot.")
            sys.exit(4)

        fig, ax = plt.subplots(figsize=(max(8, len(data) * 1.5), 7))
        bp = ax.boxplot([times for _, times in data],
                        labels=[lbl for lbl, _ in data], patch_artist=True)
        for i, patch in enumerate(bp["boxes"]):
            patch.set_facecolor(colors[i % len(colors)])
            patch.set_alpha(0.7)

        y_min, y_max = ax.get_ylim()
        ann_min_y = y_min + (y_max - y_min) * 0.04
        for i, (lbl, times) in enumerate(data, 1):
            med = statistics.median(times)
            ann_y = max(med, ann_min_y)
            ax.annotate(
                f"med={fmt_hours(med)}\nn={len(times)}",
                xy=(i, med), xytext=(i, ann_y), textcoords="data",
                fontsize=7, color="darkred", ha="center", va="bottom",
                arrowprops=dict(arrowstyle="-", color="darkred", lw=0.5) if ann_y > med else None,
            )

        ax.set_ylabel("Cycle Time (hours)")
        ax.set_xlabel("Series")
        ax.set_title(f"Cycle Time Distribution ({state})")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        _save(fig, output)
        return

    # ── trend ─────────────────────────────────────────────────────────────────
    # Pre-compute: metric_results[metric_name] = [(series_label, {bucket: value})]
    all_buckets: set[str] = set()
    metric_results: dict[str, list[tuple[str, dict]]] = {}

    for metric_name in requested_metrics:
        mdef = METRICS[metric_name]
        series_data = []
        for series in series_list:
            buckets = mdef.compute(series.rows, period, state)
            series_data.append((series.label, buckets))
            all_buckets.update(buckets.keys())
        metric_results[metric_name] = series_data

    if not all_buckets:
        log.error("No data to plot.")
        sys.exit(4)

    sorted_buckets = sorted(all_buckets)
    n_metrics = len(requested_metrics)
    period_label = "Week" if period == "week" else "Month"
    w = max(10, len(sorted_buckets) * 0.8)

    if n_metrics == 1:
        # Single metric — simple plot
        fig, ax = plt.subplots(figsize=(w, 6))
        mname = requested_metrics[0]
        mdef = METRICS[mname]
        _draw_trend_ax(ax, metric_results[mname], sorted_buckets, mdef, colors)
        ax.set_xticks(range(len(sorted_buckets)))
        ax.set_xticklabels(sorted_buckets, rotation=45, ha="right")
        ax.set_title(f"{mdef.label} by {period_label} ({state})")
        ax.legend(fontsize=8)
        plt.tight_layout()

    elif layout == "overlay" and n_metrics == 2:
        # Two metrics on one axes with dual y-axis.
        # Same color = same series. Solid line = metric 0, dashed = metric 1.
        from matplotlib.lines import Line2D

        fig, ax1 = plt.subplots(figsize=(w, 6))
        ax2 = ax1.twinx()

        mname0, mname1 = requested_metrics[0], requested_metrics[1]
        mdef0, mdef1 = METRICS[mname0], METRICS[mname1]

        _draw_trend_ax(ax1, metric_results[mname0], sorted_buckets, mdef0,
                       colors, linestyles=["-"] * len(series_list))
        _draw_trend_ax(ax2, metric_results[mname1], sorted_buckets, mdef1,
                       colors, linestyles=["--"] * len(series_list))

        ax1.set_xticks(range(len(sorted_buckets)))
        ax1.set_xticklabels(sorted_buckets, rotation=45, ha="right")
        ax1.set_title(f"{mdef0.label} & {mdef1.label} by {period_label} ({state})")

        # Combined legend: series (by color) + metric style guide
        series_handles, series_labels = ax1.get_legend_handles_labels()
        style_handles = [
            Line2D([0], [0], color="gray", ls="-",  label=f"─  {mdef0.label}"),
            Line2D([0], [0], color="gray", ls="--", label=f"╌  {mdef1.label}"),
        ]
        ax1.legend(handles=series_handles + style_handles,
                   labels=series_labels + [h.get_label() for h in style_handles],
                   fontsize=7, loc="best")
        plt.tight_layout()

    else:
        # Stack: N subplots sharing the x-axis (works for any number of metrics)
        fig, axes = plt.subplots(
            n_metrics, 1,
            figsize=(w, 4 * n_metrics),
            sharex=True,
            squeeze=False,
        )
        axes = [row[0] for row in axes]

        for ax, mname in zip(axes, requested_metrics):
            mdef = METRICS[mname]
            _draw_trend_ax(ax, metric_results[mname], sorted_buckets, mdef, colors)
            ax.legend(fontsize=8)

        axes[-1].set_xticks(range(len(sorted_buckets)))
        axes[-1].set_xticklabels(sorted_buckets, rotation=45, ha="right")
        title = " + ".join(METRICS[m].label for m in requested_metrics)
        axes[0].set_title(f"{title} by {period_label} ({state})")
        plt.tight_layout()

    out_path = Path(output)
    if out_path.suffix.lower() == ".html":
        ok = _save_trend_html(out_path, metric_results, sorted_buckets,
                              requested_metrics, layout, period_label, state)
        if ok:
            plt.close(fig)
            print(f"Chart saved to {out_path}", flush=True)
            return
        log.warning("plotly not installed, saving as PNG instead.")
        output = str(out_path.with_suffix(".png"))

    _save(fig, output)
