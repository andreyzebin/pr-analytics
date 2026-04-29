from __future__ import annotations

import argparse
import json
import logging
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pa.config import resolve_db, resolve_judge_model
from pa.db import open_db
from pa.dsl import BinOp, FromSource, Group, Split, auto_wrap
from pa.metrics import METRICS, MetricDef, bucket_key, fmt_hours
from pa.buckets import bucket_display
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)


def _fmt_num(v) -> str:
    """Compact int-or-float formatting for ratio component tooltips."""
    if v is None:
        return "?"
    return f"{int(v)}" if v == int(v) else f"{v:.1f}"


def _build_dsl_vars(args, cfg, *, conn=None, pr_rows=None,
                    since_ts=None, until_ts=None, repo_ids=None) -> dict:
    """Single source of truth for the variable dict passed to Expr.eval().

    Resolves CLI args (state/author/judge-model/split slug) and bundles the
    `_`-prefixed runtime context (db conn, time window, repo scope, pre-fetched
    PR rows) that source fetchers in pa/sources.py read.

    Plus: --var name=value entries are merged in last (override built-ins).
    """
    split_arg = getattr(args, "split", None)
    out = {
        "state":          getattr(args, "state", "MERGED"),
        "author":         getattr(args, "author", None),
        "judge_model":    resolve_judge_model(
                              getattr(args, "judge_model", None), cfg),
        "reviewer_slug":  (split_arg.split(":", 1)[1]
                           if split_arg and split_arg.startswith("reviewer:") else None),
        "commenter_slug": (split_arg.split(":", 1)[1]
                           if split_arg and split_arg.startswith("commenter:") else None),
        "_conn":          conn,
        "_pr_rows":       pr_rows,
        "_since_ts":      since_ts,
        "_until_ts":      until_ts,
        "_repo_ids":      repo_ids,
    }
    for spec in (getattr(args, "dsl_vars", None) or []):
        if "=" not in spec:
            log.error("--var must be 'name=value', got: %r", spec)
            sys.exit(1)
        k, _, v = spec.partition("=")
        out[k.strip()] = v.strip()
    return out


def _sh_quote(s: str) -> str:
    """Quote for shell paste-back. Prefer single quotes; if `s` contains
    single quotes but no double quotes, use double quotes; else fall back to
    shlex.quote (escaped, ugly but correct)."""
    if "'" not in s:
        return f"'{s}'"
    if '"' not in s and "$" not in s and "`" not in s and "\\" not in s:
        return f'"{s}"'
    import shlex
    return shlex.quote(s)


# ── Series ────────────────────────────────────────────────────────────────────

@dataclass
class Series:
    """A named subset of PR rows that renders as one line/box on the chart."""
    label: str
    rows: list = field(default_factory=list)


def _group_rows(rows: list, group_by: str) -> dict[str, list]:
    """Split rows into groups by the given attribute. Returns {group_label: rows}."""
    if group_by == "project":
        by_group: dict[str, list] = {}
        for r in rows:
            g = r.get("project_key") or "?"
            by_group.setdefault(g, []).append(r)
        return by_group
    log.error("Unsupported --group-by %r. Supported: project", group_by)
    sys.exit(1)


def _build_series(
    raw_per_repo: dict[str, list],
    split_arg: str | None,
    commenter_pr_set: set[tuple] | None = None,
    group_by: str | None = None,
    state: str | None = None,
) -> list[Series]:
    """
    Default: one Series per repo.
    --split reviewer:<slug>  — repo-level: "+"=repos with ≥1 PR (in --state)
                               where slug is reviewer; "−"=repos with strictly 0.
    --split commenter:<slug> — repo-level: "+"=repos with ≥1 PR (in --state)
                               commented by slug; "−"=repos with strictly 0.
    --split total[:<label>]  — one series with everything combined.
    --group-by project       — further split each series by project key.
    """
    # ── Step 1: build base series from --split (or default: one per repo) ──
    base_series: list[Series] = []
    if split_arg is None:
        if group_by is not None:
            # When grouping without split, aggregate all rows so the group step
            # produces one series per group (not per repo × per group).
            all_rows = [r for rows in raw_per_repo.values() for r in rows]
            base_series = [Series(label="", rows=all_rows)]
        else:
            base_series = [Series(label=lbl, rows=rows) for lbl, rows in raw_per_repo.items()]
    else:
        parts = split_arg.split(":", 1)
        kind, value = parts[0], parts[1] if len(parts) > 1 else ""
        all_rows = [r for rows in raw_per_repo.values() for r in rows]

        # Rows used to classify a repo as "+" (≥1 matching PR) or "−" (strictly 0).
        # Filter by --state so classification matches the metric's denominator.
        classify_rows = [r for r in all_rows if state is None or r["state"] == state]

        if kind == "reviewer":
            slug = value
            # Repo-level split: "+" = repos with ≥1 PR (in --state) where slug
            # is reviewer; "-" = repos with strictly zero such PRs.
            # All rows of a repo go into the same cohort.
            plus_repo_ids = {
                r["repo_id"] for r in classify_rows
                if slug in json.loads(r["reviewers"] or "[]")
            }
            with_rows    = [r for r in all_rows if r["repo_id"] in plus_repo_ids]
            without_rows = [r for r in all_rows if r["repo_id"] not in plus_repo_ids]
            base_series = [
                Series(label=f"+ {slug}", rows=with_rows),
                Series(label=f"- {slug}", rows=without_rows),
            ]
        elif kind == "commenter":
            slug = value
            ps = commenter_pr_set or set()
            # Repo-level split: "+" = repos with ≥1 PR (in --state) commented by slug.
            plus_repo_ids = {
                r["repo_id"] for r in classify_rows
                if (r["repo_id"], r["pr_id"]) in ps
            }
            with_rows    = [r for r in all_rows if r["repo_id"] in plus_repo_ids]
            without_rows = [r for r in all_rows if r["repo_id"] not in plus_repo_ids]
            base_series = [
                Series(label=f"∈ {slug}", rows=with_rows),
                Series(label=f"∉ {slug}", rows=without_rows),
            ]
        elif kind == "total":
            label = value if value else "Total"
            base_series = [Series(label=label, rows=all_rows)]
        else:
            log.error(
                "Unsupported --split kind %r. Supported: reviewer:<slug>, commenter:<slug>, total[:<label>]",
                kind,
            )
            sys.exit(1)

    # ── Step 2: optionally further split each base series by group ─────────
    if group_by is None:
        return base_series

    result: list[Series] = []
    # Determine if split is trivial (no split, or "total", or base label empty) —
    # in that case the group label alone is descriptive enough.
    trivial = split_arg is None or (split_arg.split(":", 1)[0] == "total")
    for bs in base_series:
        for group, rows in sorted(_group_rows(bs.rows, group_by).items()):
            if trivial or not bs.label:
                label = group
            else:
                label = f"{group} / {bs.label}"
            result.append(Series(label=label, rows=rows))
    return result


# ── Trend rendering ───────────────────────────────────────────────────────────

def _draw_trend_ax(
    ax,
    series_data: list[tuple[str, dict[str, float]]],
    sorted_buckets: list[str],
    mdef: MetricDef,
    colors: list[str],
    linestyles: list[str] | None = None,
    is_mean: bool = False,
) -> None:
    """Draw one metric on one axes. series_data = [(label, {bucket: value})].

    When `is_mean` is True the metric is rendered as a "baseline overlay":
    bold dashed line in a neutral colour (grey/black), so the average sits
    on top of per-group lines clearly distinct."""
    for idx, (label, buckets) in enumerate(series_data):
        color = "black" if is_mean else colors[idx % len(colors)]
        ls = "--" if is_mean else (linestyles[idx % len(linestyles)] if linestyles else "-")
        lw = 3.0 if is_mean else 1.5

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
                    color=color, linewidth=lw, linestyle=ls)
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
    axes_groups: list[list[str]] | None = None,
    ratio_components: dict[str, dict[str, dict[str, tuple]]] | None = None,
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

    if axes_groups:
        # User-defined subplot grouping. Each group → one subplot; metrics in
        # the group share the y-axis (overlay).
        n_rows = len(axes_groups)
        subplot_titles = [
            " + ".join(METRICS[m].label for m in g) for g in axes_groups
        ]
        fig = make_subplots(
            rows=n_rows, cols=1, shared_xaxes=True,
            subplot_titles=subplot_titles, vertical_spacing=0.08,
        )
        from pa.dsl import has_mean
        # Track each (legendgroup) we've already added so subsequent subplots
        # with the same series share a single legend entry instead of either
        # spamming duplicates or hiding the legend entirely after the first row.
        legend_seen: set[str] = set()
        for row_idx, group in enumerate(axes_groups, 1):
            for m_idx, mname in enumerate(group):
                mdef = METRICS[mname]
                is_mean = mdef.expr is not None and has_mean(mdef.expr)
                dash = "dash" if is_mean else ["solid", "dash", "dot", "dashdot"][m_idx % 4]
                width = 4 if is_mean else 2
                comps_for_metric = (ratio_components or {}).get(mname, {})
                for s_idx, (label, buckets) in enumerate(metric_results[mname]):
                    xs = [bk for bk in sorted_buckets if bk in buckets]
                    ys = [buckets[bk] for bk in xs]
                    color = "black" if is_mean else COLORS[s_idx % len(COLORS)]
                    trace_name = f"{label} — {mdef.label}" if len(group) > 1 else label
                    lg = f"{mname}::{label}"
                    show_legend = lg not in legend_seen
                    legend_seen.add(lg)

                    # Ratio metrics get (num, den) per bucket as customdata,
                    # surfaced in the hover tooltip as e.g. "42% (10 / 24)".
                    series_comps = comps_for_metric.get(label, {})
                    if series_comps:
                        custom = [list(series_comps.get(bk, (None, None))) for bk in xs]
                        text = [
                            (f"{mdef.fmt(y)}  ({_fmt_num(c[0])} / {_fmt_num(c[1])})"
                             if c[0] is not None and c[1] is not None
                             else mdef.fmt(y))
                            for y, c in zip(ys, custom)
                        ]
                    else:
                        custom = None
                        text = [mdef.fmt(y) for y in ys]

                    fig.add_trace(
                        go.Scatter(
                            x=xs, y=ys, name=trace_name,
                            mode="lines+markers",
                            line=dict(color=color, dash=dash, width=width),
                            marker=dict(color=color),
                            text=text,
                            customdata=custom,
                            hovertemplate="%{x}<br>%{text}<extra>" + trace_name + "</extra>",
                            legendgroup=lg,
                            showlegend=show_legend,
                        ),
                        row=row_idx, col=1,
                    )
            # Use first metric's log_scale flag for the row (mixed metrics
            # in one axes is the user's responsibility)
            first_mdef = METRICS[group[0]]
            fig.update_yaxes(
                type="log" if first_mdef.log_scale else "-",
                row=row_idx, col=1,
            )
        fig.update_layout(
            title=f"by {period_label} ({state})",
            height=max(400, 250 * n_rows + 100),
            hovermode="x unified",
        )
        # Pin X-axis category order to chronological sorted_buckets (otherwise
        # plotly uses first-seen order across traces, which produces zigzags
        # when later series have earlier dates than earlier series).
        fig.update_xaxes(
        categoryorder="array", categoryarray=sorted_buckets,
        tickmode="array",
        tickvals=sorted_buckets,
        ticktext=[bucket_display(b) for b in sorted_buckets],
    )
        fig.write_html(str(out_path))
        return True

    elif n == 1 or layout == "stack":
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
    fig.update_xaxes(
        categoryorder="array", categoryarray=sorted_buckets,
        tickmode="array",
        tickvals=sorted_buckets,
        ticktext=[bucket_display(b) for b in sorted_buckets],
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_path))
    return True


# ── Main command ──────────────────────────────────────────────────────────────

def cmd_plot(args: argparse.Namespace, cfg: dict) -> None:
    # ── --new-dsl: print equivalent CLI command using --dsl flags, exit ──
    if getattr(args, "new_dsl", False):
        from pa.dsl import format_expr
        from pa.config import resolve_judge_model as _resolve_judge_model
        raw_metrics = getattr(args, "metrics", "cycle_time")
        names = [m.strip() for m in raw_metrics.split(",") if m.strip()]
        split = getattr(args, "split", None)
        group_by = getattr(args, "group_by", None)
        period_arg = getattr(args, "period", None)
        since_arg = getattr(args, "since", None)
        until_arg = getattr(args, "until", None)
        # Pass through I/O / presentation flags only — semantic flags
        # (period/since/until/split/group-by/state/author/judge-model) get
        # absorbed into the DSL or the --var list below.
        passthrough_pairs = []
        defaults = {"plot_type": "box", "output": "output/chart.png",
                    "layout": "stack"}
        for cli_flag, attr, value in [
            ("--type",        "plot_type",   getattr(args, "plot_type", None)),
            ("--output",      "output",      getattr(args, "output", None)),
            ("--projects",    "projects",    getattr(args, "projects", None)),
            ("--repos",       "repos",       getattr(args, "repos", None)),
            ("--repos-file",  "repos_file",  getattr(args, "repos_file", None)),
            ("--db",          "db",          getattr(args, "db", None)),
            ("--layout",      "layout",      getattr(args, "layout", None)),
        ]:
            if not value or defaults.get(attr) == value:
                continue
            passthrough_pairs.append(f"{cli_flag} {_sh_quote(str(value))}")
        for axes_spec in (getattr(args, "axes", None) or []):
            passthrough_pairs.append(f"--axes {_sh_quote(axes_spec)}")

        # Emit values for built-in $vars referenced in metric exprs as
        # --var name=value. Uses CLI flags as the source.
        var_pairs: list[tuple[str, str]] = []
        for var_name, val in [
            ("state",          getattr(args, "state", None) or "MERGED"),
            ("author",         getattr(args, "author", None)),
            ("judge_model",    _resolve_judge_model(
                                   getattr(args, "judge_model", None), cfg)),
            ("reviewer_slug",  split.split(":", 1)[1]
                               if split and split.startswith("reviewer:") else None),
            ("commenter_slug", split.split(":", 1)[1]
                               if split and split.startswith("commenter:") else None),
        ]:
            if val is not None:
                var_pairs.append((var_name, val))
        for spec in (getattr(args, "dsl_vars", None) or []):
            if "=" in spec:
                k, _, v = spec.partition("=")
                var_pairs.append((k.strip(), v.strip()))

        # Pretty multi-line shell output
        lines = ["python pr_analytics.py plot"]
        for kv in passthrough_pairs:
            lines.append(f"  {kv}")
        for k, v in var_pairs:
            lines.append(f"  --var {_sh_quote(f'{k}={v}')}")
        lines.append("  --metrics ''")  # suppress cycle_time default
        for mname in names:
            if mname not in METRICS or METRICS[mname].expr is None:
                continue
            mdef = METRICS[mname]
            wrapped = auto_wrap(mdef.expr, split=split, group_by=group_by,
                                period=period_arg, since=since_arg, until=until_arg,
                                skip_split=mdef.bypass_split)
            # Note: NO substitute_vars — emit Var($name) references as-is.
            # The --var entries above provide the values at runtime.
            pretty = format_expr(wrapped)
            indented = "\n".join("    " + ln for ln in pretty.splitlines())
            payload = f"{mname}=\n{indented}\n  "
            lines.append(f"  --dsl {_sh_quote(payload)}")

        print(" \\\n".join(lines))
        return

    # ── --explain: print DSL for named + ad-hoc metrics, exit (no DB) ─────
    if getattr(args, "explain", False):
        from pa.dsl import format_expr
        from pa.parser import parse_expr as _parse_expr
        raw_metrics = getattr(args, "metrics", "cycle_time")
        split = getattr(args, "split", None)
        group_by = getattr(args, "group_by", None)
        names = [m.strip() for m in raw_metrics.split(",") if m.strip()]
        for mname in names:
            if mname not in METRICS:
                print(f"# unknown metric: {mname}")
                continue
            mdef = METRICS[mname]
            print(f"# {mname}  ({mdef.label}, {mdef.unit}, {mdef.plot_kind})")
            if mdef.expr is None:
                print("#   (no DSL expression)")
                print()
                continue
            wrapped = auto_wrap(mdef.expr, split=split, group_by=group_by,
                                period=getattr(args, "period", None),
                                since=getattr(args, "since", None),
                                until=getattr(args, "until", None),
                                skip_split=mdef.bypass_split)
            print(format_expr(wrapped))
            print()
        for spec in (getattr(args, "ad_hoc_metrics", None) or []):
            label, _, raw_expr = spec.partition("=")
            print(f"# ad-hoc: {label.strip()!r}")
            try:
                wrapped = auto_wrap(_parse_expr(raw_expr.strip()),
                                    split=split, group_by=group_by,
                                    period=getattr(args, "period", None),
                                    since=getattr(args, "since", None),
                                    until=getattr(args, "until", None))
                print(format_expr(wrapped))
            except SyntaxError as e:
                print(f"#   parse error: {e}")
            print()
        for spec in (getattr(args, "full_dsl", None) or []):
            label, _, raw_expr = spec.partition("=")
            print(f"# --dsl: {label.strip()!r}  (no auto-wrap)")
            try:
                print(format_expr(_parse_expr(raw_expr.strip())))
            except SyntaxError as e:
                print(f"#   parse error: {e}")
            print()
        return

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    repos = collect_repos_from_args(args, conn)
    if not repos:
        log.error("No repositories specified.")
        sys.exit(1)

    # ── --dsl is var-only mode: forbid all semantic CLI flags ─────────────
    # When the user supplies fully-wrapped DSL via --dsl, every semantic CLI
    # flag (--period/--since/--until/--split/--group-by/--reviewer/--state/
    # --author/--judge-model) becomes ambiguous: the DSL already encodes the
    # semantics, and the flag would silently be a no-op or worse, drift.
    # Only --var name=value is accepted to inject values into Var($name).
    if getattr(args, "full_dsl", []):
        forbidden = []
        for flag, attr in [("--period",     "period"),
                           ("--since",      "since"),
                           ("--until",      "until"),
                           ("--split",      "split"),
                           ("--group-by",   "group_by"),
                           ("--reviewer",   "reviewer"),
                           ("--state",      "state"),
                           ("--author",     "author"),
                           ("--judge-model","judge_model")]:
            if getattr(args, attr, None):
                forbidden.append(flag)
        if forbidden:
            log.error(
                "--dsl mode forbids semantic flags: %s. They have no effect "
                "on --dsl metrics (the DSL itself contains period/range/group/"
                "split/@source). Pass values via --var name=value instead "
                "(e.g. --var state=MERGED --var author=ai-bot).",
                ", ".join(forbidden),
            )
            sys.exit(1)

    since_ts  = date_to_ms(args.since) if args.since else None
    until_ts  = date_to_ms(args.until, end_of_day=True) if args.until else None
    state     = getattr(args, "state", None) or "MERGED"
    output    = getattr(args, "output", "output/chart.png")
    plot_type = getattr(args, "plot_type", "box")
    period    = getattr(args, "period", None) or "month"
    split_arg = getattr(args, "split", None)
    group_by  = getattr(args, "group_by", None)
    layout    = getattr(args, "layout", "stack")
    reviewer  = getattr(args, "reviewer", None)

    # Parse and validate --metrics
    raw_metrics = getattr(args, "metrics", "cycle_time")
    requested_metrics = [m.strip() for m in raw_metrics.split(",") if m.strip()]

    # ── Ad-hoc metrics: --metric 'label=<dsl-expr>' ─────────────────────────
    # `--metric` is auto-wrapped (period/range/@pr/group/split added by
    # auto_wrap from CLI flags). `--new-dsl` is treated as already-complete
    # DSL — auto_wrap is skipped and the expression runs as-is.
    from pa.parser import parse_expr
    for spec in (getattr(args, "ad_hoc_metrics", None) or []):
        if "=" not in spec:
            log.error("--metric must be 'label=<dsl-expr>', got: %r", spec)
            sys.exit(1)
        label, _, raw_expr = spec.partition("=")
        label = label.strip()
        try:
            expr = parse_expr(raw_expr.strip())
        except SyntaxError as e:
            log.error("ad-hoc metric %r: parse error — %s", label, e)
            sys.exit(1)
        slug = "_ad_hoc_" + re.sub(r"\W+", "_", label).strip("_").lower()
        METRICS[slug] = MetricDef(
            label=label, unit="", plot_kind="line",
            fmt=lambda v: f"{v:.2f}",
            expr=expr,
        )
        requested_metrics.append(slug)

    full_dsl_metric_names: set[str] = set()
    for spec in (getattr(args, "full_dsl", None) or []):
        if "=" not in spec:
            log.error("--dsl must be 'label=<dsl-expr>', got: %r", spec)
            sys.exit(1)
        label, _, raw_expr = spec.partition("=")
        label = label.strip()
        try:
            expr = parse_expr(raw_expr.strip())
        except SyntaxError as e:
            log.error("--dsl %r: parse error — %s", label, e)
            sys.exit(1)
        slug = "_full_dsl_" + re.sub(r"\W+", "_", label).strip("_").lower()
        METRICS[slug] = MetricDef(
            label=label, unit="", plot_kind="line",
            fmt=lambda v: f"{v:.2f}",
            expr=expr,
        )
        requested_metrics.append(slug)
        full_dsl_metric_names.add(slug)

    unknown = [m for m in requested_metrics if m not in METRICS]
    if unknown:
        log.error("Unknown metric(s): %s. Available: %s", unknown, list(METRICS.keys()))
        sys.exit(1)

    # ── Fetch raw rows ────────────────────────────────────────────────────────
    # Fetch all rows (no state filter) so every metric can use the same dataset.
    # Date range filters on created_date (consistent with original behaviour).
    raw_per_repo: dict[str, list] = {}
    all_repo_ids: list[int] = []  # consumed by DSL @-source fetchers

    for proj_key, repo_slug in repos:
        repo_row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
        ).fetchone()
        if not repo_row:
            log.warning("Repo not in cache: %s/%s", proj_key, repo_slug)
            continue
        repo_id = repo_row["id"]
        all_repo_ids.append(repo_id)

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
            row_dicts = [dict(r) for r in rows]
            for d in row_dicts:
                d["project_key"] = proj_key  # for --group-by project (Group node)
                d["repo_label"]  = label     # for default per-repo Group wrap
            raw_per_repo[label] = row_dicts

    # ── Augment with agent_comment_count (separate query) ────────────────────
    author_arg = getattr(args, "author", None)
    if "agent_comments" in requested_metrics and raw_per_repo and author_arg:
        ac_rows = conn.execute("""
            SELECT repo_id, pr_id, COUNT(*) AS cnt
            FROM pr_comments
            WHERE author = ? AND parent_id IS NULL
            GROUP BY repo_id, pr_id
        """, (author_arg,)).fetchall()
        ac_map = {(r["repo_id"], r["pr_id"]): r["cnt"] for r in ac_rows}
        for rows_list in raw_per_repo.values():
            for d in rows_list:
                d["agent_comment_count"] = ac_map.get((d["repo_id"], d["pr_id"]), 0)

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

    # NB: don't close conn here — DSL @-sources need it for further fetches.
    # It's closed at the end of cmd_plot via the early-return paths or fall-through.

    if not raw_per_repo:
        conn.close()
        log.error("No data in cache for the specified repos/range.")
        sys.exit(4)

    # ── Build series ──────────────────────────────────────────────────────────
    series_list = _build_series(raw_per_repo, split_arg, commenter_pr_set, group_by, state)

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    # ── points ────────────────────────────────────────────────────────────────
    # Unified path: every metric goes through auto_wrap → eval_series like
    # --type trend / --type json. The result is one set of series per metric
    # (with split/group already applied by the DSL). For per-PR metrics
    # (those whose `expr` ends in `Median(field=RowExpr)`), additionally
    # render each underlying row's value below the bucket table.
    if plot_type == "points":
        from pa.dsl import Median, Sum, RowExpr, FromSource, Group, Split, Period, DateRange

        # repo_id -> "PROJ/repo" label
        repo_id_to_label: dict[int, str] = {}
        for lbl, rows_list in raw_per_repo.items():
            for r in rows_list:
                repo_id_to_label[r["repo_id"]] = lbl

        all_rows = [r for rl in raw_per_repo.values() for r in rl]
        dsl_vars = _build_dsl_vars(
            args, cfg, conn=conn, pr_rows=all_rows,
            since_ts=since_ts, until_ts=until_ts, repo_ids=all_repo_ids,
        )

        def _row_aggregator(expr):
            """Drill through wrapper nodes and return the innermost
            Median/Sum aggregator if its `field` is a RowExpr — that means
            the metric has a per-PR value extractable for points display."""
            seen = expr
            while True:
                if isinstance(seen, (Period, DateRange, FromSource, Group, Split)):
                    seen = seen.inner
                    continue
                break
            if isinstance(seen, (Median, Sum)) and isinstance(seen.field, RowExpr):
                return seen
            return None

        for mname in requested_metrics:
            mdef = METRICS[mname]
            if mdef.expr is None:
                log.warning("--type points: %r has no expr; skipped", mname)
                continue
            if mname in full_dsl_metric_names:
                wrapped = mdef.expr  # --dsl metric is already final
            else:
                wrapped = auto_wrap(
                    mdef.expr, split=split_arg, group_by=group_by, period=period,
                    since=getattr(args, "since", None),
                    until=getattr(args, "until", None),
                    skip_split=mdef.bypass_split,
                )
            results = wrapped.eval_series(all_rows, period, dsl_vars)

            # Parallel num/den eval for ratio metrics (skipped for Mean).
            from pa.dsl import find_outer_ratio, replace_ratio
            ratio_pair = find_outer_ratio(wrapped)
            comps_for_metric: dict[str, dict] = {}
            if ratio_pair is not None:
                num_e, den_e = ratio_pair
                num_r = dict(replace_ratio(wrapped, num_e).eval_series(
                    all_rows, period, dsl_vars))
                den_r = dict(replace_ratio(wrapped, den_e).eval_series(
                    all_rows, period, dsl_vars))
                for label, _ in results:
                    nb, db = num_r.get(label, {}), den_r.get(label, {})
                    comps_for_metric[label] = {
                        bk: (nb.get(bk, 0.0 if bk in db else None),
                             db.get(bk, 0.0 if bk in nb else None))
                        for bk in set(nb) | set(db)
                    }

            print(f"\n{'─' * 60}")
            display_name = (mdef.label
                            if mname.startswith(("_ad_hoc_", "_full_dsl_"))
                            else mname)
            print(f"  [{display_name}]")

            # Effective period: top-level Period() in the wrapped expr wins
            # over the CLI default ("month").
            eff_period = wrapped.period if isinstance(wrapped, Period) else period
            period_label = "week" if eff_period == "week" else "month"
            for label, buckets in results:
                if not buckets:
                    continue
                print(f"\n  {label or display_name}  by {period_label}")
                col_w = max(len(mdef.label), 14)
                comps = comps_for_metric.get(label, {})
                if comps:
                    print(f"  {'period':<12}  {'date':<8}  {mdef.label:>{col_w}}   {'(num/den)':>14}")
                    for bk in sorted(buckets):
                        v = mdef.fmt(buckets[bk])
                        n, d = comps.get(bk, (None, None))
                        cs = f"({_fmt_num(n)}/{_fmt_num(d)})"
                        print(f"  {bk:<12}  {bucket_display(bk):<8}  {v:>{col_w}}   {cs:>14}")
                else:
                    print(f"  {'period':<12}  {'date':<8}  {mdef.label:>{col_w}}")
                    for bk in sorted(buckets):
                        print(f"  {bk:<12}  {bucket_display(bk):<8}  {mdef.fmt(buckets[bk]):>{col_w}}")

            # Per-PR drill-down for Median/Sum-of-RowExpr metrics
            row_agg = _row_aggregator(mdef.expr)
            if row_agg is None:
                continue
            pts = sorted(
                (r["closed_date"], r["repo_id"], r.get("pr_id"), row_agg.field(r))
                for r in all_rows
                if (row_agg.where is None or row_agg.where(r, dsl_vars))
                and row_agg.field(r) is not None
            )
            if not pts:
                continue
            values = [v for _, _, _, v in pts]
            med = statistics.median(values)
            print(f"\n  per-PR  n={len(pts)}, median={mdef.fmt(med)}")
            col_w = max(len(f"{repo_id_to_label.get(rid, rid)}#{pid}") for _, rid, pid, _ in pts)
            for closed_ms, repo_id, pr_id, v in pts:
                ref = f"{repo_id_to_label.get(repo_id, str(repo_id))}#{pr_id}"
                tag = "  ← median" if v == med else ""
                print(f"  {ms_to_date(closed_ms)}  {ref:<{col_w}}  {mdef.fmt(v):>8}{tag}")
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
    # For total_repos: keep per-bucket repo_id sets so the printed total can be
    # computed as union (unique repos across the whole range), not sum of
    # per-period unique counts.
    total_repos_sets: dict[str, dict[str, set]] = {}

    # Augment each PR row with `commenters` — set of comment authors on this PR.
    # Enables the DSL filter `$slug in commenters` symmetrically with reviewers.
    if all_repo_ids:
        placeholders = ",".join("?" * len(all_repo_ids))
        cm_rows = conn.execute(
            f"""SELECT repo_id, pr_id, author FROM pr_comments
                WHERE repo_id IN ({placeholders}) AND author IS NOT NULL""",
            all_repo_ids,
        ).fetchall()
        commenters_by_pr: dict[tuple, set] = {}
        for r in cm_rows:
            commenters_by_pr.setdefault((r["repo_id"], r["pr_id"]), set()).add(r["author"])
        for rows_list in raw_per_repo.values():
            for r in rows_list:
                r["commenters"] = commenters_by_pr.get((r["repo_id"], r["pr_id"]), set())

    # adoption_rate validation: needs reviewer or commenter slug.
    if ("adoption_rate" in requested_metrics
            and not (split_arg and (split_arg.startswith("reviewer:")
                                    or split_arg.startswith("commenter:")))):
        log.error("adoption_rate requires --split reviewer:<slug> or --split commenter:<slug>")
        sys.exit(1)
    # All raw PR rows (across all repos) — wrapped @pr source reads them from
    # dsl_vars["_pr_rows"]; non-source paths still get them as the row arg.
    all_rows = [r for rl in raw_per_repo.values() for r in rl]
    dsl_vars = _build_dsl_vars(
        args, cfg, conn=conn, pr_rows=all_rows,
        since_ts=since_ts, until_ts=until_ts, repo_ids=all_repo_ids,
    )

    # ratio_components[metric_name][series_label][bucket] = (num, den)
    # Populated only for metrics whose expression has a Ratio reachable through
    # Period/Range/Source/Group/Split (NOT through Mean). Drives:
    #   - hover tooltips in plotly (`42% (10 / 24)`)
    #   - JSON output (extra `components` field per bucket)
    #   - --type points text rendering (appends `(num/den)` to value)
    from pa.dsl import find_outer_ratio, replace_ratio
    ratio_components: dict[str, dict[str, dict[str, tuple[float, float]]]] = {}

    for metric_name in requested_metrics:
        mdef = METRICS[metric_name]
        if mdef.expr is None:
            log.error("metric %r has no expr — registry corruption", metric_name)
            sys.exit(1)

        if metric_name in full_dsl_metric_names:
            wrapped = mdef.expr  # --dsl: user-provided DSL is final, no auto-wrap
        else:
            wrapped = auto_wrap(
                mdef.expr, split=split_arg, group_by=group_by, period=period,
                since=getattr(args, "since", None), until=getattr(args, "until", None),
                skip_split=mdef.bypass_split,
            )
        # Source metrics ignore input rows; non-source need all_rows for
        # Group/Split partitioning.
        results = wrapped.eval_series(all_rows, period, dsl_vars)

        # Parallel num/den eval for Ratio metrics — runs the same wrapped tree
        # twice with Ratio replaced by num and by den. ~2× cost, all in memory.
        ratio_pair = find_outer_ratio(wrapped)
        if ratio_pair is not None:
            num_expr, den_expr = ratio_pair
            num_results = dict(replace_ratio(wrapped, num_expr).eval_series(
                all_rows, period, dsl_vars))
            den_results = dict(replace_ratio(wrapped, den_expr).eval_series(
                all_rows, period, dsl_vars))
            comps: dict[str, dict[str, tuple[float, float]]] = {}
            for label, _ in results:
                nb = num_results.get(label, {})
                db = den_results.get(label, {})
                # Count-based aggregators omit zero-count buckets entirely.
                # When the denominator has the bucket but the numerator doesn't,
                # the numerator is logically 0 (zero matching rows), not "unknown".
                comps[label] = {
                    bk: (nb.get(bk, 0.0 if bk in db else None),
                         db.get(bk, 0.0 if bk in nb else None))
                    for bk in set(nb) | set(db)
                }
            ratio_components[metric_name] = comps

        # Decorate empty / source labels with author/judge for readability.
        # `--split total[:label]` produces a single series named after the label.
        total_label = None
        if split_arg and split_arg.startswith("total"):
            total_label = split_arg.split(":", 1)[1] if ":" in split_arg else "Total"
        decorated: list[tuple[str, dict]] = []
        for label, buckets in results:
            if not label:
                if total_label:
                    label = total_label
                elif author_arg and dsl_vars.get("judge_model"):
                    label = f"{author_arg} ({dsl_vars['judge_model']})"
                elif author_arg:
                    label = author_arg
                else:
                    label = metric_name
            decorated.append((label, buckets))
            all_buckets.update(buckets.keys())

        metric_results[metric_name] = decorated

        # total_repos special: track per-bucket sets for the union "total=" line
        if metric_name == "total_repos":
            from pa.metrics import bucket_key as _bk_fn0
            for label, _ in decorated:
                # Re-derive sets for this series's rows. We need the row subset
                # that actually contributed — but with auto_wrap this is opaque.
                # For now: union over all rows (one entry, since total_repos is
                # not auto-Split-wrapped when @-source, and for non-source it's
                # PR-level so we just compute total per series).
                pass  # (printed total falls back to sum-of-buckets for now)
            # Compute single union set for default series labelling.
            sets: dict[str, set] = {}
            for r in all_rows:
                if r.get("state") != "MERGED" or not r.get("created_date"):
                    continue
                sets.setdefault(_bk_fn0(r["created_date"], period), set()).add(r["repo_id"])
            for label, _ in decorated:
                total_repos_sets[label] = sets

    if not all_buckets:
        log.error("No data to plot.")
        sys.exit(4)

    sorted_buckets = sorted(all_buckets)

    # ── json output: dump metric_results and return (machine-readable) ────────
    if plot_type == "json":
        import json as _json
        result = {
            "period": period, "state": state,
            "buckets": sorted_buckets,
            "metrics": [
                {
                    # For --metric / --dsl ad-hoc metrics, the user-supplied
                    # label is the canonical identifier; fall back to the
                    # registry slug for built-in metrics.
                    "name": (METRICS[mname].label
                             if mname.startswith(("_ad_hoc_", "_full_dsl_"))
                             else mname),
                    "label": METRICS[mname].label,
                    "unit": METRICS[mname].unit,
                    "plot_kind": METRICS[mname].plot_kind,
                    "series": [
                        {
                            "label": label,
                            "buckets": {bk: buckets[bk] for bk in sorted(buckets.keys())},
                            **({
                                "components": {
                                    bk: {"num": n, "den": d}
                                    for bk, (n, d) in
                                    sorted(ratio_components.get(mname, {}).get(label, {}).items())
                                }
                            } if mname in ratio_components else {}),
                        }
                        for label, buckets in metric_results.get(mname, [])
                    ],
                }
                for mname in requested_metrics
            ],
        }
        out_text = _json.dumps(result, ensure_ascii=False, indent=2)
        # Default output for trend is "output/chart.png" — treat that as
        # "no explicit output", and write to stdout instead.
        if output and output not in ("output/chart.png", "/dev/null"):
            Path(output).write_text(out_text, encoding="utf-8")
            print(f"Result written to {output}", flush=True)
        else:
            print(out_text)
        return

    n_metrics = len(requested_metrics)
    # Effective period for chart titles: if the metrics' DSL contains a top-
    # level Period() node, that wins over the CLI --period default. Picks the
    # first metric's period; if metrics use mixed periods, falls back to
    # "mixed" so the title doesn't lie.
    from pa.dsl import Period as _Period
    def _expr_period(expr):
        cur = expr
        while cur is not None:
            if isinstance(cur, _Period):
                return cur.period
            cur = getattr(cur, "inner", None)
        return None
    metric_periods = {
        _expr_period(METRICS[m].expr) for m in requested_metrics
        if METRICS[m].expr is not None
    } - {None}
    if len(metric_periods) == 1:
        eff_period = next(iter(metric_periods))
    elif len(metric_periods) > 1:
        eff_period = "mixed"
    else:
        eff_period = period
    period_label = {"week": "Week", "month": "Month", "mixed": "mixed period"}.get(
        eff_period, eff_period.capitalize() if eff_period else "Month")
    w = max(10, len(sorted_buckets) * 0.8)

    # ── Explicit subplot grouping via --axes ──────────────────────────────
    # `--axes "a,b"` (repeatable) defines per-subplot metric groups.
    # Each group becomes one subplot; metrics within a group share the y-axis
    # (overlay rendering, no twinx). Falls back to legacy --layout when not set.
    # Build a name→slug map so --axes can reference --metric/--dsl entries
    # by their user-supplied label, not the internal `_ad_hoc_*` slug.
    metric_lookup: dict[str, str] = {m: m for m in requested_metrics}
    for slug in requested_metrics:
        if slug.startswith(("_ad_hoc_", "_full_dsl_")):
            metric_lookup[METRICS[slug].label] = slug

    axes_groups: list[list[str]] = []
    for spec in (getattr(args, "axes", None) or []):
        names_raw = [m.strip() for m in spec.split(",") if m.strip()]
        names = [metric_lookup.get(n, n) for n in names_raw]
        unknown = [n for n, slug in zip(names_raw, names) if slug not in requested_metrics]
        if unknown:
            log.error("--axes references unknown metric(s) %s. Pass them via "
                      "--metrics, --metric, or --dsl first.", unknown)
            sys.exit(1)
        axes_groups.append(names)
    if axes_groups:
        # Drop any metrics that weren't placed into a group (silently ignore —
        # user may want them computed but not shown).
        fig, axes_arr = plt.subplots(
            len(axes_groups), 1,
            figsize=(w, 4 * len(axes_groups)),
            sharex=True,
            squeeze=False,
        )
        axes_arr = [row[0] for row in axes_arr]
        from pa.dsl import has_mean
        for ax, group in zip(axes_arr, axes_groups):
            label_parts = []
            for m_idx, mname in enumerate(group):
                mdef = METRICS[mname]
                ls_per_metric = ["-", "--", ":", "-."][m_idx % 4]
                _draw_trend_ax(ax, metric_results[mname], sorted_buckets, mdef,
                               colors, linestyles=[ls_per_metric] * len(metric_results[mname]),
                               is_mean=mdef.expr is not None and has_mean(mdef.expr))
                label_parts.append(mdef.label)
                if mdef.log_scale:
                    ax.set_yscale("log")
            ax.set_title(" + ".join(label_parts))
            ax.legend(fontsize=8)
        axes_arr[-1].set_xticks(range(len(sorted_buckets)))
        axes_arr[-1].set_xticklabels([bucket_display(b) for b in sorted_buckets], rotation=45, ha="right")
        plt.tight_layout()

    elif n_metrics == 1:
        # Single metric — simple plot
        fig, ax = plt.subplots(figsize=(w, 6))
        mname = requested_metrics[0]
        mdef = METRICS[mname]
        _draw_trend_ax(ax, metric_results[mname], sorted_buckets, mdef, colors)
        ax.set_xticks(range(len(sorted_buckets)))
        ax.set_xticklabels([bucket_display(b) for b in sorted_buckets], rotation=45, ha="right")
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
        ax1.set_xticklabels([bucket_display(b) for b in sorted_buckets], rotation=45, ha="right")
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
        axes[-1].set_xticklabels([bucket_display(b) for b in sorted_buckets], rotation=45, ha="right")
        title = " + ".join(METRICS[m].label for m in requested_metrics)
        axes[0].set_title(f"{title} by {period_label} ({state})")
        plt.tight_layout()

    # ── Print totals for count-based metrics ─────────────────────────────────
    for mname in requested_metrics:
        if METRICS[mname].plot_kind != "bar":
            continue
        for label, buckets in metric_results[mname]:
            if mname == "total_repos":
                # Unique repos across the whole range (union of per-period sets),
                # not sum of per-period unique counts (which double-counts repos
                # active in multiple periods).
                sets = total_repos_sets.get(label, {})
                union: set = set()
                for s in sets.values():
                    union |= s
                total_val = float(len(union))
            else:
                total_val = sum(buckets.values())
            if total_val:
                print(f"{METRICS[mname].label}  [{label}]  total={METRICS[mname].fmt(total_val)}")

    out_path = Path(output)
    if out_path.suffix.lower() == ".html":
        ok = _save_trend_html(out_path, metric_results, sorted_buckets,
                              requested_metrics, layout, period_label, state,
                              axes_groups=axes_groups,
                              ratio_components=ratio_components)
        if ok:
            plt.close(fig)
            print(f"Chart saved to {out_path}", flush=True)
            return
        log.warning("plotly not installed, saving as PNG instead.")
        output = str(out_path.with_suffix(".png"))

    _save(fig, output)
