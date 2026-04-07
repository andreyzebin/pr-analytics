"""
Metric registry for trend charts.

Adding a new metric:
  1. Write a compute function: (rows, period, state) -> dict[bucket_str, float]
     - rows: list of sqlite3.Row with fields created_date, closed_date, state, reviewers
     - period: "week" | "month"
     - state: the --state CLI arg (used by metrics that care about it)
  2. Add a MetricDef entry to METRICS.

The render layer in cmd_plot.py is metric-agnostic — it only uses
MetricDef.label, unit, plot_kind, compute, and fmt.
"""
from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


def bucket_key(ts_ms: int, period: str) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%G-W%V") if period == "week" else dt.strftime("%Y-%m")


def fmt_hours(hours: float) -> str:
    return f"{hours * 60:.0f}m" if hours < 1 else f"{hours:.1f}h"


@dataclass
class MetricDef:
    label: str       # Y-axis label
    unit: str        # displayed in parentheses after label
    plot_kind: str   # "line" | "bar"
    compute: Callable  # (rows, period, state) -> dict[str, float]
    fmt: Callable      # (value) -> annotation string
    log_scale: bool = False  # logarithmic Y-axis
    row_value: Callable | None = None  # (row, state) -> float | None
                                       # Non-None = per-PR metric; None = aggregated only


# ── compute functions ─────────────────────────────────────────────────────────

def _cycle_time(rows, period: str, state: str) -> dict[str, float]:
    """Median cycle time in hours, bucketed by closed_date."""
    buckets: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        if r["state"] != state or not r["closed_date"] or not r["created_date"]:
            continue
        ct = (r["closed_date"] - r["created_date"]) / 3_600_000
        buckets[bucket_key(r["closed_date"], period)].append(ct)
    return {bk: statistics.median(v) for bk, v in buckets.items()}


def _acceptance_rate(rows, period: str, state: str) -> dict[str, float]:
    """MERGED / (MERGED + DECLINED) × 100, bucketed by closed_date."""
    merged: dict[str, int] = defaultdict(int)
    total: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["state"] not in ("MERGED", "DECLINED") or not r["closed_date"]:
            continue
        bk = bucket_key(r["closed_date"], period)
        total[bk] += 1
        if r["state"] == "MERGED":
            merged[bk] += 1
    return {bk: merged[bk] / total[bk] * 100 for bk in total}


def _throughput(rows, period: str, state: str) -> dict[str, float]:
    """Count of MERGED PRs per period, bucketed by closed_date."""
    buckets: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["state"] == "MERGED" and r["closed_date"]:
            buckets[bucket_key(r["closed_date"], period)] += 1
    return dict(buckets)


def _total_prs(rows, period: str, state: str) -> dict[str, float]:
    """Total count of PRs (any terminal state) per period, bucketed by closed_date."""
    buckets: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["state"] in ("MERGED", "DECLINED") and r["closed_date"]:
            buckets[bucket_key(r["closed_date"], period)] += 1
    return dict(buckets)


def _agent_comments(rows, period: str, state: str) -> dict[str, float]:
    """Total root comments by agent per period, bucketed by closed_date."""
    buckets: dict[str, float] = defaultdict(float)
    for r in rows:
        if r["state"] not in ("MERGED", "DECLINED") or not r["closed_date"]:
            continue
        cnt = r.get("agent_comment_count")
        if cnt:
            buckets[bucket_key(r["closed_date"], period)] += cnt
    return dict(buckets)


def _time_to_first_comment(rows, period: str, state: str) -> dict[str, float]:
    """Median hours from PR creation to first non-author comment, bucketed by closed_date.

    PRs with no reviewer comments are excluded from the median.
    Requires first_comment_date field in rows (populated via LEFT JOIN in cmd_plot).
    """
    buckets: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        if r["state"] != state or not r["closed_date"]:
            continue
        fcd = r.get("first_comment_date") if isinstance(r, dict) else None
        if not fcd or not r["created_date"]:
            continue
        hours = (fcd - r["created_date"]) / 3_600_000
        if hours < 0:
            continue  # data anomaly: comment before PR creation
        buckets[bucket_key(r["closed_date"], period)].append(hours)
    return {bk: statistics.median(v) for bk, v in buckets.items()}


# ── registry ──────────────────────────────────────────────────────────────────

METRICS: dict[str, MetricDef] = {
    "cycle_time": MetricDef(
        label="Median Cycle Time", unit="hours", plot_kind="line",
        compute=_cycle_time, fmt=fmt_hours, log_scale=True,
        row_value=lambda r, state: (
            (r["closed_date"] - r["created_date"]) / 3_600_000
            if r.get("state") == state and r.get("closed_date") and r.get("created_date")
            else None
        ),
    ),
    "acceptance_rate": MetricDef(
        label="Acceptance Rate", unit="%", plot_kind="line",
        compute=_acceptance_rate, fmt=lambda v: f"{v:.0f}%",
    ),
    "throughput": MetricDef(
        label="Throughput", unit="PRs merged", plot_kind="bar",
        compute=_throughput, fmt=lambda v: str(int(v)),
    ),
    "total_prs": MetricDef(
        label="Total PRs", unit="count", plot_kind="bar",
        compute=_total_prs, fmt=lambda v: str(int(v)),
    ),
    "time_to_first_comment": MetricDef(
        label="Time to First Review Comment", unit="hours", plot_kind="line",
        compute=_time_to_first_comment, fmt=fmt_hours, log_scale=True,
        row_value=lambda r, state: (
            (r["first_comment_date"] - r["created_date"]) / 3_600_000
            if r.get("state") == state and r.get("first_comment_date")
               and r.get("created_date") and r["first_comment_date"] >= r["created_date"]
            else None
        ),
    ),
    "agent_comments": MetricDef(
        label="Agent Comments", unit="count", plot_kind="bar",
        compute=_agent_comments, fmt=lambda v: str(int(v)),
        row_value=lambda r, state: (
            r.get("agent_comment_count")
            if r.get("state") in ("MERGED", "DECLINED") and r.get("closed_date")
            else None
        ),
    ),
    "feedback_rate": MetricDef(
        label="Feedback Rate", unit="%", plot_kind="line",
        # compute=None — fetched separately in cmd_plot (requires --author)
        # feedback_rate = comments_with_feedback / total_comments × 100%
        compute=None,
        fmt=lambda v: f"{v:.0f}%",
    ),
    "semantic_acceptance_rate": MetricDef(
        label="Semantic Acceptance Rate", unit="%", plot_kind="line",
        # compute=None — fetched separately in cmd_plot (requires --author + --judge-model)
        # acceptance_rate = yes / (yes + no) × 100%, only among comments with feedback
        compute=None,
        fmt=lambda v: f"{v:.0f}%",
    ),
}
