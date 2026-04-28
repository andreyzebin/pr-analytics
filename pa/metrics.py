"""
Metric registry.

Each metric is defined declaratively as a `MetricDef` with an `expr` from
pa.dsl. The render layer in cmd_plot.py is metric-agnostic — it evaluates
the expression against series rows and renders the resulting buckets.

Adding a new metric:
  METRICS["my_metric"] = MetricDef(
      label="…", unit="…", plot_kind="bar",
      fmt=lambda v: str(int(v)),
      expr=Count(where=Eq("state", "MERGED")),
  )

For multi-source ratios use `FromSource`, for repo-level adoption metrics
set `bypass_split=True` and group via --group-by.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

# Re-exported for backward compatibility — the canonical home is pa.buckets.
from pa.buckets import bucket_key, fmt_hours  # noqa: F401

from pa.dsl import (
    And, BinOp, Const, Contains, Count, CountDistinct, Eq, FromSource, In,
    IsNotNull, Median, Or, Ratio, RowBinOp, RowConst, RowField, Sum, Var,
)


def _hours_between(later_field: str, earlier_field: str):
    """Row expression: (later - earlier) in hours (ms→hours: /3_600_000)."""
    return RowBinOp("/",
                    RowBinOp("-", RowField(later_field), RowField(earlier_field)),
                    RowConst(3_600_000))
from pa.sources import analysis_source, comments_source, merge_source


@dataclass
class MetricDef:
    label: str       # Y-axis label
    unit: str        # displayed in parentheses after label
    plot_kind: str   # "line" | "bar"
    fmt: Callable      # (value) -> annotation string
    log_scale: bool = False  # logarithmic Y-axis
    row_value: Callable | None = None  # (row, state) -> float | None
                                       # Non-None = per-PR metric for box/points
    expr: object | None = None  # pa.dsl.Expr — required for trend/json
    # When True, evaluate over the *unsplit* row-set, grouped only by the CLI
    # --group-by field (one series per group). For PR-rate metrics where each
    # row contributes to numerator+denominator within its group, regardless of
    # cohort. Default: respect series_list (split + group-by).
    bypass_split: bool = False


# ── registry ──────────────────────────────────────────────────────────────────

METRICS: dict[str, MetricDef] = {
    "cycle_time": MetricDef(
        label="Median Cycle Time", unit="hours", plot_kind="line",
        fmt=fmt_hours, log_scale=True,
        expr=Median(
            field=_hours_between("closed_date", "created_date"),
            where=And((Eq("state", Var("state")), IsNotNull("created_date"))),
        ),
        row_value=lambda r, state: (
            (r["closed_date"] - r["created_date"]) / 3_600_000
            if r.get("state") == state and r.get("closed_date") and r.get("created_date")
            else None
        ),
    ),
    "acceptance_rate": MetricDef(
        label="Acceptance Rate", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        expr=Ratio(
            Count(where=Eq("state", "MERGED")),
            Count(where=In("state", ["MERGED", "DECLINED"])),
        ),
    ),
    "throughput": MetricDef(
        label="Throughput", unit="PRs merged", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=Count(where=Eq("state", "MERGED")),
    ),
    "total_prs": MetricDef(
        label="Total PRs", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=Count(where=Eq("state", Var("state"))),
    ),
    "total_repos": MetricDef(
        label="Active Repos", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=CountDistinct("repo_id", where=Eq("state", "MERGED"),
                           bucket_field="created_date"),
    ),
    "time_to_first_comment": MetricDef(
        label="Time to First Review Comment", unit="hours", plot_kind="line",
        fmt=fmt_hours, log_scale=True,
        expr=Median(
            field=_hours_between("first_comment_date", "created_date"),
            where=And((Eq("state", Var("state")), IsNotNull("first_comment_date"))),
        ),
        row_value=lambda r, state: (
            (r["first_comment_date"] - r["created_date"]) / 3_600_000
            if r.get("state") == state and r.get("first_comment_date")
               and r.get("created_date") and r["first_comment_date"] >= r["created_date"]
            else None
        ),
    ),
    "agent_comments": MetricDef(
        label="Agent Comments", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=Sum("agent_comment_count",
                 where=In("state", ["MERGED", "DECLINED"])),
        row_value=lambda r, state: (
            r.get("agent_comment_count")
            if r.get("state") in ("MERGED", "DECLINED") and r.get("closed_date")
            else None
        ),
    ),
    "adoption_rate": MetricDef(
        label="Adoption Rate", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        # adoption_rate = PRs_with_agent / PRs_total × 100% per (group, period).
        # bypass_split: cohort split is irrelevant — every PR (in --state) goes
        # into both numerator and denominator of its group.
        bypass_split=True,
        expr=Ratio(
            Count(where=And((
                Eq("state", Var("state")),
                Or((Contains(Var("reviewer_slug"), "reviewers"),
                    Contains(Var("commenter_slug"), "commenters"))),
            )), bucket_field="created_date"),
            Count(where=Eq("state", Var("state")), bucket_field="created_date"),
        ),
    ),
    "agent_inline_comments": MetricDef(
        label="Agent Inline Comments", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(comments_source, Count(where=And((
            Eq("author", Var("author")),
            Eq("parent_id", None),
            IsNotNull("file_path"),
        )))),
    ),
    "feedback_rate": MetricDef(
        label="Feedback Rate", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        # comments_with_feedback / total_root_comments × 100%
        expr=FromSource(comments_source, Ratio(
            Count(where=And((
                Eq("author", Var("author")),
                Eq("parent_id", None),
                Or((Eq("has_reaction", 1), Eq("has_reply", 1))),
            ))),
            Count(where=And((
                Eq("author", Var("author")),
                Eq("parent_id", None),
            ))),
        )),
    ),
    "feedback_all": MetricDef(
        label="Comments with Feedback", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(comments_source, Count(where=And((
            Eq("author", Var("author")),
            Eq("parent_id", None),
            Or((Eq("has_reaction", 1), Eq("has_reply", 1))),
        )))),
    ),
    "feedback_acceptance_rate": MetricDef(
        label="Feedback Acceptance Rate", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        # yes / (yes + no) × 100% — only comments with verdict
        expr=FromSource(analysis_source, Ratio(
            Count(where=And((Eq("verdict", "yes"), Eq("author", Var("author"))))),
            Count(where=And((In("verdict", ["yes", "no"]),
                             Eq("author", Var("author"))))),
        )),
    ),
    "feedback_acceptance_rate_all": MetricDef(
        label="Feedback Acceptance Rate (all)", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        # yes (from @analysis) / all_root_agent_comments (from @comments)
        expr=Ratio(
            FromSource(analysis_source, Count(where=And((
                Eq("verdict", "yes"), Eq("author", Var("author")))))),
            FromSource(comments_source, Count(where=And((
                Eq("author", Var("author")), Eq("parent_id", None))))),
        ),
    ),
    "merge_acceptance_rate": MetricDef(
        label="Merge Acceptance Rate", unit="%", plot_kind="line",
        fmt=lambda v: f"{v:.0f}%",
        # (YES + 0.5*PARTIAL) / (YES + PARTIAL + NO) × 100%
        expr=FromSource(merge_source, Ratio(
            BinOp("+",
                  Count(where=And((Eq("verdict", "YES"),
                                   Eq("author", Var("author"))))),
                  BinOp("*", Const(0.5),
                        Count(where=And((Eq("verdict", "PARTIAL"),
                                         Eq("author", Var("author"))))))),
            Count(where=And((In("verdict", ["YES", "PARTIAL", "NO"]),
                             Eq("author", Var("author"))))),
        )),
    ),
    # ── Absolute count metrics (bar charts) ──────────────────────────────
    "feedback_yes": MetricDef(
        label="Feedback: Accepted", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(analysis_source, Count(where=And((
            Eq("verdict", "yes"), Eq("author", Var("author")))))),
    ),
    "feedback_no": MetricDef(
        label="Feedback: Rejected", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(analysis_source, Count(where=And((
            Eq("verdict", "no"), Eq("author", Var("author")))))),
    ),
    "feedback_unclear": MetricDef(
        label="Feedback: Unclear", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(analysis_source, Count(where=And((
            Eq("verdict", "unclear"), Eq("author", Var("author")))))),
    ),
    "merge_yes": MetricDef(
        label="Merge: Accepted", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(merge_source, Count(where=And((
            Eq("verdict", "YES"), Eq("author", Var("author")))))),
    ),
    "merge_partial": MetricDef(
        label="Merge: Partial", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(merge_source, Count(where=And((
            Eq("verdict", "PARTIAL"), Eq("author", Var("author")))))),
    ),
    "merge_yes_partial": MetricDef(
        label="Merge: Accepted+Partial", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(merge_source, Count(where=And((
            In("verdict", ["YES", "PARTIAL"]), Eq("author", Var("author")))))),
    ),
    "merge_no": MetricDef(
        label="Merge: Not Accepted", unit="count", plot_kind="bar",
        fmt=lambda v: str(int(v)),
        expr=FromSource(merge_source, Count(where=And((
            Eq("verdict", "NO"), Eq("author", Var("author")))))),
    ),
}
