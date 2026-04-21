#!/usr/bin/env python3
"""
pr_analytics.py — Bitbucket Server PR analytics tool.

Config files (loaded in order, local overrides base):
    config.yaml        — base config, committed to VCS
    config.local.yaml  — local overrides, NOT committed

Environment variables (override config files):
    BB_TOKEN / BITBUCKET_SERVER_BEARER_TOKEN   Bearer token
    BB_URL                                     Bitbucket base URL
    BB_DB                                      SQLite DB path
    REQUESTS_CA_BUNDLE                         CA bundle path
    BITBUCKET_SERVER_CLIENT_CERT               Client PEM path (mTLS)
"""
from __future__ import annotations

import os
# Force UTF-8 mode (fixes Windows cp1251 issues with redirect to file)
os.environ.setdefault("PYTHONUTF8", "1")

# Use OS trust store for SSL (picks up corporate proxy CAs like CheckPoint)
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import argparse
import logging
import sys

from pa.cmd_acceptance import cmd_acceptance
from pa.cmd_analyze import cmd_analyze_feedback
from pa.cmd_merge_analysis import cmd_merge_analysis
from pa.cmd_select_golden import cmd_select_golden
from pa.cmd_cache import cmd_cache
from pa.cmd_feedback import cmd_review_feedback
from pa.cmd_find_repos import cmd_find_repos
from pa.cmd_plot import cmd_plot
from pa.cmd_sql import cmd_sql
from pa.cmd_status import cmd_status
from pa.config import DEFAULT_CONCURRENCY, DEFAULT_DB, DEFAULT_JUDGE_MODEL, load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pr_analytics.py",
        description="Bitbucket Server PR analytics tool",
    )
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")

    sub = parser.add_subparsers(dest="command", required=True)

    # ── cache ──────────────────────────────────────────────────────────────────
    p = sub.add_parser("cache", help="Fetch and cache PRs from Bitbucket Server")
    p.add_argument("--token", help="Personal Access Token")
    p.add_argument("--url", help="Bitbucket Server base URL")
    p.add_argument("--since", help="Cache PRs from this date (YYYY-MM-DD)")
    p.add_argument("--until", help="Cache PRs until this date (YYYY-MM-DD)")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--concurrency", type=int, default=None,
                   help=f"Parallel threads (default from config or {DEFAULT_CONCURRENCY})")
    p.add_argument("--no-comments", action="store_true", dest="no_comments",
                   help="Skip loading comments and reactions")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── plot ───────────────────────────────────────────────────────────────────
    p = sub.add_parser("plot", help="Plot Cycle Time boxplot")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--until", help="End date (YYYY-MM-DD)")
    p.add_argument("--state", default="MERGED", choices=["MERGED", "DECLINED", "OPEN"])
    p.add_argument("--reviewer", help="include:<slug> or exclude:<slug>  (filters dataset)")
    p.add_argument("--type", default="box", choices=["box", "points", "trend"],
                   dest="plot_type",
                   help="Chart type: box (default), points (raw values to stdout), trend (over time)")
    p.add_argument("--period", default="month", choices=["week", "month"],
                   help="Bucketing period for trend (default: month)")
    p.add_argument("--metrics", default="cycle_time",
                   help="Comma-separated metrics for trend: cycle_time, acceptance_rate, throughput"
                        " (default: cycle_time)")
    p.add_argument("--split", default=None, metavar="KIND:VALUE",
                   help="Series mode: reviewer:<slug>, commenter:<slug>, "
                        "or total[:<label>] (aggregate all repos into one series)")
    p.add_argument("--layout", default="stack", choices=["stack", "overlay"],
                   help="Layout when multiple metrics: stack (default) or overlay (dual y-axis, max 2 metrics)")
    p.add_argument("--author", default=None,
                   help="AI agent slug — required for semantic_acceptance_rate metric")
    p.add_argument("--judge-model", default=None, dest="judge_model",
                   help=f"LLM judge model for semantic_acceptance_rate (default: {DEFAULT_JUDGE_MODEL})")
    p.add_argument("--output", default="output/chart.png", help="Output file (.png/.svg)")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── analyze-feedback ───────────────────────────────────────────────────────
    p = sub.add_parser("analyze-feedback",
                       help="Run LLM judge on AI-agent comments to measure semantic acceptance rate")
    p.add_argument("--author", required=True, help="AI agent slug whose comments to analyze")
    p.add_argument("--since", help="Start date (YYYY-MM-DD) — filters on PR created_date")
    p.add_argument("--until", help="End date (YYYY-MM-DD) — filters on PR created_date")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p.add_argument("--judge-model", default=None, dest="judge_model",
                   help=f"LLM judge model (default: {DEFAULT_JUDGE_MODEL})")
    p.add_argument("--batch-size", type=int, default=50, dest="batch_size",
                   help="Max comments to process per run (default: 50, 0 = unlimited)")
    p.add_argument("--budget-tokens", type=int, default=None, dest="budget_tokens",
                   help="Stop when total tokens consumed exceeds this limit (default: unlimited)")
    p.add_argument("--max-comment-chars", type=int, default=2000, dest="max_comment_chars",
                   help="Truncate comment text to this length before sending to LLM (default: 2000)")
    p.add_argument("--force", action="store_true",
                   help="Re-analyze all comments, ignoring cached results")
    p.add_argument("--dry-run", action="store_true", dest="dry_run",
                   help="Show which comments would be analyzed, without calling the LLM")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── select-golden ──────────────────────────────────────────────────────────
    p = sub.add_parser("select-golden",
                       help="Find high-quality PRs suitable as AI code reviewer benchmarks")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--until", help="End date (YYYY-MM-DD)")
    p.add_argument("--steps", default="heuristic,classify,analyze,score,judge",
                   help="Pipeline steps: heuristic,classify,analyze,score,judge (default: all)")
    p.add_argument("--classifier-model", default=None, dest="classifier_model",
                   help=f"LLM model for comment classification (default: from config)")
    p.add_argument("--judge-model", default=None, dest="judge_model",
                   help="LLM model for final GOLD/SILVER/REJECT verdict (default: same as classifier)")
    p.add_argument("--change-judge-model", default=None, dest="change_judge_model",
                   help="Judge model used in analyze-feedback (for change_score lookup)")
    p.add_argument("--top-pct", type=int, default=20, dest="top_pct",
                   help="Top %% of scored PRs to send to final judge (default: 20)")
    p.add_argument("--budget-tokens", type=int, default=None, dest="budget_tokens",
                   help="Total token budget for the run (default: unlimited)")
    p.add_argument("--budget-classify", type=int, default=None, dest="budget_classify",
                   help="Token budget for classify step")
    p.add_argument("--budget-analyze", type=int, default=None, dest="budget_analyze",
                   help="Token budget for analyze step (comment acceptance)")
    p.add_argument("--budget-judge", type=int, default=None, dest="budget_judge",
                   help="Token budget for final judge step")
    p.add_argument("--exclude-authors", default=None, dest="exclude_authors",
                   help="Comma-separated slugs to exclude from all phases (also configurable in golden.exclude_authors)")
    p.add_argument("--max-comment-chars", type=int, default=1500, dest="max_comment_chars",
                   help="Truncate comment text to this length before sending to LLM (default: 1500)")
    # Heuristic thresholds (defaults from config.yaml → golden section)
    p.add_argument("--min-lifetime-h", type=float, default=None, dest="min_lifetime_h")
    p.add_argument("--max-lifetime-h", type=float, default=None, dest="max_lifetime_h")
    p.add_argument("--min-reviewers", type=int, default=None, dest="min_reviewers")
    p.add_argument("--min-comments", type=int, default=None, dest="min_comments")
    p.add_argument("--max-comments", type=int, default=None, dest="max_comments")
    p.add_argument("--output", default="output/golden.html", help="HTML report output path")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── find-repos ─────────────────────────────────────────────────────────────
    p = sub.add_parser("find-repos", help="Find repos where user was a reviewer or commenter")
    p.add_argument("--reviewer", help="Find repos where slug is in formal reviewers list")
    p.add_argument("--commenter", help="Find repos where slug left at least one comment")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--until", help="End date (YYYY-MM-DD)")
    p.add_argument("--state", choices=["MERGED", "DECLINED", "OPEN"])
    p.add_argument("--output", help="Output file path")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── sql ────────────────────────────────────────────────────────────────────
    p = sub.add_parser("sql", help="Run arbitrary SELECT query")
    p.add_argument("--query", help="SQL SELECT query string")
    p.add_argument("--file", help="Path to .sql file")
    p.add_argument("--output", help="Output file path")
    p.add_argument("--format", default="table", choices=["table", "csv", "json"])
    p.add_argument("--limit", type=int, default=10000,
                   help="Row limit (0 = unlimited, default: 10000)")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── status ─────────────────────────────────────────────────────────────────
    p = sub.add_parser("status", help="Show cache status")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── review-feedback ────────────────────────────────────────────────────────
    p = sub.add_parser("review-feedback",
                       help="Export AI-agent comments with reactions and replies")
    p.add_argument("--author", required=True, help="Author slug (AI agent)")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--until", help="End date (YYYY-MM-DD)")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p.add_argument("--state", choices=["MERGED", "DECLINED", "OPEN"])
    p.add_argument("--min-reactions", type=int, default=0, dest="min_reactions")
    p.add_argument("--output", help="Output file path")
    p.add_argument("--format", default="table", choices=["table", "csv", "json"])
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── analyze-merges ──────────────────────────────────────────────────────
    p = sub.add_parser("analyze-merges",
                       help="Check if agent comments led to actual code changes via PR diffs")
    p.add_argument("--author", required=True, help="AI agent slug whose comments to analyze")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--until", help="End date (YYYY-MM-DD)")
    p.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p.add_argument("--projects", help="Comma-separated project keys")
    p.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p.add_argument("--judge-model", default=None, dest="judge_model",
                   help=f"LLM judge model (default: {DEFAULT_JUDGE_MODEL})")
    p.add_argument("--batch-size", type=int, default=50, dest="batch_size",
                   help="Max comments per run (default: 50, 0 = unlimited)")
    p.add_argument("--budget-tokens", type=int, default=None, dest="budget_tokens",
                   help="Token budget (default: unlimited)")
    p.add_argument("--max-comment-chars", type=int, default=2000, dest="max_comment_chars")
    p.add_argument("--max-diff-chars", type=int, default=4000, dest="max_diff_chars",
                   help="Truncate file diff to this length (default: 4000)")
    p.add_argument("--verbose", action="store_true",
                   help="Print full prompt and response for each comment")
    p.add_argument("--force", action="store_true",
                   help="Re-analyze all comments, ignoring cached results")
    p.add_argument("--dry-run", action="store_true", dest="dry_run")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── acceptance ──────────────────────────────────────────────────────────
    p = sub.add_parser("acceptance",
                       help="Acceptance metrics by diffgraph prompt hash")
    p.add_argument("--dg-hash", required=True, dest="dg_hash",
                   help="Diffgraph prompt hash (from dg: tag in comments)")
    p.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p.add_argument("--format", default="text", choices=["text", "json"])
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    cfg = load_config()

    commands = {
        "cache": cmd_cache,
        "plot": cmd_plot,
        "analyze-feedback": cmd_analyze_feedback,
        "select-golden": cmd_select_golden,
        "find-repos": cmd_find_repos,
        "sql": cmd_sql,
        "status": cmd_status,
        "review-feedback": cmd_review_feedback,
        "analyze-merges": cmd_merge_analysis,
        "acceptance": cmd_acceptance,
    }

    fn = commands.get(args.command)
    if fn:
        fn(args, cfg)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
