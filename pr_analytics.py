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

import argparse
import logging
import sys

from pa.cmd_cache import cmd_cache
from pa.cmd_feedback import cmd_review_feedback
from pa.cmd_find_repos import cmd_find_repos
from pa.cmd_plot import cmd_plot
from pa.cmd_sql import cmd_sql
from pa.cmd_status import cmd_status
from pa.config import DEFAULT_CONCURRENCY, DEFAULT_DB, load_config


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
                   help="Split into two series, e.g. reviewer:<slug>")
    p.add_argument("--layout", default="stack", choices=["stack", "overlay"],
                   help="Layout when multiple metrics: stack (default) or overlay (dual y-axis, max 2 metrics)")
    p.add_argument("--output", default="output/chart.png", help="Output file (.png/.svg)")
    p.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── find-repos ─────────────────────────────────────────────────────────────
    p = sub.add_parser("find-repos", help="Find repos where user was a reviewer")
    p.add_argument("--reviewer", required=True, help="Reviewer slug")
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
        "find-repos": cmd_find_repos,
        "sql": cmd_sql,
        "status": cmd_status,
        "review-feedback": cmd_review_feedback,
    }

    fn = commands.get(args.command)
    if fn:
        fn(args, cfg)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
