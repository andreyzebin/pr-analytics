from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import ms_to_date


def cmd_status(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    db_file = Path(db_path)

    if not db_file.exists():
        print(f"Database not found: {db_path}")
        sys.exit(4)

    conn = open_db(db_path)

    projects = conn.execute("SELECT key, name, cache_date FROM projects ORDER BY key").fetchall()
    total_repos = conn.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
    total_prs = conn.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
    total_comments = conn.execute("SELECT COUNT(*) FROM pr_comments").fetchone()[0]
    total_reactions = conn.execute("SELECT COUNT(*) FROM comment_reactions").fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(created_date), MAX(created_date) FROM pull_requests"
    ).fetchone()
    conn.close()

    db_size = db_file.stat().st_size / (1024 * 1024)
    print(f"Database: {db_path}  ({db_size:.1f} MB)")
    print(f"Projects: {len(projects)}  Repos: {total_repos}  PRs: {total_prs}")
    print(f"Comments: {total_comments}  Reactions: {total_reactions}")

    if date_range[0] and date_range[1]:
        print(f"PR date range: {ms_to_date(date_range[0])} — {ms_to_date(date_range[1])}")

    if projects:
        print("\nProjects:")
        for p in projects:
            print(f"  {p['key']} ({p['name']})  cached: {p['cache_date'] or 'unknown'}")
    else:
        print("\nNo projects cached yet.")
