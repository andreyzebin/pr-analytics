from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import date_to_ms


def cmd_find_repos(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    reviewer = getattr(args, "reviewer", None)
    commenter = getattr(args, "commenter", None)
    if not reviewer and not commenter:
        print("Error: --reviewer or --commenter is required.", file=sys.stderr)
        sys.exit(1)
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)

    if commenter:
        query = """
            SELECT DISTINCT r.project_key || '/' || r.slug AS repo
            FROM pull_requests pr
            JOIN repos r ON r.id = pr.repo_id
            WHERE EXISTS (
                SELECT 1 FROM pr_comments c
                WHERE c.repo_id = pr.repo_id AND c.pr_id = pr.pr_id AND c.author = ?
            )
        """
        params: list[Any] = [commenter]
    else:
        query = """
            SELECT DISTINCT r.project_key || '/' || r.slug AS repo
            FROM pull_requests pr
            JOIN repos r ON r.id = pr.repo_id
            WHERE EXISTS (
                SELECT 1 FROM json_each(pr.reviewers) WHERE value = ?
            )
        """
        params = [reviewer]

    if since_ts:
        query += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND pr.created_date <= ?"
        params.append(until_ts)
    if state:
        query += " AND pr.state = ?"
        params.append(state)

    query += " ORDER BY repo"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No repositories found for the given filters.", flush=True)
        sys.exit(4)

    lines = [row["repo"] for row in rows]
    output = getattr(args, "output", None)

    if output:
        Path(output).write_text("\n".join(lines) + "\n")
        print(f"{len(lines)} repositories written to {output}", flush=True)
    else:
        for line in lines:
            print(line)
