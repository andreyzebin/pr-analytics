"""
find-repos: list repos with PRs matching filters.

Output: one row per repo with PR count. With --output, writes a plain
`PROJ/repo` list compatible with `plot --repos-file`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms, format_output


def cmd_find_repos(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)
    author = getattr(args, "author", None)
    reviewer = getattr(args, "reviewer", None)
    commenter = getattr(args, "commenter", None)
    fmt = getattr(args, "format", "table")
    output = getattr(args, "output", None)

    query = """
        SELECT
            r.project_key AS project,
            r.project_key || '/' || r.slug AS repo,
            COUNT(*) AS prs
        FROM pull_requests pr
        JOIN repos r ON r.id = pr.repo_id
        WHERE 1=1
    """
    params: list[Any] = []

    repos = collect_repos_from_args(args, conn)
    if repos:
        repo_ids = []
        for proj_key, slug in repos:
            row = conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, slug)
            ).fetchone()
            if row:
                repo_ids.append(row["id"])
        if not repo_ids:
            print("No matching repos found.", flush=True)
            conn.close()
            sys.exit(4)
        query += f" AND pr.repo_id IN ({','.join('?' * len(repo_ids))})"
        params.extend(repo_ids)

    if author:
        query += " AND pr.author = ?"
        params.append(author)
    if state:
        query += " AND pr.state = ?"
        params.append(state)
    if since_ts:
        query += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND pr.created_date <= ?"
        params.append(until_ts)
    if reviewer:
        query += " AND EXISTS (SELECT 1 FROM json_each(pr.reviewers) WHERE value = ?)"
        params.append(reviewer)
    if commenter:
        query += """
            AND EXISTS (SELECT 1 FROM pr_comments c
                        WHERE c.repo_id = pr.repo_id AND c.pr_id = pr.pr_id AND c.author = ?)
        """
        params.append(commenter)

    query += " GROUP BY r.id ORDER BY r.project_key, r.slug"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No repositories found for the given filters.", flush=True)
        sys.exit(4)

    if output:
        Path(output).write_text("\n".join(r["repo"] for r in rows) + "\n")
        print(f"{len(rows)} repositories written to {output}", flush=True)
        return

    data = [{"project": r["project"], "repo": r["repo"], "prs": r["prs"]} for r in rows]
    print(format_output(data, ["project", "repo", "prs"], fmt))
    print(f"\n{len(data)} repositories found ({sum(r['prs'] for r in rows)} PRs).", flush=True)
