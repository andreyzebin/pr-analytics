"""
find-prs command: list PRs matching filters.

Outputs repo#pr_id, title, author, state, dates.
Filterable by projects, repos, author, reviewer, commenter, state, date range.
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date, format_output


def cmd_find_prs(args: argparse.Namespace, cfg: dict) -> None:
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
    limit = getattr(args, "limit", 1000)

    # ── Build query ───────────────────────────────────────────────────────
    query = """
        SELECT
            r.project_key || '/' || r.slug AS repo,
            pr.pr_id,
            pr.title,
            pr.author,
            pr.state,
            pr.created_date,
            pr.closed_date
        FROM pull_requests pr
        JOIN repos r ON r.id = pr.repo_id
        WHERE 1=1
    """
    params: list[Any] = []

    # Repo filter
    repos = collect_repos_from_args(args, conn)
    if repos:
        repo_ids = []
        for proj_key, slug in repos:
            row = conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, slug)
            ).fetchone()
            if row:
                repo_ids.append(row["id"])
        if repo_ids:
            query += f" AND pr.repo_id IN ({','.join('?' * len(repo_ids))})"
            params.extend(repo_ids)
        else:
            print("No matching repos found.", flush=True)
            conn.close()
            sys.exit(4)

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

    query += " ORDER BY pr.created_date DESC"
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No PRs found for the given filters.", flush=True)
        sys.exit(4)

    data = [
        {
            "repo": r["repo"],
            "pr_id": r["pr_id"],
            "title": (r["title"] or "")[:80],
            "author": r["author"],
            "state": r["state"],
            "created": ms_to_date(r["created_date"]),
            "closed": ms_to_date(r["closed_date"]),
        }
        for r in rows
    ]
    columns = ["repo", "pr_id", "title", "author", "state", "created", "closed"]
    text = format_output(data, columns, fmt)

    if output:
        from pathlib import Path
        Path(output).write_text(text, encoding="utf-8")
        print(f"{len(data)} PRs written to {output}", flush=True)
    else:
        print(text)
        print(f"\n{len(data)} PRs found.", flush=True)
