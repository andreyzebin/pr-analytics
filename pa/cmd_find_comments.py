"""
find-comments command: list comments matching filters.

Outputs comment details with repo, PR, author, file, line, text snippet.
Filterable by projects, repos, comment author, PR author, state, date range, severity.
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date, format_output


def cmd_find_comments(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)
    author = getattr(args, "author", None)
    pr_author = getattr(args, "pr_author", None)
    severity = getattr(args, "severity", None)
    file_only = getattr(args, "file_only", False)
    root_only = not getattr(args, "include_replies", False)
    fmt = getattr(args, "format", "table")
    output = getattr(args, "output", None)
    limit = getattr(args, "limit", 1000)

    # ── Build query ───────────────────────────────────────────────────────
    query = """
        SELECT
            r.project_key || '/' || r.slug AS repo,
            c.pr_id,
            pr.title AS pr_title,
            c.id AS comment_id,
            c.author,
            c.created_date,
            c.severity,
            c.file_path,
            c.line,
            c.text,
            pr.state AS pr_state,
            pr.author AS pr_author
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
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
            query += f" AND c.repo_id IN ({','.join('?' * len(repo_ids))})"
            params.extend(repo_ids)
        else:
            print("No matching repos found.", flush=True)
            conn.close()
            sys.exit(4)

    if root_only:
        query += " AND c.parent_id IS NULL"
    if author:
        query += " AND c.author = ?"
        params.append(author)
    if pr_author:
        query += " AND pr.author = ?"
        params.append(pr_author)
    if state:
        query += " AND pr.state = ?"
        params.append(state)
    if severity:
        query += " AND c.severity = ?"
        params.append(severity)
    if file_only:
        query += " AND c.file_path IS NOT NULL"
    if since_ts:
        query += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND pr.created_date <= ?"
        params.append(until_ts)

    query += " ORDER BY c.created_date DESC"
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No comments found for the given filters.", flush=True)
        sys.exit(4)

    data = [
        {
            "repo": r["repo"],
            "pr_id": r["pr_id"],
            "comment_id": r["comment_id"],
            "author": r["author"],
            "date": ms_to_date(r["created_date"]),
            "severity": r["severity"] or "",
            "file": (r["file_path"] or "")[:50],
            "line": r["line"] or "",
            "text": (r["text"] or "").replace("\n", " ")[:100],
            "pr_state": r["pr_state"],
        }
        for r in rows
    ]
    columns = ["repo", "pr_id", "comment_id", "author", "date", "severity", "file", "line", "text", "pr_state"]
    text = format_output(data, columns, fmt)

    if output:
        from pathlib import Path
        Path(output).write_text(text, encoding="utf-8")
        print(f"{len(data)} comments written to {output}", flush=True)
    else:
        print(text)
        print(f"\n{len(data)} comments found.", flush=True)
