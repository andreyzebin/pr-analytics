from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms, format_output, ms_to_date

POSITIVE_EMOJIS = {"+1", "thumbsup", "heart", "tada"}
NEGATIVE_EMOJIS = {"-1", "thumbsdown"}


def cmd_review_feedback(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    author = args.author
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)
    min_reactions = getattr(args, "min_reactions", 0)
    fmt = getattr(args, "format", "table")

    repo_ids: Optional[list[int]] = None
    repos = collect_repos_from_args(args, conn)
    if repos:
        repo_ids = []
        for proj_key, repo_slug in repos:
            row = conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
            ).fetchone()
            if row:
                repo_ids.append(row["id"])

    query = """
        SELECT
            c.id AS comment_id,
            c.repo_id,
            c.pr_id,
            c.created_date,
            c.file_path,
            c.line,
            c.severity,
            c.text AS comment_text,
            pr.title AS pr_title,
            pr.state AS pr_state,
            r.project_key || '/' || r.slug AS repo
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
        WHERE c.author = ? AND c.parent_id IS NULL
    """
    params: list[Any] = [author]

    if since_ts:
        query += " AND c.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND c.created_date <= ?"
        params.append(until_ts)
    if state:
        query += " AND pr.state = ?"
        params.append(state)
    if repo_ids is not None:
        if not repo_ids:
            print("Нет данных", flush=True)
            conn.close()
            return
        placeholders = ",".join("?" * len(repo_ids))
        query += f" AND c.repo_id IN ({placeholders})"
        params.extend(repo_ids)

    query += " ORDER BY c.created_date DESC"

    comment_rows = conn.execute(query, params).fetchall()
    if not comment_rows:
        print("Нет данных", flush=True)
        conn.close()
        return

    results = []
    for row in comment_rows:
        comment_id = row["comment_id"]

        reaction_rows = conn.execute(
            "SELECT emoji, author FROM comment_reactions WHERE comment_id=?", (comment_id,)
        ).fetchall()

        reactions_pos = reactions_neg = reactions_other = 0
        reactions_detail: dict[str, int] = {}
        for r in reaction_rows:
            emoji = r["emoji"]
            reactions_detail[emoji] = reactions_detail.get(emoji, 0) + 1
            if emoji in POSITIVE_EMOJIS:
                reactions_pos += 1
            elif emoji in NEGATIVE_EMOJIS:
                reactions_neg += 1
            else:
                reactions_other += 1

        if min_reactions > 0 and (reactions_pos + reactions_neg + reactions_other) < min_reactions:
            continue

        reply_rows = conn.execute(
            """SELECT author, text, created_date FROM pr_comments
               WHERE parent_id=? AND author != ? ORDER BY created_date""",
            (comment_id, author),
        ).fetchall()
        replies = [{"author": r["author"], "text": r["text"], "created_date": r["created_date"]}
                   for r in reply_rows]

        results.append({
            "repo": row["repo"],
            "pr_id": row["pr_id"],
            "pr_title": row["pr_title"],
            "comment_id": comment_id,
            "created_date": ms_to_date(row["created_date"]),
            "file_path": row["file_path"],
            "line_from": row["line"],
            "line_to": row["line"],
            "severity": row["severity"],
            "comment_text": row["comment_text"],
            "reactions_positive": reactions_pos,
            "reactions_negative": reactions_neg,
            "reactions_other": reactions_other,
            "reactions_detail": reactions_detail,
            "replies_count": len(replies),
            "replies": replies,
        })

    conn.close()

    if not results:
        print("Нет данных", flush=True)
        return

    if fmt == "json":
        result_text = json.dumps(results, ensure_ascii=False, indent=2)
    else:
        columns = [
            "repo", "pr_id", "pr_title", "comment_id", "created_date",
            "file_path", "line_from", "line_to", "severity", "comment_text",
            "reactions_positive", "reactions_negative", "reactions_other",
            "reactions_detail", "replies_count", "replies",
        ]
        flat = []
        for r in results:
            fr = dict(r)
            fr["reactions_detail"] = json.dumps(r["reactions_detail"], ensure_ascii=False)
            fr["replies"] = json.dumps(r["replies"], ensure_ascii=False)
            flat.append(fr)
        result_text = format_output(flat, columns, fmt)

    output_path = getattr(args, "output", None)
    if output_path:
        Path(output_path).write_text(result_text, encoding="utf-8")
        print(f"{len(results)} comments written to {output_path}", flush=True)
    else:
        print(result_text)
