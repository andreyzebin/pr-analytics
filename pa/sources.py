"""DSL source fetchers — pull rows from non-PR tables (joined with PR for bucketing).

Each function takes a `vars` dict containing CLI args and context keys
(`_conn`, `_since_ts`, `_until_ts`, `_repo_ids`) and returns a list of dict
rows. The returned rows always include `closed_date` (from PR) for default
bucketing, plus the source-specific fields needed by downstream filters.
"""
from __future__ import annotations

import sys
from typing import Any


def pr_source(vars: dict) -> list[dict]:
    """Default `@pr` source — returns the pre-fetched PR rows that cmd_plot
    augments with commenters/agent_comment_count/first_comment_date/repo_label.

    Unlike the other sources, this one doesn't issue a query: cmd_plot already
    fetched these rows once for series evaluation. The function exists so that
    `@pr` can appear explicitly in DSL expressions and `--explain` output
    (uniform with @comments / @analysis / @merge).
    """
    return list(vars.get("_pr_rows") or [])


def _ctx(vars: dict) -> tuple[Any, tuple[str, list]]:
    """Pull conn + WHERE clauses bound to context (since/until/repos)."""
    conn = vars.get("_conn")
    if conn is None:
        # If no conn is supplied (e.g., in unit tests), return a no-op.
        return None, ("", [])
    extra: list[str] = []
    params: list[Any] = []
    if vars.get("_since_ts"):
        extra.append("pr.created_date >= ?")
        params.append(vars["_since_ts"])
    if vars.get("_until_ts"):
        extra.append("pr.created_date <= ?")
        params.append(vars["_until_ts"])
    repo_ids = vars.get("_repo_ids")
    if repo_ids:
        extra.append(f"pr.repo_id IN ({','.join('?' * len(repo_ids))})")
        params.extend(repo_ids)
    where = (" AND " + " AND ".join(extra)) if extra else ""
    return conn, (where, params)


def comments_source(vars: dict) -> list[dict]:
    """pr_comments JOIN pull_requests, with derived has_reaction / has_reply.

    Output rows: id, author, parent_id, file_path, severity,
                 closed_date, state, repo_id, pr_id,
                 has_reaction (bool), has_reply (bool).
    """
    conn, (where, params) = _ctx(vars)
    if conn is None:
        return []
    rows = conn.execute(f"""
        SELECT c.id, c.author, c.parent_id, c.file_path, c.severity,
               pr.closed_date, pr.created_date, pr.state, pr.repo_id, pr.pr_id,
               pr.reviewers, r.project_key,
               EXISTS (SELECT 1 FROM comment_reactions cr
                       WHERE cr.comment_id = c.id) AS has_reaction,
               EXISTS (SELECT 1 FROM pr_comments reply
                       WHERE reply.parent_id = c.id AND reply.author != c.author)
                       AS has_reply
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = pr.repo_id
        WHERE pr.closed_date IS NOT NULL{where}
    """, params).fetchall()
    return [dict(r) for r in rows]


def analysis_source(vars: dict) -> list[dict]:
    """comment_analysis JOIN pr_comments JOIN PR. Filtered by judge_model.

    Output rows: comment_id, author (of comment), verdict, judge_model,
                 closed_date, state, repo_id, pr_id.
    """
    conn, (where, params) = _ctx(vars)
    if conn is None:
        return []
    judge_model = vars.get("judge_model")
    if not judge_model:
        print("DSL @analysis: judge_model is required", file=sys.stderr)
        return []
    rows = conn.execute(f"""
        SELECT ca.comment_id, c.author, ca.verdict, ca.judge_model,
               pr.closed_date, pr.created_date, pr.state, pr.repo_id, pr.pr_id, pr.reviewers, r.project_key
        FROM comment_analysis ca
        JOIN pr_comments c ON c.id = ca.comment_id
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = pr.repo_id
        WHERE ca.judge_model = ? AND pr.closed_date IS NOT NULL{where}
    """, [judge_model] + params).fetchall()
    return [dict(r) for r in rows]


def merge_source(vars: dict) -> list[dict]:
    """merge_analysis (latest analyzer_version per (comment, judge)) JOIN comments JOIN PR.

    Output rows: comment_id, author, verdict, judge_model, analyzer_version,
                 closed_date, state, repo_id, pr_id.
    """
    conn, (where, params) = _ctx(vars)
    if conn is None:
        return []
    judge_model = vars.get("judge_model")
    if not judge_model:
        print("DSL @merge: judge_model is required", file=sys.stderr)
        return []
    rows = conn.execute(f"""
        SELECT ma.comment_id, c.author, ma.verdict, ma.judge_model,
               ma.analyzer_version, pr.closed_date, pr.created_date, pr.state,
               pr.repo_id, pr.pr_id, pr.reviewers, r.project_key
        FROM merge_analysis ma
        JOIN pr_comments c ON c.id = ma.comment_id
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = pr.repo_id
        WHERE ma.judge_model = ? AND pr.closed_date IS NOT NULL{where}
          AND ma.analyzed_at = (
              SELECT MAX(ma2.analyzed_at) FROM merge_analysis ma2
              WHERE ma2.comment_id = ma.comment_id AND ma2.judge_model = ma.judge_model
          )
    """, [judge_model] + params).fetchall()
    return [dict(r) for r in rows]
