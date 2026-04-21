"""
analyze-merges command: check if agent comments led to actual code changes.

For each root comment with a file anchor (file_path != NULL), fetches
the PR diff for that file from Bitbucket and asks an LLM judge whether
the comment was addressed in the final changes.

Results stored in merge_analysis table. Idempotent — skips already analyzed.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from urllib.parse import quote

from pa.api import api_get_text, make_session
from pa.config import (
    resolve_db, resolve_judge_api_key, resolve_judge_base_url,
    resolve_judge_model, resolve_token, resolve_url,
)
from pa.db import open_db
from pa.judge import LLMJudge
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / "prompts" / "judge_merge_acceptance.txt"

# In-memory cache: (repo_id, pr_id, file_path) → diff_text
_diff_cache: dict[tuple, str | None] = {}


def _fetch_diff(
    session, bb_url: str,
    project_key: str, repo_slug: str, pr_id: int,
    file_path: str,
    repo_id: int,
) -> str | None:
    """Fetch diff for a specific file in a PR. Caches per (repo_id, pr_id, file_path)."""
    key = (repo_id, pr_id, file_path)
    if key in _diff_cache:
        return _diff_cache[key]
    encoded_path = quote(file_path, safe="")
    url = (
        f"{bb_url}/rest/api/1.0/projects/{project_key}/repos/{repo_slug}"
        f"/pull-requests/{pr_id}/diff/{encoded_path}?contextLines=5"
    )
    text = api_get_text(session, url)
    _diff_cache[key] = text
    return text


def _truncate_diff(diff: str, max_chars: int = 4000) -> str:
    if len(diff) <= max_chars:
        return diff
    half = max_chars // 2
    return diff[:half] + "\n... [truncated] ...\n" + diff[-half:]


def cmd_merge_analysis(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    author = args.author
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    judge_model = resolve_judge_model(getattr(args, "judge_model", None), cfg)
    batch_size = getattr(args, "batch_size", 50)
    dry_run = getattr(args, "dry_run", False)
    budget_tokens = getattr(args, "budget_tokens", None)
    max_comment_chars = getattr(args, "max_comment_chars", 2000)
    max_diff_chars = getattr(args, "max_diff_chars", 4000)

    bb_url = resolve_url(None, cfg)
    token = resolve_token(None, cfg)
    if not bb_url or not token:
        log.error("Bitbucket URL and token required. Set in config or env vars.")
        sys.exit(1)

    api_key = resolve_judge_api_key(cfg)
    if not dry_run and not api_key:
        log.error("No API key found. Set ANTHROPIC_API_KEY or DEEPSEEK_API_KEY.")
        sys.exit(1)
    base_url = resolve_judge_base_url(cfg)

    conn = open_db(db_path)
    bb_url = bb_url.rstrip("/")

    # ── resolve repo filter ────────────────────────────────────────────────
    repos = collect_repos_from_args(args, conn)
    repo_ids: list[int] | None = None
    if repos:
        repo_ids = []
        for proj_key, repo_slug in repos:
            row = conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
            ).fetchone()
            if row:
                repo_ids.append(row["id"])
        if not repo_ids:
            log.error("No matching repos found in cache.")
            conn.close()
            sys.exit(4)

    # ── find unanalyzed comments with file anchors ─────────────────────────
    q = """
        SELECT
            c.id          AS comment_id,
            c.repo_id,
            c.pr_id,
            c.text        AS comment_text,
            c.file_path,
            c.line,
            c.severity,
            pr.title      AS pr_title,
            pr.closed_date,
            r.project_key,
            r.slug
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
        WHERE c.author = ?
          AND c.parent_id IS NULL
          AND c.file_path IS NOT NULL
          AND pr.closed_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM merge_analysis ma
              WHERE ma.comment_id = c.id AND ma.judge_model = ?
          )
    """
    params: list = [author, judge_model]

    if since_ts:
        q += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        q += " AND pr.created_date <= ?"
        params.append(until_ts)
    if repo_ids:
        q += f" AND c.repo_id IN ({','.join('?' * len(repo_ids))})"
        params.extend(repo_ids)

    q += " ORDER BY pr.closed_date, c.id"
    if batch_size:
        q += f" LIMIT {batch_size}"

    pending = conn.execute(q, params).fetchall()
    total = len(pending)

    if total == 0:
        print(f"No unanalyzed file-anchored comments for author={author!r} judge={judge_model!r}.")
        conn.close()
        return

    print(
        f"Found {total} file-anchored comment(s) for author={author!r}\n"
        f"Judge model: {judge_model}\n"
        f"{'DRY RUN — no API calls will be made' if dry_run else ''}"
    )

    if dry_run:
        for row in pending:
            repo = f"{row['project_key']}/{row['slug']}"
            print(
                f"  {ms_to_date(row['closed_date'])}  {repo}#{row['pr_id']}"
                f"  {row['file_path']}:{row['line']}"
                f"  {(row['comment_text'] or '')[:50]!r}"
            )
        conn.close()
        return

    # ── run ─────────────────────────────────────────────────────────────────
    prompt_template = _PROMPT_PATH.read_text(encoding="utf-8")
    from pa.config import resolve_judge_tool_choice
    tool_choice = resolve_judge_tool_choice(cfg)
    judge = LLMJudge(model=judge_model, api_key=api_key, base_url=base_url, tool_choice=tool_choice)
    session = make_session(token, cfg)

    now_ms = int(time.time() * 1000)
    n_yes = n_partial = n_no = n_skip = n_error = 0
    total_tokens = 0
    start = time.monotonic()

    for i, row in enumerate(pending, 1):
        repo = f"{row['project_key']}/{row['slug']}"
        comment_id = row["comment_id"]

        # Check budget
        if budget_tokens and total_tokens >= budget_tokens:
            print(f"\nToken budget reached: {total_tokens:,} / {budget_tokens:,}. Stopping.")
            break

        # Fetch diff
        diff = _fetch_diff(
            session, bb_url,
            row["project_key"], row["slug"], row["pr_id"],
            row["file_path"], row["repo_id"],
        )
        if not diff:
            n_skip += 1
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}"
                f"  SKIP (no diff)",
                flush=True,
            )
            continue

        diff_truncated = _truncate_diff(diff, max_diff_chars)
        comment_text = (row["comment_text"] or "")[:max_comment_chars]

        prompt = prompt_template.format(
            pr_title=row["pr_title"] or "",
            repo=repo,
            file_path=row["file_path"],
            line=row["line"] or "?",
            comment_text=comment_text,
            diff_content=diff_truncated,
        )

        try:
            data, tokens = judge.call_json(prompt)
            total_tokens += tokens
            verdict = str(data.get("verdict", "NO")).upper()
            if verdict not in ("YES", "PARTIAL", "NO"):
                verdict = "NO"
            confidence = float(data.get("confidence", 0.5))
            reasoning = str(data.get("reasoning", ""))

            conn.execute(
                """INSERT OR REPLACE INTO merge_analysis
                   (comment_id, judge_model, verdict, confidence, reasoning, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (comment_id, judge_model, verdict, confidence, reasoning, now_ms),
            )
            conn.commit()

            if verdict == "YES":
                n_yes += 1
            elif verdict == "PARTIAL":
                n_partial += 1
            else:
                n_no += 1

            elapsed = time.monotonic() - start
            eta = elapsed / i * (total - i)
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}:{row['line']}"
                f"  → {verdict} ({confidence:.1f})"
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {total_tokens:,}tok]",
                flush=True,
            )

        except Exception as exc:
            n_error += 1
            log.warning("Failed to analyze comment %d: %s", comment_id, exc)
            print(f"  [{i}/{total}]  comment#{comment_id}  ERROR: {exc}", flush=True)

    conn.close()

    elapsed = time.monotonic() - start
    total_decided = n_yes + n_partial + n_no
    rate = f"{(n_yes + n_partial * 0.5) / total_decided * 100:.0f}%" if total_decided else "n/a"
    print(
        f"\nDone in {int(elapsed)}s. "
        f"YES={n_yes}  PARTIAL={n_partial}  NO={n_no}  skip={n_skip}  error={n_error}  "
        f"merge_acceptance={rate}  tokens={total_tokens:,}  (judge={judge_model})"
    )
