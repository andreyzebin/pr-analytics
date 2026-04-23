"""
analyze-feedback command: run LLM judge on agent comments.

Finds root comments by --author that have no entry in comment_analysis
for the current judge_model, fetches context (PR title, replies,
reactions), calls LLM judge, and saves verdicts to comment_analysis.

Already-analyzed comments (same comment_id + judge_model) are skipped.
Switching to a different judge_model re-analyzes everything.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from pa.config import (
    DEFAULT_JUDGE_MODEL,
    resolve_db,
    resolve_judge_api_key,
    resolve_judge_base_url,
    resolve_judge_model,
)
from pa.db import open_db
from pa.judge import LLMJudge
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / "prompts" / "judge_acceptance.txt"

POSITIVE_EMOJIS = {"+1", "thumbsup", "heart", "tada"}
NEGATIVE_EMOJIS = {"-1", "thumbsdown"}


def _load_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


def _fmt_reactions(reactions: list[tuple[str, str]]) -> str:
    if not reactions:
        return "нет реакций"
    parts = []
    for author, emoji in reactions:
        parts.append(f"{emoji} от {author}")
    return ", ".join(parts)


def _fmt_replies(replies: list[tuple[str, str]]) -> str:
    if not replies:
        return "  (ответов нет)"
    return "\n".join(f"  - {author}: {text}" for author, text in replies)


def _fmt_location(file_path: str | None, line: int | None) -> str:
    if file_path:
        loc = f" [{file_path}"
        if line:
            loc += f":{line}"
        return loc + "]"
    return ""


def _build_prompt(
    template: str,
    pr_title: str,
    repo: str,
    severity: str,
    file_path: str | None,
    line: int | None,
    comment_text: str,
    reactions: list[tuple[str, str]],
    replies: list[tuple[str, str]],
) -> str:
    return template.format(
        pr_title=pr_title,
        repo=repo,
        severity=severity or "NORMAL",
        location=_fmt_location(file_path, line),
        comment_text=comment_text,
        reactions=_fmt_reactions(reactions),
        replies=_fmt_replies(replies),
    )


def cmd_analyze_feedback(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    author = args.author
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    judge_model = resolve_judge_model(getattr(args, "judge_model", None), cfg)
    batch_size = getattr(args, "batch_size", 50)
    dry_run = getattr(args, "dry_run", False)
    force = getattr(args, "force", False)
    verbose = getattr(args, "verbose", False)
    budget_tokens = getattr(args, "budget_tokens", None)   # None = unlimited
    max_comment_chars = getattr(args, "max_comment_chars", 2000)

    api_key = resolve_judge_api_key(cfg)
    if not dry_run and not api_key:
        log.error(
            "No API key found. Set ANTHROPIC_API_KEY env var or judge.api_key in config."
        )
        sys.exit(1)

    base_url = resolve_judge_base_url(cfg)

    conn = open_db(db_path)

    # ── resolve repo filter ────────────────────────────────────────────────────
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

    # ── find unanalyzed comments ───────────────────────────────────────────────
    q = """
        SELECT
            c.id          AS comment_id,
            c.repo_id,
            c.pr_id,
            c.text        AS comment_text,
            c.severity,
            c.file_path,
            c.line,
            c.created_date,
            pr.title      AS pr_title,
            pr.closed_date,
            r.project_key,
            r.slug
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
        WHERE c.author = ?
          AND c.parent_id IS NULL
          AND pr.closed_date IS NOT NULL
          AND (
              EXISTS (SELECT 1 FROM comment_reactions cr WHERE cr.comment_id = c.id)
              OR EXISTS (SELECT 1 FROM pr_comments reply
                         WHERE reply.parent_id = c.id AND reply.author != ?)
          )
    """
    params: list = [author, author]
    if not force:
        q += """
          AND NOT EXISTS (
              SELECT 1 FROM comment_analysis ca
              WHERE ca.comment_id = c.id AND ca.judge_model = ?
          )
        """
        params.append(judge_model)

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

    # Count total unanalyzed (with + without feedback) to report skipped count
    q_total_unanalyzed = """
        SELECT COUNT(*) FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        WHERE c.author = ?
          AND c.parent_id IS NULL
          AND pr.closed_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM comment_analysis ca
              WHERE ca.comment_id = c.id AND ca.judge_model = ?
          )
    """
    p_total: list = [author, judge_model]
    if since_ts:
        q_total_unanalyzed += " AND pr.created_date >= ?"
        p_total.append(since_ts)
    if until_ts:
        q_total_unanalyzed += " AND pr.created_date <= ?"
        p_total.append(until_ts)
    if repo_ids:
        q_total_unanalyzed += f" AND c.repo_id IN ({','.join('?' * len(repo_ids))})"
        p_total.extend(repo_ids)
    total_unanalyzed = conn.execute(q_total_unanalyzed, p_total).fetchone()[0]
    skipped_no_feedback = total_unanalyzed - total

    if total == 0:
        print(
            f"No unanalyzed comments with feedback found for author={author!r} judge={judge_model!r}.\n"
            f"({skipped_no_feedback} comment(s) skipped — no reactions or replies)"
        )
        conn.close()
        return

    print(
        f"Found {total} unanalyzed comment(s) with feedback for author={author!r}\n"
        f"  ({skipped_no_feedback} skipped — no reactions or replies)\n"
        f"Judge model: {judge_model}\n"
        f"{'DRY RUN — no API calls will be made' if dry_run else ''}"
    )

    if dry_run:
        for row in pending:
            repo = f"{row['project_key']}/{row['slug']}"
            print(
                f"  {ms_to_date(row['closed_date'])}  {repo}#{row['pr_id']}"
                f"  comment#{row['comment_id']}  {(row['comment_text'] or '')[:60]!r}"
            )
        conn.close()
        return

    # ── run judge ─────────────────────────────────────────────────────────────
    prompt_template = _load_prompt()
    from pa.config import resolve_judge_tool_choice
    tool_choice = resolve_judge_tool_choice(cfg)
    judge = LLMJudge(model=judge_model, api_key=api_key, base_url=base_url, tool_choice=tool_choice)

    now_ms = int(time.time() * 1000)
    n_yes = n_no = n_unclear = n_error = 0
    total_tokens = 0
    start = time.monotonic()

    for i, row in enumerate(pending, 1):
        repo = f"{row['project_key']}/{row['slug']}"
        comment_id = row["comment_id"]

        # Fetch reactions
        reactions = [
            (r["author"], r["emoji"])
            for r in conn.execute(
                "SELECT author, emoji FROM comment_reactions WHERE comment_id = ?",
                (comment_id,),
            ).fetchall()
        ]

        # Fetch human replies (non-agent)
        replies = [
            (r["author"], r["text"])
            for r in conn.execute(
                """SELECT author, text FROM pr_comments
                   WHERE parent_id = ? AND author != ?
                   ORDER BY created_date""",
                (comment_id, author),
            ).fetchall()
        ]

        comment_text = row["comment_text"] or ""
        if max_comment_chars and len(comment_text) > max_comment_chars:
            comment_text = comment_text[:max_comment_chars] + "…[truncated]"

        prompt = _build_prompt(
            template=prompt_template,
            pr_title=row["pr_title"] or "",
            repo=repo,
            severity=row["severity"] or "NORMAL",
            file_path=row["file_path"],
            line=row["line"],
            comment_text=comment_text,
            reactions=reactions,
            replies=replies,
        )

        if verbose:
            print(f"\n{'═' * 80}")
            print(f"[{i}/{total}]  {repo}#{row['pr_id']}  comment#{comment_id}")
            print(f"{'─' * 80}  PROMPT")
            print(prompt)

        try:
            verdict, raw = judge.judge_raw(prompt)
            total_tokens += verdict.tokens_used
            conn.execute(
                """INSERT OR REPLACE INTO comment_analysis
                   (comment_id, judge_model, verdict, confidence, reasoning, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (comment_id, judge_model, verdict.verdict, verdict.confidence,
                 verdict.reasoning, now_ms),
            )
            conn.commit()

            if verdict.verdict == "yes":
                n_yes += 1
            elif verdict.verdict == "no":
                n_no += 1
            else:
                n_unclear += 1

            elapsed = time.monotonic() - start
            eta = elapsed / i * (total - i)
            tokens_str = f"  ~{total_tokens:,}tok" if verdict.tokens_used else ""

            if verbose:
                print(f"{'─' * 80}  RAW RESPONSE")
                print(raw if raw else "(empty)")
                print(f"{'─' * 80}  PARSED: verdict={verdict.verdict} confidence={verdict.confidence} reasoning={verdict.reasoning!r}")
                print(f"{'═' * 80}")
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} comment#{comment_id}"
                f"  → {verdict.verdict} ({verdict.confidence})"
                f"  [{int(elapsed)}s, ~{int(eta)}s left{tokens_str}]",
                flush=True,
            )

            if budget_tokens and total_tokens >= budget_tokens:
                print(f"\nToken budget reached: {total_tokens:,} / {budget_tokens:,}. Stopping.")
                break

        except Exception as exc:
            n_error += 1
            raw = getattr(exc, "raw", None)
            tok = getattr(exc, "tokens_used", 0)
            if tok:
                total_tokens += tok
            log.warning("Failed to judge comment %d: %s", comment_id, exc)
            if verbose:
                print(f"{'─' * 80}  RAW RESPONSE (parse failed)")
                print(f"<<<{raw}>>>" if raw is not None else "(no raw response captured)")
                print(f"{'─' * 80}  ERROR: {exc}")
                print(f"{'═' * 80}")
            print(f"  [{i}/{total}]  comment#{comment_id}  ERROR: {exc}", flush=True)

    # ── full summary (including previously cached results) ─────────────────
    summary_q = """
        SELECT ca.verdict, COUNT(*) AS cnt
        FROM comment_analysis ca
        JOIN pr_comments c ON c.id = ca.comment_id
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        WHERE c.author = ? AND ca.judge_model = ?
          AND c.parent_id IS NULL AND pr.closed_date IS NOT NULL
    """
    summary_params: list = [author, judge_model]
    if since_ts:
        summary_q += " AND pr.created_date >= ?"
        summary_params.append(since_ts)
    if until_ts:
        summary_q += " AND pr.created_date <= ?"
        summary_params.append(until_ts)
    if repo_ids:
        summary_q += f" AND c.repo_id IN ({','.join('?' * len(repo_ids))})"
        summary_params.extend(repo_ids)
    summary_q += " GROUP BY ca.verdict"

    all_verdicts = {r["verdict"]: r["cnt"] for r in conn.execute(summary_q, summary_params).fetchall()}
    all_yes = all_verdicts.get("yes", 0)
    all_no = all_verdicts.get("no", 0)
    all_unclear = all_verdicts.get("unclear", 0)
    all_total = all_yes + all_no + all_unclear
    from_cache = all_total - (n_yes + n_no + n_unclear)
    all_decided = all_yes + all_no
    all_rate = f"{all_yes / all_decided * 100:.0f}%" if all_decided else "n/a"

    conn.close()

    elapsed = time.monotonic() - start
    tokens_summary = f"  tokens={total_tokens:,}" if total_tokens else ""
    print(
        f"\nDone in {int(elapsed)}s.  new: yes={n_yes} no={n_no} unclear={n_unclear} error={n_error}{tokens_summary}"
        f"\n\nTotal (incl. {from_cache} from cache):  "
        f"yes={all_yes}  no={all_no}  unclear={all_unclear}  "
        f"acceptance={all_rate}  (judge={judge_model})\n"
        f"Note: only comments with reactions or replies were analyzed "
        f"({skipped_no_feedback} without feedback were skipped)"
    )
