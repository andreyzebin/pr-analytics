"""
select-golden: Find high-quality PRs suitable as evaluation benchmarks for AI code reviewers.

Pipeline (--steps controls which phases run):
  heuristic — fast SQL filtering from cache (lifetime, reviewers, comments)
  classify  — LLM classification of comment type + depth per comment
  analyze   — LLM judge on unanalyzed comments (reuses analyze-feedback logic)
  score     — compute composite PR score from classifications (no LLM)
  judge     — final LLM verdict (GOLD / SILVER / REJECT) on top candidates
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pa.config import (
    resolve_db, resolve_judge_api_key, resolve_judge_base_url, resolve_judge_model, resolve_url,
)
from pa.db import open_db
from pa.judge import LLMJudge
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)

_CLASSIFY_PROMPT = Path(__file__).parent / "prompts" / "classify_comment.txt"
_JUDGE_PROMPT    = Path(__file__).parent / "prompts" / "judge_golden.txt"

VALID_TYPES = {
    "СТИЛЬ", "ПОВЕРХНОСТНАЯ_ЛОГИКА", "ГЛУБОКАЯ_ЛОГИКА",
    "АРХИТЕКТУРА", "ПРОИЗВОДИТЕЛЬНОСТЬ", "БЕЗОПАСНОСТЬ",
    "ТЕСТЫ", "БИЗНЕС_ЛОГИКА", "УСТОЙЧИВОСТЬ", "ЧИТАЕМОСТЬ",
}
DEEP_TYPES    = {"ГЛУБОКАЯ_ЛОГИКА", "АРХИТЕКТУРА", "БЕЗОПАСНОСТЬ", "БИЗНЕС_ЛОГИКА", "УСТОЙЧИВОСТЬ"}
SURFACE_TYPES = {"СТИЛЬ", "ПОВЕРХНОСТНАЯ_ЛОГИКА"}


# ── Budget tracker ─────────────────────────────────────────────────────────

@dataclass
class BudgetTracker:
    total_limit:    int | None = None
    classify_limit: int | None = None
    analyze_limit:  int | None = None
    judge_limit:    int | None = None
    total_used:    int = 0
    classify_used: int = 0
    analyze_used:  int = 0
    judge_used:    int = 0

    def add(self, tokens: int, step: str = "other") -> None:
        self.total_used += tokens
        if step == "classify":
            self.classify_used += tokens
        elif step == "analyze":
            self.analyze_used += tokens
        elif step == "judge":
            self.judge_used += tokens

    def ok(self, step: str = "other") -> bool:
        if self.total_limit and self.total_used >= self.total_limit:
            return False
        if step == "classify" and self.classify_limit and self.classify_used >= self.classify_limit:
            return False
        if step == "analyze" and self.analyze_limit and self.analyze_used >= self.analyze_limit:
            return False
        if step == "judge" and self.judge_limit and self.judge_used >= self.judge_limit:
            return False
        return True

    def summary(self) -> str:
        parts = [f"total={self.total_used:,}"]
        if self.classify_used:
            parts.append(f"classify={self.classify_used:,}")
        if self.analyze_used:
            parts.append(f"analyze={self.analyze_used:,}")
        if self.judge_used:
            parts.append(f"judge={self.judge_used:,}")
        return "  ".join(parts) + " tokens"


# ── Phase 1: Heuristic filter ──────────────────────────────────────────────

def _heuristic_filter(
    conn,
    repo_ids: list[int],
    since_ts: int | None,
    until_ts: int | None,
    min_lifetime_h: float,
    max_lifetime_h: float,
    min_reviewers: int,
    min_comments: int,
    max_comments: int,
) -> list[dict]:
    ph = ",".join("?" * len(repo_ids))
    params: list[Any] = list(repo_ids)
    q = f"""
        SELECT
            pr.repo_id, pr.pr_id, pr.title, pr.author,
            pr.created_date, pr.closed_date, pr.state,
            r.project_key, r.slug,
            (pr.closed_date - pr.created_date) / 3600000.0      AS lifetime_h,
            json_array_length(pr.reviewers)                      AS reviewer_count,
            COUNT(c.id) FILTER (WHERE c.parent_id IS NULL AND c.author != pr.author)
                AS root_comment_count,
            COUNT(c.id) FILTER (WHERE c.parent_id IS NOT NULL)   AS reply_count,
            COUNT(DISTINCT c.author) FILTER (WHERE c.author != pr.author)
                AS unique_commenters
        FROM pull_requests pr
        JOIN repos r ON r.id = pr.repo_id
        LEFT JOIN pr_comments c ON c.repo_id = pr.repo_id AND c.pr_id = pr.pr_id
        WHERE pr.repo_id IN ({ph})
          AND pr.state IN ('MERGED','DECLINED')
          AND pr.closed_date IS NOT NULL
    """
    if since_ts:
        q += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        q += " AND pr.created_date <= ?"
        params.append(until_ts)
    q += """
        GROUP BY pr.repo_id, pr.pr_id
        HAVING lifetime_h BETWEEN ? AND ?
           AND reviewer_count >= ?
           AND root_comment_count BETWEEN ? AND ?
           AND reply_count > 0
        ORDER BY pr.closed_date DESC
    """
    params.extend([min_lifetime_h, max_lifetime_h, min_reviewers, min_comments, max_comments])
    rows = conn.execute(q, params).fetchall()

    # Optional diff_stats filter
    result = []
    for r in rows:
        d = dict(r)
        ds = conn.execute(
            "SELECT files_changed, test_config_ratio FROM pr_diff_stats WHERE repo_id=? AND pr_id=?",
            (d["repo_id"], d["pr_id"]),
        ).fetchone()
        if ds:
            if not (2 <= (ds["files_changed"] or 0) <= 20):
                continue
            if (ds["test_config_ratio"] or 0) >= 0.4:
                continue
        d["files_changed"] = ds["files_changed"] if ds else None
        result.append(d)
    return result


# ── Live progress helper ──────────────────────────────────────────────────

import shutil

def _live(msg: str) -> None:
    """Overwrite current terminal line with msg."""
    w = shutil.get_terminal_size(fallback=(120, 24)).columns
    sys.stdout.write(f"\r  {msg:<{w - 4}}")
    sys.stdout.flush()

def _live_done(msg: str) -> None:
    """Print msg on a fresh line after _live calls."""
    w = shutil.get_terminal_size(fallback=(120, 24)).columns
    sys.stdout.write(f"\r  {msg:<{w - 4}}\n")
    sys.stdout.flush()


# ── Phase 2: Classify comments ──────────────────────────────────────────────

def _run_classify_step(
    conn,
    candidates: list[dict],
    judge: LLMJudge,
    classifier_model: str,
    budget: BudgetTracker,
    max_comment_chars: int,
) -> None:
    template = _CLASSIFY_PROMPT.read_text(encoding="utf-8")
    now_ms = int(time.time() * 1000)
    total_classified = 0
    start = time.monotonic()
    n_cand = len(candidates)

    for i, pr in enumerate(candidates, 1):
        if not budget.ok("classify"):
            _live_done(f"Classify budget reached ({budget.classify_used:,} tokens). Stopping.")
            break
        repo = f"{pr['project_key']}/{pr['slug']}"
        pr_ref = f"{repo}#{pr['pr_id']}"

        unclassified = conn.execute("""
            SELECT c.id, c.text, c.file_path, c.line
            FROM pr_comments c
            WHERE c.repo_id = ? AND c.pr_id = ?
              AND c.parent_id IS NULL
              AND c.author != ?
              AND NOT EXISTS (
                  SELECT 1 FROM comment_classification cc
                  WHERE cc.comment_id = c.id AND cc.classifier_model = ?
              )
        """, (pr["repo_id"], pr["pr_id"], pr["author"], classifier_model)).fetchall()

        n_total = len(unclassified)
        if n_total == 0:
            elapsed = time.monotonic() - start
            eta = elapsed / i * (n_cand - i) if i < n_cand else 0
            _live_done(
                f"[{i}/{n_cand}]  {pr_ref:<35}  0 new (all cached)"
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {budget.classify_used:,}tok]"
            )
            continue

        _live(f"[{i}/{n_cand}]  {pr_ref:<35}  classifying {n_total} comments...")

        n_done = 0
        for c in unclassified:
            if not budget.ok("classify"):
                break
            text = (c["text"] or "")[:max_comment_chars].strip()
            if not text:
                continue
            loc = ""
            if c["file_path"]:
                loc = f" [{c['file_path']}"
                if c["line"]:
                    loc += f":{c['line']}"
                loc += "]"
            prompt = template.format(
                pr_title=pr["title"] or "",
                repo=repo,
                location=loc,
                comment_text=text,
            )
            try:
                data, tokens = judge.call_json(prompt)
                budget.add(tokens, "classify")
                ctype = str(data.get("type", "")).strip().upper()
                if ctype not in VALID_TYPES:
                    ctype = "ЧИТАЕМОСТЬ"
                depth = int(data.get("depth", 2))
                if depth not in (1, 2, 3):
                    depth = 2
                conf = float(data.get("confidence", 0.5))
                conn.execute(
                    """INSERT OR REPLACE INTO comment_classification
                       (comment_id, classifier_model, comment_type, depth, confidence, classified_at)
                       VALUES (?,?,?,?,?,?)""",
                    (c["id"], classifier_model, ctype, depth, conf, now_ms),
                )
                conn.commit()
                n_done += 1
                total_classified += 1
                _live(f"[{i}/{n_cand}]  {pr_ref:<35}  {n_done}/{n_total}  {ctype} depth={depth}")
            except Exception as exc:
                log.warning("Failed to classify comment %d: %s", c["id"], exc)

        elapsed = time.monotonic() - start
        eta = elapsed / i * (n_cand - i) if i < n_cand else 0
        _live_done(
            f"[{i}/{n_cand}]  {pr_ref:<35}  {n_done}/{n_total} done"
            f"  [{int(elapsed)}s, ~{int(eta)}s left  {budget.classify_used:,}tok]"
        )

    elapsed = time.monotonic() - start
    print(f"  Classify done: {total_classified} comments in {int(elapsed)}s  ({budget.summary()})")


# ── Phase 2b: Analyze (acceptance verdict for unanalyzed comments) ──────────

_ACCEPTANCE_PROMPT = Path(__file__).parent / "prompts" / "judge_acceptance.txt"


def _run_analyze_step(
    conn,
    candidates: list[dict],
    judge: LLMJudge,
    analyze_model: str,
    budget: BudgetTracker,
    max_comment_chars: int,
) -> None:
    from pa.cmd_analyze import _build_prompt

    template = _ACCEPTANCE_PROMPT.read_text(encoding="utf-8")
    now_ms = int(time.time() * 1000)
    total_analyzed = 0
    n_yes = n_no = n_unclear = 0
    start = time.monotonic()
    n_cand = len(candidates)

    for i, pr in enumerate(candidates, 1):
        if not budget.ok("analyze"):
            _live_done(f"Analyze budget reached ({budget.summary()}). Stopping.")
            break
        repo = f"{pr['project_key']}/{pr['slug']}"
        pr_ref = f"{repo}#{pr['pr_id']}"

        unanalyzed = conn.execute("""
            SELECT c.id, c.text, c.severity, c.file_path, c.line, c.author
            FROM pr_comments c
            WHERE c.repo_id = ? AND c.pr_id = ?
              AND c.parent_id IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM comment_analysis ca
                  WHERE ca.comment_id = c.id AND ca.judge_model = ?
              )
              AND (
                  EXISTS (SELECT 1 FROM comment_reactions cr WHERE cr.comment_id = c.id)
                  OR EXISTS (SELECT 1 FROM pr_comments reply
                             WHERE reply.parent_id = c.id AND reply.author != c.author)
              )
        """, (pr["repo_id"], pr["pr_id"], analyze_model)).fetchall()

        n_total = len(unanalyzed)
        if n_total == 0:
            elapsed = time.monotonic() - start
            eta = elapsed / i * (n_cand - i) if i < n_cand else 0
            _live_done(
                f"[{i}/{n_cand}]  {pr_ref:<35}  0 new (all cached)"
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {budget.analyze_used:,}tok]"
            )
            continue

        _live(f"[{i}/{n_cand}]  {pr_ref:<35}  analyzing {n_total} comments...")

        n_done = 0
        pr_yes = pr_no = pr_unclear = 0
        for c in unanalyzed:
            if not budget.ok("analyze"):
                break
            text = (c["text"] or "")[:max_comment_chars].strip()
            if not text:
                continue

            comment_author = c["author"]
            reactions = [
                (r["author"], r["emoji"])
                for r in conn.execute(
                    "SELECT author, emoji FROM comment_reactions WHERE comment_id = ?",
                    (c["id"],),
                ).fetchall()
            ]
            replies = [
                (r["author"], r["text"])
                for r in conn.execute(
                    """SELECT author, text FROM pr_comments
                       WHERE parent_id = ? AND author != ?
                       ORDER BY created_date""",
                    (c["id"], comment_author),
                ).fetchall()
            ]

            prompt = _build_prompt(
                template=template,
                pr_title=pr["title"] or "",
                repo=repo,
                severity=c["severity"] or "NORMAL",
                file_path=c["file_path"],
                line=c["line"],
                comment_text=text,
                reactions=reactions,
                replies=replies,
            )
            try:
                verdict = judge.judge(prompt)
                budget.add(verdict.tokens_used, "analyze")
                conn.execute(
                    """INSERT OR REPLACE INTO comment_analysis
                       (comment_id, judge_model, verdict, confidence, reasoning, analyzed_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (c["id"], analyze_model, verdict.verdict, verdict.confidence,
                     verdict.reasoning, now_ms),
                )
                conn.commit()
                n_done += 1
                total_analyzed += 1
                if verdict.verdict == "yes":
                    pr_yes += 1; n_yes += 1
                elif verdict.verdict == "no":
                    pr_no += 1; n_no += 1
                else:
                    pr_unclear += 1; n_unclear += 1
                _live(
                    f"[{i}/{n_cand}]  {pr_ref:<35}  {n_done}/{n_total}"
                    f"  → {verdict.verdict} ({verdict.confidence})"
                )
            except Exception as exc:
                log.warning("Failed to judge comment %d: %s", c["id"], exc)

        elapsed = time.monotonic() - start
        eta = elapsed / i * (n_cand - i) if i < n_cand else 0
        _live_done(
            f"[{i}/{n_cand}]  {pr_ref:<35}  {n_done}/{n_total} done"
            f"  yes={pr_yes} no={pr_no} unclear={pr_unclear}"
            f"  [{int(elapsed)}s, ~{int(eta)}s left  {budget.analyze_used:,}tok]"
        )

    elapsed = time.monotonic() - start
    print(
        f"  Analyze done: {total_analyzed} comments in {int(elapsed)}s"
        f"  yes={n_yes} no={n_no} unclear={n_unclear}  ({budget.summary()})"
    )


# ── Phase 3: Score PRs ──────────────────────────────────────────────────────

def _score_pr(conn, pr: dict, classifier_model: str, judge_model: str | None) -> dict | None:
    classes = conn.execute("""
        SELECT cc.comment_type, cc.depth
        FROM comment_classification cc
        JOIN pr_comments c ON c.id = cc.comment_id
        WHERE c.repo_id = ? AND c.pr_id = ?
          AND c.parent_id IS NULL AND c.author != ?
          AND cc.classifier_model = ?
    """, (pr["repo_id"], pr["pr_id"], pr["author"], classifier_model)).fetchall()

    if not classes:
        return None

    types  = [c["comment_type"] for c in classes if c["comment_type"]]
    depths = [c["depth"] for c in classes if c["depth"]]
    if not types:
        return None

    unique_types = len(set(types))
    avg_depth    = statistics.mean(depths) if depths else 1.0
    has_deep     = any(t in DEEP_TYPES for t in types)
    style_count  = sum(1 for t in types if t == "СТИЛЬ")

    diversity    = min(unique_types, 3) / 3.0
    depth_score  = (avg_depth - 1) / 2.0
    style_noise  = 1.0 - style_count / len(types)

    # Size score from diff_stats if available
    ds = conn.execute(
        "SELECT lines_added, lines_deleted FROM pr_diff_stats WHERE repo_id=? AND pr_id=?",
        (pr["repo_id"], pr["pr_id"]),
    ).fetchone()
    if ds and ds["lines_added"] is not None:
        total_lines = (ds["lines_added"] or 0) + (ds["lines_deleted"] or 0)
        size_score = max(0.0, 1.0 - abs(total_lines - 200) / 200)
    else:
        size_score = 0.5  # neutral when unknown

    # Change score from comment_analysis if available
    change_score_ratio = None
    if judge_model:
        verdicts = conn.execute("""
            SELECT ca.verdict FROM comment_analysis ca
            JOIN pr_comments c ON c.id = ca.comment_id
            WHERE c.repo_id = ? AND c.pr_id = ? AND ca.judge_model = ?
              AND ca.verdict IN ('yes','no')
        """, (pr["repo_id"], pr["pr_id"], judge_model)).fetchall()
        if verdicts:
            yes_count = sum(1 for v in verdicts if v["verdict"] == "yes")
            change_score_ratio = yes_count / len(verdicts)

    # Weighted total
    if change_score_ratio is not None:
        # Have acceptance data → use it with 30% weight
        total = (diversity * 0.25 + depth_score * 0.25 +
                 change_score_ratio * 0.30 + style_noise * 0.10 + size_score * 0.10)
    else:
        # No acceptance data (all verdicts were "unclear" or no feedback at all)
        # → score only by comment quality, ignore acceptance signal
        total = (diversity * 0.35 + depth_score * 0.35 +
                 style_noise * 0.15 + size_score * 0.15)

    return {
        **pr,
        "unique_types":        unique_types,
        "types":               sorted(set(types)),
        "avg_depth":           round(avg_depth, 2),
        "has_deep":            has_deep,
        "diversity_score":     round(diversity, 3),
        "depth_score":         round(depth_score, 3),
        "change_score_ratio":  round(change_score_ratio, 3) if change_score_ratio is not None else None,
        "style_noise_score":   round(style_noise, 3),
        "size_score":          round(size_score, 3),
        "total_score":         round(total, 3),
        "verdict":             None,
        "verdict_reasoning":   None,
    }


def _run_score_step(
    conn,
    candidates: list[dict],
    classifier_model: str,
    judge_model: str | None,
    scorer_model: str,
    now_ms: int,
) -> list[dict]:
    scored = []
    for pr in candidates:
        s = _score_pr(conn, pr, classifier_model, judge_model)
        if s is None:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO pr_scores
            (repo_id, pr_id, scorer_model, unique_types, avg_depth,
             diversity_score, depth_score, change_score_ratio,
             style_noise_score, size_score, total_score, scored_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            s["repo_id"], s["pr_id"], scorer_model, s["unique_types"], s["avg_depth"],
            s["diversity_score"], s["depth_score"], s["change_score_ratio"],
            s["style_noise_score"], s["size_score"], s["total_score"], now_ms,
        ))
        scored.append(s)
    conn.commit()
    scored.sort(key=lambda x: x["total_score"], reverse=True)
    return scored


# ── Phase 4: Final judge ────────────────────────────────────────────────────

def _judge_pr(
    conn,
    pr: dict,
    judge: LLMJudge,
    judge_model: str,
    template: str,
    scorer_model: str,
    budget: BudgetTracker,
    max_comment_chars: int,
    now_ms: int,
) -> None:
    repo = f"{pr['project_key']}/{pr['slug']}"

    # Build comments summary (type, depth, text snippet)
    classes = conn.execute("""
        SELECT cc.comment_type, cc.depth, c.text
        FROM comment_classification cc
        JOIN pr_comments c ON c.id = cc.comment_id
        WHERE c.repo_id = ? AND c.pr_id = ?
          AND c.parent_id IS NULL AND c.author != ?
          AND cc.classifier_model = ?
        ORDER BY cc.depth DESC, c.created_date
    """, (pr["repo_id"], pr["pr_id"], pr["author"], scorer_model)).fetchall()

    lines = []
    for c in classes:
        snippet = (c["text"] or "")[:120].replace("\n", " ")
        lines.append(f"[{c['comment_type']}, depth={c['depth']}] {snippet}")
    comments_summary = "\n".join(lines) or "(нет классифицированных комментариев)"

    prompt = template.format(
        pr_title=pr["title"] or "",
        repo=repo,
        state=pr["state"],
        comment_count=pr["root_comment_count"],
        unique_types=pr["unique_types"],
        avg_depth=f"{pr['avg_depth']:.1f}",
        types_list=", ".join(pr["types"]),
        comments_summary=comments_summary,
    )
    try:
        data, tokens = judge.call_json(prompt)
        budget.add(tokens, "judge")
        verdict = str(data.get("verdict", "REJECT")).upper()
        if verdict not in ("GOLD", "SILVER", "REJECT"):
            verdict = "REJECT"
        reasoning = str(data.get("reasoning", ""))
        conn.execute("""
            UPDATE pr_scores SET verdict=?, verdict_reasoning=?, scored_at=?
            WHERE repo_id=? AND pr_id=? AND scorer_model=?
        """, (verdict, reasoning, now_ms, pr["repo_id"], pr["pr_id"], scorer_model))
        conn.commit()
        pr["verdict"] = verdict
        pr["verdict_reasoning"] = reasoning
    except Exception as exc:
        log.warning("Failed to judge PR %s#%d: %s", repo, pr["pr_id"], exc)


def _run_judge_step(
    conn,
    scored: list[dict],
    judge: LLMJudge,
    judge_model: str,
    scorer_model: str,
    budget: BudgetTracker,
    top_pct: int,
    max_comment_chars: int,
) -> None:
    template = _JUDGE_PROMPT.read_text(encoding="utf-8")
    now_ms = int(time.time() * 1000)

    n_top = max(1, math.ceil(len(scored) * top_pct / 100))
    top = scored[:n_top]
    start = time.monotonic()
    print(f"  Judging top {n_top} PRs (top {top_pct}%)...")

    for i, pr in enumerate(top, 1):
        if not budget.ok("judge"):
            _live_done(f"Judge budget reached ({budget.judge_used:,} tokens). Stopping.")
            break
        repo = f"{pr['project_key']}/{pr['slug']}"
        pr_ref = f"{repo}#{pr['pr_id']}"
        _live(f"[{i}/{len(top)}]  {pr_ref:<35}  score={pr['total_score']:.2f}  judging...")
        _judge_pr(conn, pr, judge, judge_model, template, scorer_model, budget, max_comment_chars, now_ms)
        elapsed = time.monotonic() - start
        eta = elapsed / i * (len(top) - i) if i < len(top) else 0
        verdict = pr.get("verdict", "?")
        reasoning = (pr.get("verdict_reasoning") or "")[:60]
        _live_done(
            f"[{i}/{len(top)}]  {pr_ref:<35}  score={pr['total_score']:.2f}  → {verdict}"
            f"  \"{reasoning}\""
            f"  [{int(elapsed)}s, ~{int(eta)}s left  {budget.judge_used:,}tok]"
        )


# ── Report ──────────────────────────────────────────────────────────────────

def _print_table(scored: list[dict]) -> None:
    try:
        from tabulate import tabulate
    except ImportError:
        tabulate = None

    headers = ["#", "PR", "Score", "Types", "Depth", "Comments", "Verdict"]
    rows = []
    for i, pr in enumerate(scored, 1):
        repo = f"{pr['project_key']}/{pr['slug']}"
        verdict = pr.get("verdict") or "-"
        rows.append([
            i,
            f"{repo}#{pr['pr_id']}",
            f"{pr['total_score']:.2f}",
            f"{pr['unique_types']} ({','.join(pr['types'][:3])}{'...' if len(pr['types']) > 3 else ''})",
            f"{pr['avg_depth']:.1f}",
            pr["root_comment_count"],
            verdict,
        ])
    if tabulate:
        print(tabulate(rows, headers=headers, tablefmt="simple"))
    else:
        print("\t".join(headers))
        for r in rows:
            print("\t".join(str(x) for x in r))


def _pr_url(bb_url: str | None, project_key: str, slug: str, pr_id: int) -> str | None:
    if not bb_url:
        return None
    return f"{bb_url.rstrip('/')}/projects/{project_key}/repos/{slug}/pull-requests/{pr_id}"


def _generate_html_report(
    scored: list[dict],
    heuristic_count: int,
    total_in_range: int,
    output_path: Path,
    budget: BudgetTracker,
    steps_run: list[str],
    bb_url: str | None = None,
) -> None:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    gold   = [p for p in scored if p.get("verdict") == "GOLD"]
    silver = [p for p in scored if p.get("verdict") == "SILVER"]
    reject = [p for p in scored if p.get("verdict") == "REJECT"]
    unj    = [p for p in scored if not p.get("verdict")]

    # ── Scatter chart data ─────────────────────────────────────────────────
    scatter_groups = {
        "GOLD":    (gold,   "#f4b942", "circle"),
        "SILVER":  (silver, "#a0a0a0", "circle"),
        "REJECT":  (reject, "#e05252", "x"),
        "—":       (unj,    "#4a9edd", "circle-open"),
    }
    scatter_traces = []
    for label, (group, color, symbol) in scatter_groups.items():
        if not group:
            continue
        scatter_traces.append({
            "type": "scatter", "mode": "markers", "name": label,
            "x": [p["unique_types"] for p in group],
            "y": [p["avg_depth"] for p in group],
            "marker": {
                "size": [max(8, p["root_comment_count"] * 2) for p in group],
                "color": color, "symbol": symbol, "opacity": 0.8,
                "line": {"width": 1, "color": "#333"},
            },
            "text": [
                f"{p['project_key']}/{p['slug']}#{p['pr_id']}<br>{(p['title'] or '')[:60]}"
                for p in group
            ],
            "hovertemplate": "%{text}<br>types=%{x}  depth=%{y}<extra>" + label + "</extra>",
        })

    # ── Type distribution chart ────────────────────────────────────────────
    type_counts: dict[str, int] = {}
    for p in scored:
        for t in p.get("types", []):
            type_counts[t] = type_counts.get(t, 0) + 1
    type_sorted = sorted(type_counts.items(), key=lambda x: -x[1])
    type_trace = [{
        "type": "bar",
        "x": [t for t, _ in type_sorted],
        "y": [c for _, c in type_sorted],
        "marker": {"color": "#4a9edd"},
        "name": "Comment types",
    }]

    # ── Funnel data ────────────────────────────────────────────────────────
    funnel_labels = ["In date range", "Passed heuristic", "Scored"]
    funnel_values = [total_in_range, heuristic_count, len(scored)]
    if "judge" in steps_run:
        funnel_labels += ["GOLD", "SILVER"]
        funnel_values += [len(gold), len(silver)]

    # ── PR table rows ──────────────────────────────────────────────────────
    verdict_colors = {"GOLD": "#fffbe6", "SILVER": "#f5f5f5", "REJECT": "#fff0f0"}
    verdict_badge  = {
        "GOLD":   '<span class="badge" style="background:#f4b942;color:#333">GOLD</span>',
        "SILVER": '<span class="badge bg-secondary">SILVER</span>',
        "REJECT": '<span class="badge bg-danger">REJECT</span>',
        None:     '<span class="badge bg-light text-dark">—</span>',
    }
    table_rows_html = ""
    for i, pr in enumerate(scored, 1):
        v = pr.get("verdict")
        bg = verdict_colors.get(v, "#ffffff")
        repo = f"{pr['project_key']}/{pr['slug']}"
        change_str = f"{pr['change_score_ratio']:.0%}" if pr.get("change_score_ratio") is not None else "—"
        reasoning = pr.get("verdict_reasoning") or ""
        types_str = ", ".join(pr.get("types", []))
        url = _pr_url(bb_url, pr["project_key"], pr["slug"], pr["pr_id"])
        pr_link = f'<a href="{url}" target="_blank">{repo}#{pr["pr_id"]}</a>' if url else f"{repo}#{pr['pr_id']}"
        table_rows_html += f"""
        <tr style="background:{bg}">
          <td>{i}</td>
          <td><strong>{pr_link}</strong>
              <br><small class="text-muted">{(pr.get('title') or '')[:70]}</small></td>
          <td><strong>{pr['total_score']:.2f}</strong></td>
          <td><details><summary>{pr['unique_types']} типов</summary>
              <small>{types_str}</small></details></td>
          <td>{pr['avg_depth']:.1f}</td>
          <td>{pr['root_comment_count']}</td>
          <td>{change_str}</td>
          <td>{verdict_badge.get(v, verdict_badge[None])}
              <br><small class="text-muted">{reasoning}</small></td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Golden PR Report</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
</head>
<body class="p-4">
<h2>Golden PR Report</h2>
<p class="text-muted">Generated: {now_str} &nbsp;|&nbsp; Steps: {', '.join(steps_run)}
   &nbsp;|&nbsp; {budget.summary()}</p>

<div class="row mb-4">
  <div class="col"><div class="card text-center"><div class="card-body">
    <h4>{total_in_range}</h4><p class="mb-0 text-muted">In range</p></div></div></div>
  <div class="col"><div class="card text-center"><div class="card-body">
    <h4>{heuristic_count}</h4><p class="mb-0 text-muted">Heuristic passed</p></div></div></div>
  <div class="col"><div class="card text-center"><div class="card-body">
    <h4>{len(scored)}</h4><p class="mb-0 text-muted">Scored</p></div></div></div>
  <div class="col"><div class="card text-center" style="background:#fffbe6"><div class="card-body">
    <h4 style="color:#c89000">{len(gold)}</h4><p class="mb-0 text-muted">GOLD</p></div></div></div>
  <div class="col"><div class="card text-center bg-light"><div class="card-body">
    <h4>{len(silver)}</h4><p class="mb-0 text-muted">SILVER</p></div></div></div>
  <div class="col"><div class="card text-center"><div class="card-body">
    <h4 class="text-danger">{len(reject)}</h4><p class="mb-0 text-muted">REJECT</p></div></div></div>
</div>

<div class="row mb-4">
  <div class="col-8"><div id="scatter" style="height:400px"></div></div>
  <div class="col-4"><div id="funnel"  style="height:400px"></div></div>
</div>
<div class="row mb-4">
  <div class="col-12"><div id="types" style="height:300px"></div></div>
</div>

<h4>Candidates ({len(scored)})</h4>
<table class="table table-sm table-hover">
  <thead class="table-dark"><tr>
    <th>#</th><th>PR</th><th>Score</th><th>Types</th>
    <th>Avg Depth</th><th>Comments</th><th>Accepted</th><th>Verdict</th>
  </tr></thead>
  <tbody>{table_rows_html}</tbody>
</table>

<script>
Plotly.newPlot('scatter',
  {json.dumps(scatter_traces)},
  {{title:'Diversity vs Depth (size = comment count)',
    xaxis:{{title:'Unique comment types'}},
    yaxis:{{title:'Average depth'}},
    hovermode:'closest'}},
  {{responsive:true}}
);
Plotly.newPlot('funnel',
  [{{type:'funnel',
    y:{json.dumps(funnel_labels)},
    x:{json.dumps(funnel_values)},
    textinfo:'value+percent initial',
    marker:{{color:['#4a9edd','#5bc0de','#f4b942','#ffd700','#a0a0a0']}}
  }}],
  {{title:'PR selection funnel', margin:{{l:150}}}},
  {{responsive:true}}
);
Plotly.newPlot('types',
  {json.dumps(type_trace)},
  {{title:'Comment type distribution (all scored PRs)',
    xaxis:{{tickangle:-30}}}},
  {{responsive:true}}
);
</script>
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"Report saved to {output_path}")


# ── Main command ────────────────────────────────────────────────────────────

def cmd_select_golden(args: argparse.Namespace, cfg: dict) -> None:
    db_path   = resolve_db(getattr(args, "db", None), cfg)
    bb_url    = resolve_url(None, cfg)
    since_ts  = date_to_ms(args.since) if args.since else None
    until_ts  = date_to_ms(args.until, end_of_day=True) if args.until else None
    steps     = [s.strip() for s in args.steps.split(",")]
    output    = Path(getattr(args, "output", "output/golden.html"))
    top_pct   = getattr(args, "top_pct", 20)
    max_chars = getattr(args, "max_comment_chars", 1500)

    classifier_model = resolve_judge_model(getattr(args, "classifier_model", None), cfg)
    judge_model_arg  = getattr(args, "judge_model", None)
    judge_model      = resolve_judge_model(judge_model_arg, cfg)
    # scorer_model identifies the full pipeline run in pr_scores
    scorer_model = classifier_model

    # judge_model for change_score lookup (from analyze-feedback, optional)
    change_judge_model = getattr(args, "change_judge_model", None)

    budget = BudgetTracker(
        total_limit    = getattr(args, "budget_tokens", None),
        classify_limit = getattr(args, "budget_classify", None),
        analyze_limit  = getattr(args, "budget_analyze", None),
        judge_limit    = getattr(args, "budget_judge", None),
    )

    conn = open_db(db_path)

    # ── Resolve repos ──────────────────────────────────────────────────────
    repos = collect_repos_from_args(args, conn)
    if not repos:
        log.error("No repositories specified.")
        conn.close()
        sys.exit(1)

    repo_ids: list[int] = []
    for proj_key, slug in repos:
        row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, slug)
        ).fetchone()
        if row:
            repo_ids.append(row["id"])
    if not repo_ids:
        log.error("No matching repos found in cache.")
        conn.close()
        sys.exit(4)

    # Count total PRs in range for funnel
    ph = ",".join("?" * len(repo_ids))
    count_params: list[Any] = list(repo_ids)
    count_q = f"""
        SELECT COUNT(*) FROM pull_requests
        WHERE repo_id IN ({ph}) AND state IN ('MERGED','DECLINED') AND closed_date IS NOT NULL
    """
    if since_ts:
        count_q += " AND created_date >= ?"
        count_params.append(since_ts)
    if until_ts:
        count_q += " AND created_date <= ?"
        count_params.append(until_ts)
    total_in_range = conn.execute(count_q, count_params).fetchone()[0]

    # ── Phase 1: Heuristic ─────────────────────────────────────────────────
    print(f"\nPhase 1: heuristic filter  ({total_in_range} PRs in range)")
    gcfg = cfg.get("golden", {})
    candidates = _heuristic_filter(
        conn, repo_ids, since_ts, until_ts,
        min_lifetime_h = getattr(args, "min_lifetime_h", None) or gcfg.get("min_lifetime_h", 0.25),
        max_lifetime_h = getattr(args, "max_lifetime_h", None) or gcfg.get("max_lifetime_h", 120),
        min_reviewers  = getattr(args, "min_reviewers", None) or gcfg.get("min_reviewers", 1),
        min_comments   = getattr(args, "min_comments", None) or gcfg.get("min_comments", 2),
        max_comments   = getattr(args, "max_comments", None) or gcfg.get("max_comments", 30),
    )
    print(f"  → {len(candidates)} candidates passed heuristic")

    if "heuristic" in steps and len(steps) == 1:
        _print_table([{**c, "unique_types": 0, "types": [], "avg_depth": 0,
                       "total_score": 0, "root_comment_count": c.get("root_comment_count", 0)}
                      for c in candidates])
        conn.close()
        return

    if not candidates:
        print("No candidates after heuristic filter. Adjust thresholds or expand date range.")
        conn.close()
        return

    # ── Phase 2: Classify ──────────────────────────────────────────────────
    needs_llm = any(s in steps for s in ("classify", "analyze", "judge"))
    judge = None
    if needs_llm:
        api_key  = resolve_judge_api_key(cfg)
        base_url = resolve_judge_base_url(cfg)
        if not api_key:
            log.error("No API key for LLM judge. Set ANTHROPIC_API_KEY or DEEPSEEK_API_KEY.")
            conn.close()
            sys.exit(1)
        judge = LLMJudge(model=classifier_model, api_key=api_key, base_url=base_url)

    if "classify" in steps:
        print(f"\nPhase 2a: classify comments  (model={classifier_model})")
        _run_classify_step(conn, candidates, judge, classifier_model, budget, max_chars)

    # ── Phase 2b: Analyze (acceptance verdicts) ───────────────────────────
    # Use change_judge_model for analysis; defaults to classifier_model
    analyze_model = change_judge_model or classifier_model
    if "analyze" in steps:
        # If analyze_model differs from classifier, create a new judge instance
        analyze_judge = judge
        if analyze_model != classifier_model:
            analyze_judge = LLMJudge(model=analyze_model, api_key=api_key, base_url=base_url)
        print(f"\nPhase 2b: analyze unanalyzed comments  (model={analyze_model})")
        _run_analyze_step(conn, candidates, analyze_judge, analyze_model, budget, max_chars)
        # Set change_judge_model so score step picks up the results
        if not change_judge_model:
            change_judge_model = analyze_model

    # ── Phase 3: Score ─────────────────────────────────────────────────────
    scored: list[dict] = []
    if "score" in steps or "judge" in steps:
        print("\nPhase 3: compute PR scores")
        now_ms = int(time.time() * 1000)
        scored = _run_score_step(conn, candidates, classifier_model, change_judge_model, scorer_model, now_ms)
        if not scored:
            print("  No PRs scored — run 'classify' step first.")
        else:
            print(f"  → {len(scored)} PRs scored")
            print(f"  Score range: {scored[-1]['total_score']:.2f} – {scored[0]['total_score']:.2f}")

    # ── Phase 4: Final judge ───────────────────────────────────────────────
    if "judge" in steps and scored:
        jm = resolve_judge_model(judge_model_arg, cfg)
        if judge is None or jm != classifier_model:
            api_key  = resolve_judge_api_key(cfg)
            base_url = resolve_judge_base_url(cfg)
            judge = LLMJudge(model=jm, api_key=api_key, base_url=base_url)
        print(f"\nPhase 4: final judge  (model={jm})")
        _run_judge_step(conn, scored, judge, jm, scorer_model, budget, top_pct, max_chars)

    conn.close()

    # ── Terminal summary ───────────────────────────────────────────────────
    if scored:
        gold   = sum(1 for p in scored if p.get("verdict") == "GOLD")
        silver = sum(1 for p in scored if p.get("verdict") == "SILVER")
        reject = sum(1 for p in scored if p.get("verdict") == "REJECT")
        print(f"\n{'─'*60}")
        print(f"Results: {len(scored)} scored  GOLD={gold}  SILVER={silver}  REJECT={reject}")
        print(f"Tokens:  {budget.summary()}")
        print()
        _print_table(scored[:50])

    # ── HTML report ────────────────────────────────────────────────────────
    if scored or candidates:
        _generate_html_report(
            scored or [],
            heuristic_count=len(candidates),
            total_in_range=total_in_range,
            output_path=output,
            budget=budget,
            steps_run=steps,
            bb_url=bb_url,
        )
