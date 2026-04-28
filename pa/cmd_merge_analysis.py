"""
analyze-merges command: check if agent comments led to actual code changes.

For each root comment with a file anchor (file_path != NULL), fetches
the PR diff for that file from Bitbucket and asks an LLM judge whether
the comment was addressed in the final changes.

Results stored in merge_analysis table. Idempotent — skips already analyzed.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time
from pathlib import Path
from pa.api import api_get, make_session, paginate
from pa.config import (
    resolve_db, resolve_judge_api_key, resolve_judge_base_url,
    resolve_judge_model, resolve_token, resolve_url,
)
from pa.db import open_db
from pa.judge import LLMJudge
from pa.utils import collect_repos_from_args, date_to_ms, ms_to_date

log = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / "prompts" / "judge_merge_acceptance.txt"

# In-memory caches
_diff_cache: dict[tuple, tuple[str, str | None] | None] = {}  # key → (diff_text, toHash) | None
_snippet_cache: dict[tuple, str | None] = {}  # (proj, repo, file, hash, line) → snippet


def _bb_diff_to_text(data: dict) -> str:
    """Convert Bitbucket Server diff JSON to unified diff text with line numbers."""
    lines = []
    for diff in data.get("diffs", []):
        src = (diff.get("source") or {}).get("toString", "/dev/null")
        dst = (diff.get("destination") or {}).get("toString", "/dev/null")
        lines.append(f"--- {src}")
        lines.append(f"+++ {dst}")
        for hunk in diff.get("hunks", []):
            sh = hunk.get("sourceLine", 0)
            ss = hunk.get("sourceSpan", 0)
            dh = hunk.get("destinationLine", 0)
            ds = hunk.get("destinationSpan", 0)
            lines.append(f"@@ -{sh},{ss} +{dh},{ds} @@")
            src_n, dst_n = sh, dh
            for seg in hunk.get("segments", []):
                stype = seg["type"]
                for line in seg.get("lines", []):
                    text = line["line"]
                    if stype == "REMOVED":
                        lines.append(f"-{src_n:>4}      | {text}")
                        src_n += 1
                    elif stype == "ADDED":
                        lines.append(f"+     {dst_n:>4} | {text}")
                        dst_n += 1
                    else:  # CONTEXT
                        lines.append(f" {src_n:>4} {dst_n:>4} | {text}")
                        src_n += 1
                        dst_n += 1
    return "\n".join(lines)


def _fetch_diff(
    session, bb_url: str,
    project_key: str, repo_slug: str, pr_id: int,
    file_path: str,
    repo_id: int,
) -> tuple[str, str | None] | None:
    """Fetch diff for a file in a PR. Returns (diff_text, toHash) or None.
    Caches per (repo_id, pr_id, file_path). toHash is the source commit SHA."""
    key = (repo_id, pr_id, file_path)
    if key in _diff_cache:
        return _diff_cache[key]
    url = (
        f"{bb_url}/rest/api/1.0/projects/{project_key}/repos/{repo_slug}"
        f"/pull-requests/{pr_id}/diff/{file_path}?contextLines=5"
    )
    try:
        data = api_get(session, url, allow_404=True)
        if not data or not data.get("diffs"):
            _diff_cache[key] = None
            return None
        text = _bb_diff_to_text(data)
        to_hash = data.get("toHash")
        result = (text, to_hash) if text.strip() else None
        _diff_cache[key] = result
        return result
    except SystemExit:
        raise
    except Exception as exc:
        log.debug("No diff for %s in %s/%s#%d: %s", file_path, project_key, repo_slug, pr_id, exc)
        _diff_cache[key] = None
        return None


def _fetch_source_snippet(
    session, bb_url: str,
    project_key: str, repo_slug: str,
    file_path: str, to_hash: str,
    anchor_line: int,
    context: int = 10,
) -> str | None:
    """Fetch source file at toHash (PR source commit) and return ±context lines around anchor.
    Works even for merged PRs with deleted branches — toHash is a commit SHA."""
    key = (project_key, repo_slug, file_path, to_hash, anchor_line)
    if key in _snippet_cache:
        return _snippet_cache[key]

    start_line = max(0, anchor_line - context - 1)  # API is 0-based
    limit = context * 2 + 1
    url = (
        f"{bb_url}/rest/api/1.0/projects/{project_key}/repos/{repo_slug}"
        f"/browse/{file_path}?at={to_hash}&start={start_line}&limit={limit}"
    )
    try:
        data = api_get(session, url, allow_404=True)
        if not data or not data.get("lines"):
            _snippet_cache[key] = None
            return None
        lines = []
        line_num = data.get("start", start_line) + 1  # API start is 0-based
        for entry in data["lines"]:
            text = entry.get("text", "")
            marker = " >>>" if line_num == anchor_line else "    "
            lines.append(f"{line_num:>4}{marker} | {text}")
            line_num += 1
        snippet = "\n".join(lines)
        _snippet_cache[key] = snippet
        return snippet
    except SystemExit:
        raise
    except Exception as exc:
        log.debug("Cannot fetch source snippet %s@%s:%d: %s", file_path, to_hash[:8], anchor_line, exc)
        _snippet_cache[key] = None
        return None


# Cache: (repo_id, pr_id) → list of {hash, message, timestamp, files: [path, ...], node_type: ...}
_commits_cache: dict[tuple, list[dict]] = {}


def _fetch_pr_commits(
    session, bb_url: str,
    project_key: str, repo_slug: str, pr_id: int,
    repo_id: int,
) -> list[dict]:
    """Fetch commits for a PR with changed files per commit. Cached per (repo_id, pr_id)."""
    key = (repo_id, pr_id)
    if key in _commits_cache:
        return _commits_cache[key]

    url = (
        f"{bb_url}/rest/api/1.0/projects/{project_key}/repos/{repo_slug}"
        f"/pull-requests/{pr_id}/commits"
    )
    try:
        raw_commits = paginate(session, url, limit=100)
    except SystemExit:
        raise
    except Exception as exc:
        log.debug("Cannot fetch commits for %s/%s#%d: %s", project_key, repo_slug, pr_id, exc)
        _commits_cache[key] = []
        return []

    result = []
    for c in raw_commits:
        commit_hash = c.get("id", "")[:8]
        message = (c.get("message") or "").split("\n")[0][:80]
        ts = c.get("authorTimestamp", 0)

        # Fetch changed files for this commit
        changes_url = (
            f"{bb_url}/rest/api/1.0/projects/{project_key}/repos/{repo_slug}"
            f"/commits/{c['id']}/changes"
        )
        files = []
        node_types = {}  # path → nodeType (FILE, SUBMODULE, etc.) or changeType (ADD, MODIFY, DELETE, RENAME, COPY)
        try:
            changes = paginate(session, changes_url, limit=100)
            for ch in changes:
                path_obj = ch.get("path") or {}
                path = path_obj.get("toString", "")
                if path:
                    files.append(path)
                    ct = ch.get("type", "MODIFY")  # ADD, MODIFY, DELETE, RENAME, COPY
                    node_types[path] = ct
                # Also track source path for renames
                src_path_obj = ch.get("srcPath") or {}
                src_path = src_path_obj.get("toString", "")
                if src_path and src_path != path:
                    files.append(src_path)
                    node_types[src_path] = "RENAME_SOURCE"
        except Exception:
            pass  # best-effort

        result.append({
            "hash": commit_hash,
            "message": message,
            "timestamp": ts,
            "files": files,
            "change_types": node_types,
        })

    _commits_cache[key] = result
    return result


def _build_commits_context(
    commits: list[dict],
    comment_created_date: int,
    anchor_file: str,
) -> tuple[str, bool]:
    """Build human-readable commit list after comment, return (text, anchor_file_touched).
    Returns (context_string, was_file_touched_after_comment)."""
    after = [c for c in commits if c["timestamp"] > comment_created_date]
    if not after:
        return "Нет коммитов после комментария.", False

    after.sort(key=lambda c: c["timestamp"])
    lines = []
    anchor_touched = False
    for c in after:
        ts_str = ms_to_date(c["timestamp"])
        files_str = ", ".join(c["files"][:10])
        if len(c["files"]) > 10:
            files_str += f", ... (+{len(c['files']) - 10})"
        lines.append(f"  - {c['hash']} ({ts_str}) \"{c['message']}\" — files: {files_str}")

        # Check if anchor file was touched (exact match or rename)
        for f in c["files"]:
            if f == anchor_file:
                ct = c["change_types"].get(f, "MODIFY")
                if ct == "DELETE":
                    lines.append(f"    ⚠ Файл {anchor_file} УДАЛЁН в этом коммите")
                elif ct == "RENAME_SOURCE":
                    lines.append(f"    ⚠ Файл {anchor_file} ПЕРЕИМЕНОВАН (старое имя)")
                anchor_touched = True

    text = "\n".join(lines)
    if not anchor_touched:
        text += f"\n  ⚠ Файл {anchor_file} НЕ фигурирует ни в одном коммите после комментария."

    return text, anchor_touched


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
    verbose = getattr(args, "verbose", False)
    force = getattr(args, "force", False)

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
    # Compute analyzer_version early so we can use it in the query
    prompt_content = _PROMPT_PATH.read_text(encoding="utf-8")
    analyzer_version = hashlib.sha256(prompt_content.encode()).hexdigest()[:8]

    q = """
        SELECT
            c.id          AS comment_id,
            c.repo_id,
            c.pr_id,
            c.text        AS comment_text,
            c.file_path,
            c.line,
            c.severity,
            c.created_date AS comment_created_date,
            pr.title      AS pr_title,
            pr.closed_date,
            pr.state      AS pr_state,
            r.project_key,
            r.slug
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
        WHERE c.author = ?
          AND c.parent_id IS NULL
          AND c.file_path IS NOT NULL
          AND pr.state = 'MERGED'
          AND pr.closed_date IS NOT NULL
    """
    params: list = [author]
    if not force:
        q += """
          AND NOT EXISTS (
              SELECT 1 FROM merge_analysis ma
              WHERE ma.comment_id = c.id AND ma.judge_model = ?
                AND ma.analyzer_version = ?
          )
        """
        params.extend([judge_model, analyzer_version])

    if since_ts:
        q += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        q += " AND pr.created_date <= ?"
        params.append(until_ts)
    if repo_ids:
        q += f" AND c.repo_id IN ({','.join('?' * len(repo_ids))})"
        params.extend(repo_ids)
    only_comment_id = getattr(args, "comment_id", None)
    if only_comment_id is not None:
        q += " AND c.id = ?"
        params.append(only_comment_id)

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
        f"Judge model: {judge_model}  analyzer_version: {analyzer_version}\n"
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
    prompt_template = prompt_content  # already loaded above for version hash
    from pa.judge import build_judge
    judge = build_judge(judge_model, api_key, base_url, cfg)
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
        diff_result = _fetch_diff(
            session, bb_url,
            row["project_key"], row["slug"], row["pr_id"],
            row["file_path"], row["repo_id"],
        )
        if not diff_result:
            n_skip += 1
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}"
                f"  SKIP (no diff)",
                flush=True,
            )
            continue

        diff_text, to_hash = diff_result

        # Fetch source snippet around anchor line (works for merged PRs)
        anchor_line = row["line"]
        source_snippet = ""
        if to_hash and anchor_line:
            snippet = _fetch_source_snippet(
                session, bb_url,
                row["project_key"], row["slug"],
                row["file_path"], to_hash,
                anchor_line,
            )
            if snippet:
                source_snippet = snippet

        # Fetch PR commits and build post-comment context
        commits = _fetch_pr_commits(
            session, bb_url,
            row["project_key"], row["slug"], row["pr_id"],
            row["repo_id"],
        )
        comment_ts = row["comment_created_date"] or 0
        commits_context, anchor_touched = _build_commits_context(
            commits, comment_ts, row["file_path"],
        )

        # Fast path: no commits after comment → guaranteed NO, skip LLM call
        after_count = sum(1 for c in commits if c["timestamp"] > comment_ts)
        if after_count == 0:
            verdict, confidence, reasoning = "NO", 1.0, "Нет коммитов после комментария"
            conn.execute(
                """INSERT OR REPLACE INTO merge_analysis
                   (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, now_ms),
            )
            conn.commit()
            n_no += 1
            elapsed = time.monotonic() - start
            eta = elapsed / i * (total - i)
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}:{anchor_line}"
                f"  → NO (1.0) \"no commits after comment\" [skip LLM]"
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {total_tokens:,}tok]",
                flush=True,
            )
            continue

        # Fast path: commits exist but anchor file not touched → guaranteed NO
        if not anchor_touched:
            verdict, confidence, reasoning = "NO", 1.0, f"Файл не менялся после комментария (коммитов: {after_count})"
            conn.execute(
                """INSERT OR REPLACE INTO merge_analysis
                   (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, now_ms),
            )
            conn.commit()
            n_no += 1
            elapsed = time.monotonic() - start
            eta = elapsed / i * (total - i)
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}:{anchor_line}"
                f"  → NO (1.0) \"file not in post-comment commits\" [skip LLM]"
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {total_tokens:,}tok]",
                flush=True,
            )
            continue

        diff_truncated = _truncate_diff(diff_text, max_diff_chars)
        comment_text = (row["comment_text"] or "")[:max_comment_chars]

        prompt = prompt_template.format(
            pr_title=row["pr_title"] or "",
            repo=repo,
            file_path=row["file_path"],
            line=anchor_line or "?",
            comment_text=comment_text,
            source_snippet=source_snippet or "(недоступен)",
            commits_after=commits_context,
            diff_content=diff_truncated,
        )

        if verbose:
            print(f"\n{'═' * 80}")
            print(f"[{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}:{anchor_line}")
            print(f"{'─' * 80}")
            print(prompt)
            print(f"{'─' * 80}")

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
                   (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, now_ms),
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

            if verbose:
                print(f"RESPONSE: {data}")
                print(f"{'═' * 80}")
            print(
                f"  [{i}/{total}]  {repo}#{row['pr_id']} {row['file_path']}:{anchor_line}"
                f"  → {verdict} ({confidence:.1f}) \"{reasoning}\""
                f"  [{int(elapsed)}s, ~{int(eta)}s left  {total_tokens:,}tok]",
                flush=True,
            )

        except Exception as exc:
            n_error += 1
            log.warning("Failed to analyze comment %d: %s", comment_id, exc)
            print(f"  [{i}/{total}]  comment#{comment_id}  ERROR: {exc}", flush=True)

    # ── full summary (including previously cached results) ─────────────────
    summary_q = """
        SELECT ma.verdict, COUNT(*) AS cnt
        FROM merge_analysis ma
        JOIN pr_comments c ON c.id = ma.comment_id
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        WHERE c.author = ? AND ma.judge_model = ?
          AND c.parent_id IS NULL AND c.file_path IS NOT NULL
          AND pr.state = 'MERGED' AND pr.closed_date IS NOT NULL
          AND ma.analyzed_at = (
              SELECT MAX(ma2.analyzed_at) FROM merge_analysis ma2
              WHERE ma2.comment_id = ma.comment_id AND ma2.judge_model = ma.judge_model
          )
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
    summary_q += " GROUP BY ma.verdict"

    all_verdicts = {r["verdict"]: r["cnt"] for r in conn.execute(summary_q, summary_params).fetchall()}
    all_yes = all_verdicts.get("YES", 0)
    all_partial = all_verdicts.get("PARTIAL", 0)
    all_no = all_verdicts.get("NO", 0)
    all_total = all_yes + all_partial + all_no
    from_cache = all_total - (n_yes + n_partial + n_no)
    all_rate = (
        f"{(all_yes + all_partial * 0.5) / all_total * 100:.0f}%"
        if all_total else "n/a"
    )

    conn.close()

    elapsed = time.monotonic() - start
    print(
        f"\nDone in {int(elapsed)}s.  new: YES={n_yes} PARTIAL={n_partial} NO={n_no}"
        f"  skip={n_skip} error={n_error}  tokens={total_tokens:,}"
        f"\n\nTotal (incl. {from_cache} from cache, latest version per comment):  "
        f"YES={all_yes}  PARTIAL={all_partial}  NO={all_no}  "
        f"merge_acceptance={all_rate}  (judge={judge_model})"
    )
