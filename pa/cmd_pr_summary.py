"""pr-summary — markdown digest of one-or-many PRs for LLM hand-off.

For each selected PR, prints (in markdown):
  - title, URL, state, timing
  - bot inline suggestions with severity / verdicts / LLM reasoning
  - human ROOT inline comments (what the bot missed)

Multi-PR selection:
  --pr PROJ/repo#ID                      single PR
  --since X --projects P [--state S]     by date range
  --sort latest|worst-merge|worst-coverage|late
  --limit N

Output is markdown so the result can be pasted into an LLM for
hypothesis generation, while URLs stay clickable. Works against the
local cache only — no network.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pa.config import resolve_db, resolve_url
from pa.db import open_db
from pa.utils import collect_repos_from_args, date_to_ms


def _ts(ms: int | None) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _delta(a: int | None, b: int | None) -> str:
    if a is None or b is None:
        return ""
    d = (a - b) / 60000  # minutes
    sign = "+" if d >= 0 else "-"
    d = abs(d)
    if d < 60:
        return f"{sign}{d:.0f}m"
    if d < 60 * 24:
        return f"{sign}{d / 60:.1f}h"
    return f"{sign}{d / 60 / 24:.1f}d"


def _select_prs(conn, args, since_ts, until_ts, repo_ids) -> list:
    bot = args.bot
    sort_mode = args.sort
    limit = args.limit

    where = ["pr.state = ?"]
    params: list = [args.state]
    if since_ts:
        where.append("pr.created_date >= ?")
        params.append(since_ts)
    if until_ts:
        where.append("pr.created_date <= ?")
        params.append(until_ts)
    if repo_ids:
        placeholders = ",".join("?" * len(repo_ids))
        where.append(f"pr.repo_id IN ({placeholders})")
        params.extend(repo_ids)

    # Default: only PRs where the bot actually commented (otherwise summary
    # is empty — purpose of the tool is bot-attended PRs).
    where.append(
        "EXISTS (SELECT 1 FROM pr_comments c "
        "WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id AND c.author=?)"
    )
    params.append(bot)

    base_select = f"""
        SELECT pr.repo_id, pr.pr_id, pr.title, pr.state, pr.author AS pr_author,
               pr.created_date AS pr_open, pr.closed_date AS pr_close,
               r.project_key, r.slug,
               (SELECT COUNT(*) FROM pr_comments c
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author=? AND c.parent_id IS NULL
                    AND c.file_path IS NOT NULL) AS bot_inline,
               (SELECT COUNT(*) FROM pr_comments c
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author!=? AND c.parent_id IS NULL
                    AND c.file_path IS NOT NULL) AS human_inline,
               (SELECT MIN(c.created_date) FROM pr_comments c
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author=?) AS bot_first,
               (SELECT MAX(c.created_date) FROM pr_comments c
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author=?) AS bot_last,
               (SELECT COUNT(*) FROM pr_comments c
                  JOIN merge_analysis ma ON ma.comment_id=c.id
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author=? AND ma.verdict='YES') AS n_yes,
               (SELECT COUNT(*) FROM pr_comments c
                  JOIN merge_analysis ma ON ma.comment_id=c.id
                  WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                    AND c.author=? AND ma.verdict='NO') AS n_no
        FROM pull_requests pr
        JOIN repos r ON r.id=pr.repo_id
        WHERE {" AND ".join(where)}
    """
    # Repeat the bot-author parameter for each subquery binding (6 times).
    select_params = [bot] * 6 + params

    if sort_mode == "latest":
        order = "ORDER BY pr.created_date DESC"
    elif sort_mode == "worst-merge":
        # Most NO verdicts, ties broken by inline volume (more chances missed)
        order = "ORDER BY n_no DESC, bot_inline DESC, pr.created_date DESC"
    elif sort_mode == "worst-coverage":
        # Highest human/bot inline ratio (bot missed the most)
        order = "ORDER BY (human_inline - bot_inline) DESC, pr.created_date DESC"
    elif sort_mode == "late":
        # Where the bot's last comment was after PR close
        order = "ORDER BY (bot_last - pr_close) DESC, pr.created_date DESC"
    else:
        order = "ORDER BY pr.created_date DESC"

    sql = base_select + " " + order
    if limit and limit > 0:
        sql += f" LIMIT {limit}"
    return conn.execute(sql, select_params).fetchall()


def _render_pr(conn, pr_row, bot, bb_url) -> str:
    proj = pr_row["project_key"]
    slug = pr_row["slug"]
    pr_id = pr_row["pr_id"]
    repo = f"{proj}/{slug}"
    url = ""
    if bb_url:
        url = f"{bb_url}/projects/{proj}/repos/{slug}/pull-requests/{pr_id}/overview"

    lines: list[str] = []
    title = pr_row["title"] or ""
    lines.append(f"## {repo}#{pr_id} — {title}")
    lines.append("")
    if url:
        lines.append(f"- URL: {url}")
    lines.append(f"- State: **{pr_row['state']}**  author: `{pr_row['pr_author']}`")

    pr_open = pr_row["pr_open"]
    pr_close = pr_row["pr_close"]
    lifetime = _delta(pr_close, pr_open) if pr_open and pr_close else ""
    lines.append(f"- Open: {_ts(pr_open)}  →  Close: {_ts(pr_close)}  (lifetime: {lifetime})")

    bot_first = pr_row["bot_first"]
    bot_last = pr_row["bot_last"]
    if bot_first:
        first_offset = _delta(bot_first, pr_open)
        last_vs_close = _delta(bot_last, pr_close)
        lines.append(
            f"- Bot timing: first comment {first_offset} after open, "
            f"last {last_vs_close} vs close"
        )
        if pr_close and bot_last and bot_last > pr_close:
            lines.append(f"  - ⚠ Bot's last comment posted **after PR was closed**")

    n_inline_bot = pr_row["bot_inline"]
    n_inline_human = pr_row["human_inline"]
    n_yes = pr_row["n_yes"]
    n_no = pr_row["n_no"]
    lines.append(
        f"- Coverage: bot_inline=**{n_inline_bot}**, human_inline=**{n_inline_human}**, "
        f"merge_verdicts: YES={n_yes} NO={n_no}"
    )
    if n_inline_human > n_inline_bot:
        lines.append(
            f"  - ⚠ Humans posted **more inline comments than the bot** "
            f"(+{n_inline_human - n_inline_bot}) — possible coverage gap"
        )
    lines.append("")

    # Bot inline suggestions
    bot_comments = conn.execute(
        """SELECT c.id, c.text, c.severity, c.file_path, c.line, c.created_date,
                  c.dg_gen, c.dg_hash,
                  (SELECT verdict FROM comment_analysis ca WHERE ca.comment_id=c.id LIMIT 1) AS fb,
                  (SELECT verdict FROM merge_analysis ma WHERE ma.comment_id=c.id LIMIT 1) AS mg,
                  (SELECT reasoning FROM merge_analysis ma WHERE ma.comment_id=c.id LIMIT 1) AS mg_reason
           FROM pr_comments c
           WHERE c.repo_id=? AND c.pr_id=? AND c.author=? AND c.parent_id IS NULL
             AND c.file_path IS NOT NULL
           ORDER BY c.created_date""",
        (pr_row["repo_id"], pr_id, bot),
    ).fetchall()

    if bot_comments:
        lines.append(f"### Bot inline suggestions ({len(bot_comments)})")
        lines.append("")
        for c in bot_comments:
            sev = c["severity"] or "NORMAL"
            tag = f"  dg={c['dg_hash']}" if c["dg_hash"] else ""
            fb = c["fb"] or "-"
            mg = c["mg"] or "-"
            lines.append(
                f"#### `{c['file_path']}:{c['line']}`  severity={sev}  fb={fb}  mg={mg}{tag}"
            )
            if c["mg_reason"]:
                lines.append(f"_mg-reason:_ {c['mg_reason']}")
            lines.append("")
            for tl in (c["text"] or "").splitlines():
                lines.append(f"> {tl}")
            lines.append("")
            if bb_url:
                lines.append(
                    f"_(comment "
                    f"[#{c['id']}]({bb_url}/projects/{proj}/repos/{slug}"
                    f"/pull-requests/{pr_id}/overview?commentId={c['id']}))_"
                )
                lines.append("")

    # Human ROOT inline comments
    human_comments = conn.execute(
        """SELECT c.id, c.author, c.text, c.severity, c.file_path, c.line, c.created_date
           FROM pr_comments c
           WHERE c.repo_id=? AND c.pr_id=? AND c.author!=? AND c.parent_id IS NULL
             AND c.file_path IS NOT NULL
           ORDER BY c.created_date""",
        (pr_row["repo_id"], pr_id, bot),
    ).fetchall()

    if human_comments:
        lines.append(f"### Human ROOT inline comments ({len(human_comments)})")
        lines.append("")
        for c in human_comments:
            lines.append(
                f"#### `{c['file_path']}:{c['line']}` by `{c['author']}`  "
                f"_{_ts(c['created_date'])}_"
            )
            for tl in (c["text"] or "").splitlines()[:6]:
                lines.append(f"> {tl}")
            lines.append("")

    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def cmd_pr_summary(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    bb_url = (resolve_url(None, cfg) or "").rstrip("/")

    if args.pr:
        # Single-PR shortcut
        if "/" not in args.pr or "#" not in args.pr:
            print("Use --pr PROJ/repo#PR_ID  (e.g. PCCFT/sql-gbd#261)", file=sys.stderr)
            sys.exit(1)
        proj_repo, pr_id_str = args.pr.split("#", 1)
        proj, repo_slug = proj_repo.split("/", 1)
        pr = conn.execute(
            """SELECT pr.repo_id, pr.pr_id, pr.title, pr.state, pr.author AS pr_author,
                      pr.created_date AS pr_open, pr.closed_date AS pr_close,
                      r.project_key, r.slug,
                      (SELECT COUNT(*) FROM pr_comments c WHERE c.repo_id=pr.repo_id
                         AND c.pr_id=pr.pr_id AND c.author=? AND c.parent_id IS NULL
                         AND c.file_path IS NOT NULL) AS bot_inline,
                      (SELECT COUNT(*) FROM pr_comments c WHERE c.repo_id=pr.repo_id
                         AND c.pr_id=pr.pr_id AND c.author!=? AND c.parent_id IS NULL
                         AND c.file_path IS NOT NULL) AS human_inline,
                      (SELECT MIN(c.created_date) FROM pr_comments c WHERE c.repo_id=pr.repo_id
                         AND c.pr_id=pr.pr_id AND c.author=?) AS bot_first,
                      (SELECT MAX(c.created_date) FROM pr_comments c WHERE c.repo_id=pr.repo_id
                         AND c.pr_id=pr.pr_id AND c.author=?) AS bot_last,
                      (SELECT COUNT(*) FROM pr_comments c
                         JOIN merge_analysis ma ON ma.comment_id=c.id
                         WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                           AND c.author=? AND ma.verdict='YES') AS n_yes,
                      (SELECT COUNT(*) FROM pr_comments c
                         JOIN merge_analysis ma ON ma.comment_id=c.id
                         WHERE c.repo_id=pr.repo_id AND c.pr_id=pr.pr_id
                           AND c.author=? AND ma.verdict='NO') AS n_no
               FROM pull_requests pr JOIN repos r ON r.id=pr.repo_id
               WHERE r.project_key=? AND r.slug=? AND pr.pr_id=?""",
            (args.bot, args.bot, args.bot, args.bot, args.bot, args.bot,
             proj, repo_slug, int(pr_id_str)),
        ).fetchone()
        if not pr:
            print(f"PR {args.pr} not found in cache.", file=sys.stderr)
            sys.exit(4)
        prs = [pr]
    else:
        since_ts = date_to_ms(args.since) if args.since else None
        until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
        repos = collect_repos_from_args(args, conn)
        repo_ids: list[int] | None = None
        if repos:
            repo_ids = []
            for proj_key, repo_slug in repos:
                row = conn.execute(
                    "SELECT id FROM repos WHERE project_key=? AND slug=?",
                    (proj_key, repo_slug),
                ).fetchone()
                if row:
                    repo_ids.append(row["id"])
        prs = _select_prs(conn, args, since_ts, until_ts, repo_ids)

    if not prs:
        print("No PRs match.", file=sys.stderr)
        conn.close()
        return

    chunks: list[str] = []
    chunks.append(f"# PR summary — {len(prs)} PR(s), bot=`{args.bot}`, sort=`{args.sort}`")
    chunks.append("")
    for pr in prs:
        chunks.append(_render_pr(conn, pr, args.bot, bb_url))

    output_text = "\n".join(chunks)
    output_path = getattr(args, "output", None)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(output_text)
        print(f"{len(prs)} PR(s) written to {output_path}", file=sys.stderr)
    else:
        print(output_text)

    conn.close()
