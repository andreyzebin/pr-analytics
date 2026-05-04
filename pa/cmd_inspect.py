"""inspect-comment / pr-timeline — drill-down tools for metric debugging.

Both commands work entirely against the SQLite cache (no network).
Use them when a metric drops on the plot to locate the underlying rows
without hand-writing SQL joins.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import ms_to_date


def _ts(ms: int | None) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _delta(a: int | None, b: int | None) -> str:
    """Human-readable signed delta a-b in minutes/hours/days."""
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


def cmd_inspect_comment(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)
    cid = args.comment_id

    row = conn.execute(
        """SELECT c.*, r.project_key, r.slug,
                  pr.title AS pr_title, pr.state AS pr_state,
                  pr.created_date AS pr_created, pr.closed_date AS pr_closed,
                  pr.author       AS pr_author
           FROM pr_comments c
           JOIN repos r ON r.id = c.repo_id
           JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
           WHERE c.id = ?""",
        (cid,),
    ).fetchone()
    if not row:
        print(f"Comment #{cid} not found in cache.", file=sys.stderr)
        sys.exit(4)

    repo = f"{row['project_key']}/{row['slug']}"
    cmt_ts = row["created_date"]
    pr_open = row["pr_created"]
    pr_close = row["pr_closed"]

    print(f"Comment #{cid}")
    print("=" * 60)
    print(f"  Repo:      {repo}")
    print(f"  PR:        #{row['pr_id']}  state={row['pr_state']}  author={row['pr_author']}")
    print(f"             {(row['pr_title'] or '')[:80]}")
    print(f"  PR open:   {_ts(pr_open)}")
    print(f"  PR close:  {_ts(pr_close)}  ({_delta(pr_close, pr_open)} after open)")
    print(f"  Author:    {row['author']}")
    if row['file_path']:
        print(f"  Anchor:    {row['file_path']}:{row['line']}  severity={row['severity'] or 'NORMAL'}")
    print(f"  Created:   {_ts(cmt_ts)}  ({_delta(cmt_ts, pr_open)} after open, "
          f"{_delta(cmt_ts, pr_close)} vs close)")
    if cmt_ts and pr_close and cmt_ts > pr_close:
        print(f"  ⚠ Comment posted AFTER PR was closed ({_delta(cmt_ts, pr_close)} late)")

    print()
    print("Text:")
    text = row['text'] or ''
    for line in text.splitlines() or ['(empty)']:
        print(f"  {line}")

    # Reactions
    reactions = conn.execute(
        "SELECT author, emoji FROM comment_reactions WHERE comment_id=? ORDER BY author",
        (cid,),
    ).fetchall()
    print()
    if reactions:
        print(f"Reactions ({len(reactions)}):")
        for r in reactions:
            print(f"  {r['emoji']:>10}  by {r['author']}")
    else:
        print("Reactions: (none)")

    # Replies
    replies = conn.execute(
        """SELECT id, author, text, created_date FROM pr_comments
           WHERE parent_id=? ORDER BY created_date""",
        (cid,),
    ).fetchall()
    if replies:
        print(f"\nReplies ({len(replies)}):")
        for r in replies:
            tag = "(self)" if r['author'] == row['author'] else ""
            print(f"  [{_ts(r['created_date'])}]  {r['author']} {tag}  #{r['id']}")
            for line in (r['text'] or '').splitlines()[:5]:
                print(f"    {line}")
            if (r['text'] or '').count('\n') > 5:
                print(f"    ... ({(r['text'] or '').count(chr(10))} lines total)")
    else:
        print("Replies: (none)")

    # Feedback analysis (comment_analysis)
    fa = conn.execute(
        """SELECT judge_model, verdict, confidence, reasoning, analyzed_at
           FROM comment_analysis WHERE comment_id=? ORDER BY analyzed_at DESC""",
        (cid,),
    ).fetchall()
    print()
    if fa:
        print("Feedback analysis (comment_analysis):")
        for r in fa:
            print(f"  [{_ts(r['analyzed_at'])}]  judge={r['judge_model']}")
            print(f"    verdict={r['verdict']}  confidence={r['confidence']}")
            print(f"    reasoning: {(r['reasoning'] or '')[:200]}")
    else:
        print("Feedback analysis: (no entry — run `analyze-feedback`)")

    # Merge analysis
    ma = conn.execute(
        """SELECT judge_model, analyzer_version, verdict, confidence, reasoning, analyzed_at
           FROM merge_analysis WHERE comment_id=? ORDER BY analyzed_at DESC""",
        (cid,),
    ).fetchall()
    print()
    if ma:
        print("Merge analysis (merge_analysis):")
        for r in ma:
            print(f"  [{_ts(r['analyzed_at'])}]  judge={r['judge_model']}  v={r['analyzer_version']}")
            print(f"    verdict={r['verdict']}  confidence={r['confidence']}")
            print(f"    reasoning: {(r['reasoning'] or '')[:200]}")
    elif row['file_path']:
        print("Merge analysis: (no entry — run `analyze-merges`)")
    else:
        print("Merge analysis: (n/a — comment has no file anchor)")

    conn.close()


def cmd_pr_timeline(args: argparse.Namespace, cfg: dict) -> None:
    """Chronological event log for a single PR.
    Shows: open, root comments (with reaction/reply count + analysis verdicts), close.
    Commits are not in the cache — run analyze-merges --comment-id <X> --verbose
    for fresh diff/commit data.
    """
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    if "/" not in args.pr or "#" not in args.pr:
        print("Use --pr PROJ/repo#PR_ID  (e.g. PCCFT/sql-gbd#261)", file=sys.stderr)
        sys.exit(1)
    proj_repo, pr_id_str = args.pr.split("#", 1)
    if "/" not in proj_repo:
        print("Use --pr PROJ/repo#PR_ID  (e.g. PCCFT/sql-gbd#261)", file=sys.stderr)
        sys.exit(1)
    proj, repo_slug = proj_repo.split("/", 1)
    try:
        pr_id = int(pr_id_str)
    except ValueError:
        print(f"Invalid PR id: {pr_id_str!r}", file=sys.stderr)
        sys.exit(1)

    pr = conn.execute(
        """SELECT pr.*, r.project_key, r.slug
           FROM pull_requests pr
           JOIN repos r ON r.id = pr.repo_id
           WHERE r.project_key=? AND r.slug=? AND pr.pr_id=?""",
        (proj, repo_slug, pr_id),
    ).fetchone()
    if not pr:
        print(f"PR {args.pr} not found in cache.", file=sys.stderr)
        sys.exit(4)

    print(f"{proj}/{repo_slug}#{pr_id}  state={pr['state']}  author={pr['author']}")
    print(f"  {(pr['title'] or '')[:100]}")
    print()

    events: list[tuple[int, str, str]] = []  # (ts, kind, text)

    if pr['created_date']:
        events.append((pr['created_date'], "OPEN", f"PR opened by {pr['author']}"))

    # All comments (root + replies) on this PR
    comments = conn.execute(
        """SELECT c.id, c.parent_id, c.author, c.text, c.created_date,
                  c.file_path, c.line, c.severity,
                  (SELECT COUNT(*) FROM comment_reactions cr WHERE cr.comment_id=c.id) AS n_react,
                  (SELECT COUNT(*) FROM pr_comments rep WHERE rep.parent_id=c.id) AS n_reply,
                  (SELECT verdict FROM comment_analysis ca WHERE ca.comment_id=c.id LIMIT 1) AS fb_verdict,
                  (SELECT verdict FROM merge_analysis ma WHERE ma.comment_id=c.id LIMIT 1) AS mg_verdict
           FROM pr_comments c
           WHERE c.repo_id=? AND c.pr_id=?
           ORDER BY c.created_date""",
        (pr['repo_id'], pr_id),
    ).fetchall()

    for c in comments:
        prefix = "REPLY" if c['parent_id'] else "COMMENT"
        anchor = f" {c['file_path']}:{c['line']}" if c['file_path'] else ""
        sev = f" [{c['severity']}]" if c['severity'] and c['severity'] != 'NORMAL' else ""
        verdicts = []
        if c['fb_verdict']:
            verdicts.append(f"fb={c['fb_verdict']}")
        if c['mg_verdict']:
            verdicts.append(f"mg={c['mg_verdict']}")
        v_str = f" [{', '.join(verdicts)}]" if verdicts else ""
        snippet = (c['text'] or '').replace('\n', ' ')[:60]
        signal = ""
        if c['n_react'] or c['n_reply']:
            signal = f" 💬{c['n_reply']} ❤{c['n_react']}"
        text = (
            f"{prefix} #{c['id']} by {c['author']}{anchor}{sev}{signal}{v_str}\n"
            f"           {snippet!r}"
        )
        events.append((c['created_date'] or 0, prefix, text))

    if pr['closed_date']:
        kind = pr['state']  # MERGED or DECLINED
        events.append((pr['closed_date'], kind, f"PR {kind.lower()}"))

    events.sort(key=lambda e: e[0])
    pr_open = pr['created_date']
    for ts, kind, text in events:
        rel = _delta(ts, pr_open) if pr_open else ""
        rel_str = f"  ({rel})" if rel else ""
        print(f"  [{_ts(ts)}{rel_str}]  {text}")

    conn.close()
