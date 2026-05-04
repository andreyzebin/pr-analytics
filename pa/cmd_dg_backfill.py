"""
Re-extract diffgraph footer fields on already-cached comments.

The cache stage runs `extract_dg_tag()` per fetched comment and writes
``dg_gen / dg_hash / dg_run`` into ``pr_comments``. Two situations make
those columns NULL even though the comment text DOES have a footer:

  1. The comment was cached BEFORE the regex was widened (e.g. it only
     matched ``dg:`` and the agent posted with ``--comment-tag=qodo2``).
  2. The cache was started against an older diffgraph version that
     didn't append a footer at all, then re-cache only updates rows
     touched by the API call.

`dg-backfill` re-runs `extract_dg_tag()` over the local DB and updates
matched rows. No API calls — fast and idempotent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import DEFAULT_DB
from .dg_tag import extract_dg_tag


def cmd_dg_backfill(args, cfg) -> None:
    db_path = Path(args.db) if args.db else Path(DEFAULT_DB)
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    where = "" if args.all else "WHERE dg_gen IS NULL"
    rows = conn.execute(
        f"SELECT id, text FROM pr_comments {where}"
    ).fetchall()

    scanned = len(rows)
    matched = 0
    updates: list[tuple] = []
    for r in rows:
        tag = extract_dg_tag(r["text"] or "")
        if not tag:
            continue
        matched += 1
        updates.append((tag["gen"], tag["hash"], tag["run"], r["id"]))

    print(f"scanned: {scanned}")
    print(f"matched: {matched}")

    if not matched:
        return

    if args.dry_run:
        # Print a few sample matches as preview. Hash is already short
        # by construction (6-16 hex), run gets truncated to 8 chars for
        # readability — full values are still written to DB.
        for gen, h, run, cid in updates[:5]:
            run_short = run if len(run) <= 8 else run[:8] + "…"
            print(f"  would update #{cid}: gen={gen} hash={h[:8]} run={run_short}")
        if len(updates) > 5:
            print(f"  ... and {len(updates) - 5} more (dry-run, not written)")
        return

    conn.executemany(
        "UPDATE pr_comments SET dg_gen=?, dg_hash=?, dg_run=? WHERE id=?",
        updates,
    )
    conn.commit()
    conn.close()
    print(f"updated: {matched}")
