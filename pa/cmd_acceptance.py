"""
Command: acceptance — query acceptance metrics by diffgraph prompt hash.

Usage:
    pr_analytics.py acceptance --dg-hash abc123
    pr_analytics.py acceptance --dg-hash abc123 --since 2026-04-01
"""
from __future__ import annotations

import json
import logging
import sqlite3

log = logging.getLogger(__name__)


def cmd_acceptance(args, cfg=None) -> None:
    """Show acceptance metrics for a diffgraph prompt hash."""
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row

    dg_hash = args.dg_hash
    since_clause = f"AND p.created_date >= {int(args.since_ts * 1000)}" if hasattr(args, "since_ts") and args.since_ts else ""

    # Total comments with this hash
    row = conn.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN a.verdict = 'yes' THEN 1 END) as accepted,
            COUNT(CASE WHEN a.verdict = 'no' THEN 1 END) as rejected,
            COUNT(CASE WHEN a.verdict IS NOT NULL THEN 1 END) as analyzed,
            COUNT(CASE WHEN cr.comment_id IS NOT NULL THEN 1 END) as with_feedback
        FROM pr_comments c
        LEFT JOIN comment_analysis a ON a.comment_id = c.id
        LEFT JOIN (
            SELECT DISTINCT comment_id FROM comment_reactions
        ) cr ON cr.comment_id = c.id
        LEFT JOIN pull_requests p ON p.repo_id = c.repo_id AND p.pr_id = c.pr_id
        WHERE c.dg_hash = ?
        AND c.parent_id IS NULL
        {since_clause}
    """, (dg_hash,)).fetchone()

    total = row["total"]
    accepted = row["accepted"]
    rejected = row["rejected"]
    analyzed = row["analyzed"]
    with_feedback = row["with_feedback"]

    result = {
        "dg_hash": dg_hash,
        "total_comments": total,
        "analyzed": analyzed,
        "with_feedback": with_feedback,
        "accepted": accepted,
        "rejected": rejected,
        "acceptance_rate": round(accepted / (accepted + rejected), 3) if (accepted + rejected) > 0 else None,
        "acceptance_rate_all": round(accepted / total, 3) if total > 0 else None,
        "false_positive_rate": round(rejected / (accepted + rejected), 3) if (accepted + rejected) > 0 else None,
        "feedback_rate": round(with_feedback / total, 3) if total > 0 else None,
    }

    conn.close()

    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        for k, v in result.items():
            print(f"  {k}: {v}")
