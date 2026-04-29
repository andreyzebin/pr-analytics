"""Time-bucket helpers shared by metrics and the DSL."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def bucket_key(ts_ms: int, period: str) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%G-W%V") if period == "week" else dt.strftime("%Y-%m")


def bucket_display(bk: str) -> str:
    """Human-readable label for a bucket key:
        '2026-W17' → 'Apr 20-26'      (Monday-Sunday of that ISO week)
        '2026-W18' → 'Apr 27-May 03'  (week spans a month boundary)
        '2026-04'  → 'Apr 2026'
    Falls back to the key itself if it doesn't match either format.
    Internal sorting/aggregation still uses the raw key — this is purely
    a render-layer concern.
    """
    if len(bk) >= 7 and "W" in bk:
        try:
            mon = datetime.strptime(bk + "-1", "%G-W%V-%u")
            sun = mon + timedelta(days=6)
            if mon.month == sun.month:
                return f"{mon.strftime('%b %d')}-{sun.strftime('%d')}"
            return f"{mon.strftime('%b %d')}-{sun.strftime('%b %d')}"
        except ValueError:
            return bk
    if len(bk) == 7 and bk[4] == "-":
        try:
            dt = datetime.strptime(bk, "%Y-%m")
            return dt.strftime("%b %Y")
        except ValueError:
            return bk
    return bk


def fmt_hours(hours: float) -> str:
    return f"{hours * 60:.0f}m" if hours < 1 else f"{hours:.1f}h"
