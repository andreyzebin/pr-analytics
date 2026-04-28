"""Time-bucket helpers shared by metrics and the DSL."""
from __future__ import annotations

from datetime import datetime, timezone


def bucket_key(ts_ms: int, period: str) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%G-W%V") if period == "week" else dt.strftime("%Y-%m")


def fmt_hours(hours: float) -> str:
    return f"{hours * 60:.0f}m" if hours < 1 else f"{hours:.1f}h"
