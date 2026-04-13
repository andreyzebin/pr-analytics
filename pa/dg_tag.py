"""
Extract diffgraph metadata tags from comment text.

Tag format: `dg:<generation>:<prompt_hash>:<run_id>`
Example:    `dg:prompts:f7917d6:ae0bd23d-8d9`
"""
from __future__ import annotations

import re

_DG_RE = re.compile(r"`dg:(\S+):(\w+):([\w-]+)`")


def extract_dg_tag(text: str) -> dict | None:
    """Extract dg: tag from comment text.

    Returns {"gen": ..., "hash": ..., "run": ...} or None.
    """
    if not text:
        return None
    m = _DG_RE.search(text)
    if m:
        return {"gen": m.group(1), "hash": m.group(2), "run": m.group(3)}
    return None
