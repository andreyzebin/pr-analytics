"""
Extract diffgraph metadata tags from comment text.

Tag format: ``<tag>:<generation>:<prompt_hash>:<run_id>``

The leading ``<tag>`` is configurable on the agent side via
``cli.py run --comment-tag <prefix>`` (default ``dg``). Different
deployments use different prefixes — for instance ``dg`` for the
production bot and ``qodo2`` for a test bot — and the cache must
recognise all of them.

Examples:
    `dg:prompts:f7917d6:ae0bd23d-8d9`
    `qodo2:diffgraph:04de6f3:5536b832-971`
"""
from __future__ import annotations

import re

# Match any backtick-delimited 4-segment footer that LOOKS like an agent tag:
#   <tag> : <generation> : <hex hash 6+ chars> : <run id with letters/digits/dashes>
# Restricting the hash to lowercase hex avoids false positives on stray
# `path:to:file:line`-shaped backtick spans in user comments.
_DG_RE = re.compile(
    r"`(?P<tag>\w+):(?P<gen>\S+?):(?P<hash>[a-f0-9]{6,16}):(?P<run>[\w-]+)`"
)


def extract_dg_tag(text: str) -> dict | None:
    """Extract diffgraph footer fields from comment text.

    Returns ``{"tag", "gen", "hash", "run"}`` or ``None``. The ``tag`` field
    captures the configurable prefix (e.g. ``dg``, ``qodo2``) so callers can
    distinguish bot deployments if needed; existing callers ignore it and
    only consume gen/hash/run.
    """
    if not text:
        return None
    m = _DG_RE.search(text)
    if m:
        return {
            "tag": m.group("tag"),
            "gen": m.group("gen"),
            "hash": m.group("hash"),
            "run": m.group("run"),
        }
    return None
