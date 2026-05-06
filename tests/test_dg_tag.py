"""Tests for diffgraph tag extraction."""
from pa.dg_tag import extract_dg_tag


def test_extract_tag():
    text = "Some finding text\n\n`dg:prompts:f7917d6:ae0bd23d-8d9`"
    result = extract_dg_tag(text)
    assert result == {"tag": "dg", "gen": "prompts", "hash": "f7917d6", "run": "ae0bd23d-8d9"}


def test_extract_tag_v2():
    text = "**Title**\n\nexplanation\n\n`qodo2:diffgraph:04de6f3:5536b832-971`"
    result = extract_dg_tag(text)
    assert result == {"tag": "qodo2", "gen": "diffgraph", "hash": "04de6f3", "run": "5536b832-971"}


def test_no_tag():
    assert extract_dg_tag("just a regular comment") is None


def test_empty():
    assert extract_dg_tag("") is None
    assert extract_dg_tag(None) is None


def test_tag_in_middle():
    # Hash must be lowercase hex 6-16 chars to avoid false positives on
    # `path:to:file:line`-shaped backtick spans.
    text = "before `dg:gen:abc123:run456` after"
    result = extract_dg_tag(text)
    assert result == {"tag": "dg", "gen": "gen", "hash": "abc123", "run": "run456"}


def test_rejects_non_hex_hash():
    # "hash123" contains 'h'/'s' — not hex → must not match.
    assert extract_dg_tag("text `dg:gen:hash123:run456` more") is None
