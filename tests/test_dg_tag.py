"""Tests for diffgraph tag extraction."""
from pa.dg_tag import extract_dg_tag


def test_extract_tag():
    text = "Some finding text\n\n`dg:prompts:f7917d6:ae0bd23d-8d9`"
    result = extract_dg_tag(text)
    assert result == {"gen": "prompts", "hash": "f7917d6", "run": "ae0bd23d-8d9"}


def test_extract_tag_v2():
    text = "**Title**\n\nexplanation\n\n`dg:v2:abc1234:run-001`"
    result = extract_dg_tag(text)
    assert result == {"gen": "v2", "hash": "abc1234", "run": "run-001"}


def test_no_tag():
    assert extract_dg_tag("just a regular comment") is None


def test_empty():
    assert extract_dg_tag("") is None
    assert extract_dg_tag(None) is None


def test_tag_in_middle():
    text = "before `dg:gen:hash123:run456` after"
    result = extract_dg_tag(text)
    assert result == {"gen": "gen", "hash": "hash123", "run": "run456"}
