"""Tests for DB schema, upsert helpers, and comment threading."""
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

import pr_analytics as pa


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = pa.open_db(path)
    yield conn
    conn.close()
    Path(path).unlink(missing_ok=True)


# ── schema ────────────────────────────────────────────────────────────────────

def test_schema_creates_all_tables(db):
    tables = {
        row[0] for row in
        db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert {"projects", "repos", "pull_requests", "pr_comments", "comment_reactions"} <= tables


# ── projects / repos ──────────────────────────────────────────────────────────

def test_upsert_project(db):
    pa.upsert_project(db, "MYPROJ", "My Project")
    db.commit()
    row = db.execute("SELECT * FROM projects WHERE key='MYPROJ'").fetchone()
    assert row["name"] == "My Project"
    assert row["cache_date"] is not None


def test_upsert_project_replaces(db):
    pa.upsert_project(db, "MYPROJ", "Old Name")
    pa.upsert_project(db, "MYPROJ", "New Name")
    db.commit()
    count = db.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    assert count == 1
    assert db.execute("SELECT name FROM projects WHERE key='MYPROJ'").fetchone()[0] == "New Name"


def test_upsert_repo(db):
    pa.upsert_project(db, "PROJ", "Project")
    pa.upsert_repo(db, 101, "PROJ", "my-repo", "My Repo")
    db.commit()
    row = db.execute("SELECT * FROM repos WHERE id=101").fetchone()
    assert row["slug"] == "my-repo"
    assert row["project_key"] == "PROJ"


# ── pull_requests ─────────────────────────────────────────────────────────────

def _make_pr(pr_id=1, state="MERGED", reviewers=None):
    return {
        "id": pr_id,
        "title": f"PR #{pr_id}",
        "author": {"user": {"slug": "alice"}},
        "createdDate": 1_700_000_000_000,
        "updatedDate": 1_700_100_000_000,
        "closedDate": 1_700_100_000_000,
        "state": state,
        "reviewers": reviewers or [{"user": {"slug": "bob"}}],
    }


def test_upsert_pr_merged(db):
    pa.upsert_project(db, "P", "P")
    pa.upsert_repo(db, 1, "P", "repo", "Repo")
    pa.upsert_pr(db, 1, _make_pr(pr_id=42, state="MERGED"))
    db.commit()
    row = db.execute("SELECT * FROM pull_requests WHERE pr_id=42").fetchone()
    assert row["state"] == "MERGED"
    assert row["author"] == "alice"
    assert row["closed_date"] == 1_700_100_000_000


def test_upsert_pr_open_has_no_closed_date(db):
    pa.upsert_project(db, "P", "P")
    pa.upsert_repo(db, 1, "P", "repo", "Repo")
    pr = _make_pr(pr_id=7, state="OPEN")
    pr["closedDate"] = None
    pa.upsert_pr(db, 1, pr)
    db.commit()
    row = db.execute("SELECT closed_date FROM pull_requests WHERE pr_id=7").fetchone()
    assert row["closed_date"] is None


def test_upsert_pr_reviewers_json(db):
    pa.upsert_project(db, "P", "P")
    pa.upsert_repo(db, 1, "P", "repo", "Repo")
    pa.upsert_pr(db, 1, _make_pr(reviewers=[{"user": {"slug": "r1"}}, {"user": {"slug": "r2"}}]))
    db.commit()
    row = db.execute("SELECT reviewers FROM pull_requests WHERE pr_id=1").fetchone()
    assert json.loads(row["reviewers"]) == ["r1", "r2"]


def test_upsert_pr_is_idempotent(db):
    pa.upsert_project(db, "P", "P")
    pa.upsert_repo(db, 1, "P", "repo", "Repo")
    pa.upsert_pr(db, 1, _make_pr(pr_id=1, state="OPEN"))
    pa.upsert_pr(db, 1, _make_pr(pr_id=1, state="MERGED"))
    db.commit()
    count = db.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
    assert count == 1
    assert db.execute("SELECT state FROM pull_requests WHERE pr_id=1").fetchone()[0] == "MERGED"


# ── comments & reactions ──────────────────────────────────────────────────────

def _seed_pr(db):
    pa.upsert_project(db, "P", "P")
    pa.upsert_repo(db, 1, "P", "repo", "Repo")
    pa.upsert_pr(db, 1, _make_pr())
    db.commit()


def _make_comment(cid, text="hello", replies=None, reactions=None):
    return {
        "id": cid,
        "author": {"slug": "alice"},
        "text": text,
        "createdDate": 1_700_000_000_000,
        "updatedDate": 1_700_000_000_000,
        "severity": "NORMAL",
        "state": "OPEN",
        "comments": replies or [],
        "properties": {"reactions": reactions or []},
    }


def test_root_comment_stored(db):
    _seed_pr(db)
    comment = _make_comment(100)
    pa.walk_comment_thread(db, 1, 1, comment, None, None)
    db.commit()
    row = db.execute("SELECT * FROM pr_comments WHERE id=100").fetchone()
    assert row["author"] == "alice"
    assert row["parent_id"] is None
    assert row["text"] == "hello"


def test_inline_comment_anchor(db):
    _seed_pr(db)
    anchor = {"path": "src/main.py", "line": 42, "lineType": "ADDED", "fileType": "TO"}
    comment = _make_comment(200)
    pa.walk_comment_thread(db, 1, 1, comment, None, anchor)
    db.commit()
    row = db.execute("SELECT * FROM pr_comments WHERE id=200").fetchone()
    assert row["file_path"] == "src/main.py"
    assert row["line"] == 42
    assert row["line_type"] == "ADDED"


def test_thread_replies_have_parent_id(db):
    _seed_pr(db)
    reply = _make_comment(102, text="reply")
    comment = _make_comment(101, replies=[reply])
    pa.walk_comment_thread(db, 1, 1, comment, None, None)
    db.commit()
    child = db.execute("SELECT * FROM pr_comments WHERE id=102").fetchone()
    assert child["parent_id"] == 101


def test_deep_thread(db):
    """3-level thread: root → child → grandchild."""
    _seed_pr(db)
    grandchild = _make_comment(303, text="grandchild")
    child = _make_comment(302, text="child", replies=[grandchild])
    root = _make_comment(301, text="root", replies=[child])
    pa.walk_comment_thread(db, 1, 1, root, None, None)
    db.commit()
    gc = db.execute("SELECT parent_id FROM pr_comments WHERE id=303").fetchone()
    assert gc["parent_id"] == 302


def test_reactions_stored_for_root(db):
    _seed_pr(db)
    reactions = [
        {"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}, {"slug": "carol"}]},
        {"emoticon": {"shortcut": "heart"}, "users": [{"slug": "dave"}]},
    ]
    comment = _make_comment(400, reactions=reactions)
    pa.walk_comment_thread(db, 1, 1, comment, None, None)
    db.commit()
    rows = db.execute(
        "SELECT author, emoji FROM comment_reactions WHERE comment_id=400 ORDER BY author, emoji"
    ).fetchall()
    result = {(r["author"], r["emoji"]) for r in rows}
    assert result == {("bob", "+1"), ("carol", "+1"), ("dave", "heart")}


def test_reactions_not_stored_for_child(db):
    """Children don't get reactions even if they had some in JSON (API doesn't send them)."""
    _seed_pr(db)
    child = _make_comment(502, reactions=[
        {"emoticon": {"shortcut": "+1"}, "users": [{"slug": "eve"}]}
    ])
    root = _make_comment(501, replies=[child])
    pa.walk_comment_thread(db, 1, 1, root, None, None)
    db.commit()
    count = db.execute(
        "SELECT COUNT(*) FROM comment_reactions WHERE comment_id=502"
    ).fetchone()[0]
    assert count == 0


def test_reactions_deduplication(db):
    _seed_pr(db)
    reactions = [{"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}]}]
    comment = _make_comment(600, reactions=reactions)
    pa.walk_comment_thread(db, 1, 1, comment, None, None)
    pa.walk_comment_thread(db, 1, 1, comment, None, None)  # second insert
    db.commit()
    count = db.execute(
        "SELECT COUNT(*) FROM comment_reactions WHERE comment_id=600"
    ).fetchone()[0]
    assert count == 1


def test_delete_pr_comments_clears_reactions(db):
    _seed_pr(db)
    reactions = [{"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}]}]
    comment = _make_comment(700, reactions=reactions)
    pa.walk_comment_thread(db, 1, 1, comment, None, None)
    db.commit()

    pa.delete_pr_comments(db, 1, 1)
    db.commit()

    assert db.execute("SELECT COUNT(*) FROM pr_comments").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM comment_reactions").fetchone()[0] == 0
