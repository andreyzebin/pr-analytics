"""Tests for DB schema, upsert helpers, and comment threading."""
import json
import tempfile
from pathlib import Path

import pytest

from pa.db import (
    delete_pr_comments,
    insert_comment,
    insert_reactions,
    open_db,
    upsert_pr,
    upsert_project,
    upsert_repo,
    walk_comment_thread,
)


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = open_db(path)
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
    upsert_project(db, "MYPROJ", "My Project")
    db.commit()
    row = db.execute("SELECT * FROM projects WHERE key='MYPROJ'").fetchone()
    assert row["name"] == "My Project"
    assert row["cache_date"] is not None


def test_upsert_project_replaces(db):
    upsert_project(db, "MYPROJ", "Old Name")
    upsert_project(db, "MYPROJ", "New Name")
    db.commit()
    assert db.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 1
    assert db.execute("SELECT name FROM projects WHERE key='MYPROJ'").fetchone()[0] == "New Name"


def test_upsert_repo(db):
    upsert_project(db, "PROJ", "Project")
    upsert_repo(db, 101, "PROJ", "my-repo", "My Repo")
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
    upsert_project(db, "P", "P"); upsert_repo(db, 1, "P", "repo", "Repo")
    upsert_pr(db, 1, _make_pr(pr_id=42, state="MERGED"))
    db.commit()
    row = db.execute("SELECT * FROM pull_requests WHERE pr_id=42").fetchone()
    assert row["state"] == "MERGED"
    assert row["author"] == "alice"
    assert row["closed_date"] == 1_700_100_000_000


def test_upsert_pr_open_has_no_closed_date(db):
    upsert_project(db, "P", "P"); upsert_repo(db, 1, "P", "repo", "Repo")
    pr = _make_pr(pr_id=7, state="OPEN")
    pr["closedDate"] = None
    upsert_pr(db, 1, pr)
    db.commit()
    assert db.execute("SELECT closed_date FROM pull_requests WHERE pr_id=7").fetchone()["closed_date"] is None


def test_upsert_pr_reviewers_json(db):
    upsert_project(db, "P", "P"); upsert_repo(db, 1, "P", "repo", "Repo")
    upsert_pr(db, 1, _make_pr(reviewers=[{"user": {"slug": "r1"}}, {"user": {"slug": "r2"}}]))
    db.commit()
    row = db.execute("SELECT reviewers FROM pull_requests WHERE pr_id=1").fetchone()
    assert json.loads(row["reviewers"]) == ["r1", "r2"]


def test_upsert_pr_is_idempotent(db):
    upsert_project(db, "P", "P"); upsert_repo(db, 1, "P", "repo", "Repo")
    upsert_pr(db, 1, _make_pr(pr_id=1, state="OPEN"))
    upsert_pr(db, 1, _make_pr(pr_id=1, state="MERGED"))
    db.commit()
    assert db.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0] == 1
    assert db.execute("SELECT state FROM pull_requests WHERE pr_id=1").fetchone()[0] == "MERGED"


# ── comments & reactions ──────────────────────────────────────────────────────

def _seed_pr(db):
    upsert_project(db, "P", "P"); upsert_repo(db, 1, "P", "repo", "Repo")
    upsert_pr(db, 1, _make_pr()); db.commit()


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
    walk_comment_thread(db, 1, 1, _make_comment(100), None, None)
    db.commit()
    row = db.execute("SELECT * FROM pr_comments WHERE id=100").fetchone()
    assert row["author"] == "alice"
    assert row["parent_id"] is None


def test_inline_comment_anchor(db):
    _seed_pr(db)
    anchor = {"path": "src/main.py", "line": 42, "lineType": "ADDED", "fileType": "TO"}
    walk_comment_thread(db, 1, 1, _make_comment(200), None, anchor)
    db.commit()
    row = db.execute("SELECT * FROM pr_comments WHERE id=200").fetchone()
    assert row["file_path"] == "src/main.py"
    assert row["line"] == 42
    assert row["line_type"] == "ADDED"


def test_thread_replies_have_parent_id(db):
    _seed_pr(db)
    walk_comment_thread(db, 1, 1, _make_comment(101, replies=[_make_comment(102)]), None, None)
    db.commit()
    assert db.execute("SELECT parent_id FROM pr_comments WHERE id=102").fetchone()["parent_id"] == 101


def test_deep_thread(db):
    _seed_pr(db)
    grandchild = _make_comment(303)
    child = _make_comment(302, replies=[grandchild])
    root = _make_comment(301, replies=[child])
    walk_comment_thread(db, 1, 1, root, None, None)
    db.commit()
    assert db.execute("SELECT parent_id FROM pr_comments WHERE id=303").fetchone()["parent_id"] == 302


def test_reactions_stored_for_root(db):
    _seed_pr(db)
    reactions = [
        {"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}, {"slug": "carol"}]},
        {"emoticon": {"shortcut": "heart"}, "users": [{"slug": "dave"}]},
    ]
    walk_comment_thread(db, 1, 1, _make_comment(400, reactions=reactions), None, None)
    db.commit()
    rows = db.execute(
        "SELECT author, emoji FROM comment_reactions WHERE comment_id=400"
    ).fetchall()
    result = {(r["author"], r["emoji"]) for r in rows}
    assert result == {("bob", "+1"), ("carol", "+1"), ("dave", "heart")}


def test_reactions_not_stored_for_child(db):
    _seed_pr(db)
    child = _make_comment(502, reactions=[{"emoticon": {"shortcut": "+1"}, "users": [{"slug": "eve"}]}])
    walk_comment_thread(db, 1, 1, _make_comment(501, replies=[child]), None, None)
    db.commit()
    assert db.execute("SELECT COUNT(*) FROM comment_reactions WHERE comment_id=502").fetchone()[0] == 0


def test_reactions_deduplication(db):
    _seed_pr(db)
    comment = _make_comment(600, reactions=[{"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}]}])
    walk_comment_thread(db, 1, 1, comment, None, None)
    walk_comment_thread(db, 1, 1, comment, None, None)
    db.commit()
    assert db.execute("SELECT COUNT(*) FROM comment_reactions WHERE comment_id=600").fetchone()[0] == 1


def test_delete_pr_comments_clears_reactions(db):
    _seed_pr(db)
    reactions = [{"emoticon": {"shortcut": "+1"}, "users": [{"slug": "bob"}]}]
    walk_comment_thread(db, 1, 1, _make_comment(700, reactions=reactions), None, None)
    db.commit()
    delete_pr_comments(db, 1, 1)
    db.commit()
    assert db.execute("SELECT COUNT(*) FROM pr_comments").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM comment_reactions").fetchone()[0] == 0
