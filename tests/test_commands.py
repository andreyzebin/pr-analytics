"""Tests for CLI commands that work purely against the local SQLite cache."""
import argparse
import json
import tempfile
from pathlib import Path

import pytest

from pa.cmd_find_repos import cmd_find_repos
from pa.cmd_feedback import cmd_review_feedback
from pa.cmd_sql import cmd_sql
from pa.cmd_status import cmd_status
from pa.db import open_db, upsert_project, upsert_repo, upsert_pr
from pa.utils import date_to_ms, ms_to_date, format_output


# ── helpers ───────────────────────────────────────────────────────────────────

def make_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    conn = open_db(db_path)
    upsert_project(conn, "PROJ", "Project")
    upsert_repo(conn, 1, "PROJ", "backend", "Backend")
    conn.commit()
    return db_path, conn


def insert_pr(conn, pr_id, state="MERGED", created=1_700_000_000_000,
              closed=1_700_360_000_000, reviewers=None):
    conn.execute(
        """INSERT OR REPLACE INTO pull_requests
           (repo_id, pr_id, title, author, created_date, closed_date, updated_date, state, reviewers)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (1, pr_id, f"PR #{pr_id}", "alice", created, closed, closed, state,
         json.dumps(reviewers or ["bob"])),
    )


def args(**kwargs) -> argparse.Namespace:
    defaults = dict(db=None, since=None, until=None, repos=None, projects=None,
                    repos_file=None, state=None, output=None, format="table")
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ── status ────────────────────────────────────────────────────────────────────

def test_status_empty_db(tmp_path, capsys):
    db_path, conn = make_db(tmp_path)
    conn.close()
    cmd_status(args(db=db_path), {})
    out = capsys.readouterr().out
    assert "PRs: 0" in out
    assert "Comments: 0" in out


def test_status_with_data(tmp_path, capsys):
    db_path, conn = make_db(tmp_path)
    insert_pr(conn, 1); insert_pr(conn, 2)
    conn.commit(); conn.close()
    cmd_status(args(db=db_path), {})
    assert "PRs: 2" in capsys.readouterr().out


def test_status_missing_db_exits_4(tmp_path):
    with pytest.raises(SystemExit) as exc:
        cmd_status(args(db=str(tmp_path / "nope.db")), {})
    assert exc.value.code == 4


# ── find-repos ────────────────────────────────────────────────────────────────

def test_find_repos_returns_repo(tmp_path, capsys):
    db_path, conn = make_db(tmp_path)
    insert_pr(conn, 1, reviewers=["ivan"]); conn.commit(); conn.close()
    cmd_find_repos(args(db=db_path, reviewer="ivan"), {})
    assert "PROJ/backend" in capsys.readouterr().out


def test_find_repos_no_match_exits_4(tmp_path):
    db_path, conn = make_db(tmp_path)
    insert_pr(conn, 1, reviewers=["someone-else"]); conn.commit(); conn.close()
    with pytest.raises(SystemExit) as exc:
        cmd_find_repos(args(db=db_path, reviewer="ivan"), {})
    assert exc.value.code == 4


def test_find_repos_state_filter(tmp_path, capsys):
    db_path, conn = make_db(tmp_path)
    insert_pr(conn, 1, state="OPEN", closed=None, reviewers=["ivan"])
    insert_pr(conn, 2, state="MERGED", reviewers=["ivan"])
    conn.commit(); conn.close()
    cmd_find_repos(args(db=db_path, reviewer="ivan", state="MERGED"), {})
    assert capsys.readouterr().out.strip().count("PROJ/backend") == 1


def test_find_repos_writes_file(tmp_path):
    db_path, conn = make_db(tmp_path)
    insert_pr(conn, 1, reviewers=["ivan"]); conn.commit(); conn.close()
    out_file = str(tmp_path / "repos.txt")
    cmd_find_repos(args(db=db_path, reviewer="ivan", output=out_file), {})
    assert Path(out_file).read_text().strip() == "PROJ/backend"


# ── sql ───────────────────────────────────────────────────────────────────────

def test_sql_select(tmp_path, capsys):
    db_path, conn = make_db(tmp_path); conn.close()
    cmd_sql(args(db=db_path, query="SELECT 1+1 AS n", limit=100), {})
    assert "2" in capsys.readouterr().out


def test_sql_rejects_insert(tmp_path):
    db_path, conn = make_db(tmp_path); conn.close()
    with pytest.raises(SystemExit) as exc:
        cmd_sql(args(db=db_path, query="INSERT INTO projects VALUES('x','y',NULL)", limit=0), {})
    assert exc.value.code == 5


def test_sql_rejects_drop(tmp_path):
    db_path, conn = make_db(tmp_path); conn.close()
    with pytest.raises(SystemExit) as exc:
        cmd_sql(args(db=db_path, query="DROP TABLE projects", limit=0), {})
    assert exc.value.code == 5


def test_sql_with_cte(tmp_path, capsys):
    db_path, conn = make_db(tmp_path); conn.close()
    cmd_sql(args(db=db_path, query="WITH x AS (SELECT 42 AS v) SELECT v FROM x", limit=100), {})
    assert "42" in capsys.readouterr().out


def test_sql_json_format(tmp_path, capsys):
    db_path, conn = make_db(tmp_path); conn.close()
    cmd_sql(args(db=db_path, query="SELECT key FROM projects", limit=100, format="json"), {})
    assert json.loads(capsys.readouterr().out)[0]["key"] == "PROJ"


def test_sql_csv_format(tmp_path, capsys):
    db_path, conn = make_db(tmp_path); conn.close()
    cmd_sql(args(db=db_path, query="SELECT key FROM projects", limit=100, format="csv"), {})
    out = capsys.readouterr().out
    assert "key" in out and "PROJ" in out


def test_sql_output_file(tmp_path):
    db_path, conn = make_db(tmp_path); conn.close()
    out_file = str(tmp_path / "result.csv")
    cmd_sql(args(db=db_path, query="SELECT key FROM projects", limit=100,
                 format="csv", output=out_file), {})
    assert "PROJ" in Path(out_file).read_text()


# ── review-feedback ───────────────────────────────────────────────────────────

def _seed_feedback(conn):
    insert_pr(conn, 10, state="MERGED"); conn.commit()
    conn.execute(
        """INSERT INTO pr_comments
           (id, repo_id, pr_id, parent_id, author, text, created_date, updated_date,
            severity, state, file_path, line, line_type, file_type)
           VALUES(1001,1,10,NULL,'ai-bot','NPE risk',1700000000000,1700000000000,
                  'BLOCKER','OPEN','src/Foo.java',42,'ADDED','TO')"""
    )
    conn.execute("INSERT INTO comment_reactions VALUES(1001,'bob','+1')")
    conn.execute("INSERT INTO comment_reactions VALUES(1001,'carol','-1')")
    conn.execute(
        """INSERT INTO pr_comments
           (id,repo_id,pr_id,parent_id,author,text,created_date,updated_date,
            severity,state,file_path,line,line_type,file_type)
           VALUES(1002,1,10,1001,'bob','Good catch!',1700000100000,1700000100000,
                  'NORMAL','OPEN',NULL,NULL,NULL,NULL)"""
    )
    conn.commit()


def test_review_feedback_basic(tmp_path, capsys):
    db_path, conn = make_db(tmp_path)
    _seed_feedback(conn); conn.close()
    cmd_review_feedback(args(db=db_path, author="ai-bot", min_reactions=0, format="table"), {})
    out = capsys.readouterr().out
    assert "NPE risk" in out
    assert "PROJ/backend" in out


def test_review_feedback_reaction_counts(tmp_path):
    db_path, conn = make_db(tmp_path)
    _seed_feedback(conn); conn.close()
    out_file = str(tmp_path / "feedback.json")
    cmd_review_feedback(
        args(db=db_path, author="ai-bot", min_reactions=0, format="json", output=out_file), {}
    )
    data = json.loads(Path(out_file).read_text())
    assert len(data) == 1
    row = data[0]
    assert row["reactions_positive"] == 1
    assert row["reactions_negative"] == 1
    assert row["replies_count"] == 1
    assert row["replies"][0]["author"] == "bob"


def test_review_feedback_min_reactions_filter(tmp_path):
    db_path, conn = make_db(tmp_path)
    _seed_feedback(conn)
    conn.execute(
        """INSERT INTO pr_comments
           (id,repo_id,pr_id,parent_id,author,text,created_date,updated_date,
            severity,state,file_path,line,line_type,file_type)
           VALUES(1003,1,10,NULL,'ai-bot','Minor style',1700000200000,1700000200000,
                  'NORMAL','OPEN',NULL,NULL,NULL,NULL)"""
    )
    conn.commit(); conn.close()
    out_file = str(tmp_path / "out.json")
    cmd_review_feedback(
        args(db=db_path, author="ai-bot", min_reactions=1, format="json", output=out_file), {}
    )
    data = json.loads(Path(out_file).read_text())
    assert len(data) == 1
    assert data[0]["comment_id"] == 1001


def test_review_feedback_no_data_exits_0(tmp_path, capsys):
    db_path, conn = make_db(tmp_path); conn.close()
    cmd_review_feedback(args(db=db_path, author="nobody", min_reactions=0), {})
    assert "Нет данных" in capsys.readouterr().out


# ── date helpers ──────────────────────────────────────────────────────────────

def test_date_to_ms_round_trip():
    assert ms_to_date(date_to_ms("2026-01-15")) == "2026-01-15"


def test_date_to_ms_end_of_day():
    start = date_to_ms("2026-01-15")
    end = date_to_ms("2026-01-15", end_of_day=True)
    assert end - start == 86_399_000


# ── format_output ─────────────────────────────────────────────────────────────

def test_format_output_csv():
    out = format_output([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], ["a", "b"], "csv")
    lines = out.strip().splitlines()
    assert lines[0] == "a,b"
    assert "1,x" in lines[1]


def test_format_output_json():
    assert json.loads(format_output([{"a": 1}], ["a"], "json")) == [{"a": 1}]


def test_format_output_table_contains_data():
    assert "value123" in format_output([{"col": "value123"}], ["col"], "table")
