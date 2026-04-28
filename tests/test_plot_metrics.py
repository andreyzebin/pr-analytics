"""Plot metrics correctness tests using --type json output.

Builds a small synthetic DB with predictable PRs/comments/analyses across
3 months and 2 projects, then asserts exact bucket values for representative
metrics (basic counts, ratios, splits, group-by, judge tables).
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from io import StringIO

import pytest

from pa.cmd_plot import cmd_plot
from pa.db import open_db, upsert_project, upsert_repo, upsert_pr


AGENT = "agent-bot"
JUDGE = "test-judge"


def ms(date_str: str) -> int:
    """YYYY-MM-DD → epoch ms (UTC midnight)."""
    return int(datetime.strptime(date_str, "%Y-%m-%d")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def _pr(repo_id: int, pr_id: int, *, state="MERGED", author="dev",
        reviewers=None, created="2026-01-15", closed="2026-01-16"):
    return {
        "repo_id": repo_id, "pr_id": pr_id,
        "title": f"PR #{pr_id}", "author": author,
        "created_date": ms(created), "closed_date": ms(closed),
        "updated_date": ms(closed), "state": state,
        "reviewers": reviewers or [],
    }


def _insert_pr(conn, p: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO pull_requests
           (repo_id, pr_id, title, author, created_date, closed_date,
            updated_date, state, reviewers)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (p["repo_id"], p["pr_id"], p["title"], p["author"],
         p["created_date"], p["closed_date"], p["updated_date"],
         p["state"], json.dumps(p["reviewers"])),
    )


def _insert_comment(conn, *, comment_id: int, repo_id: int, pr_id: int,
                    author=AGENT, parent_id=None, file_path=None,
                    text="…", created="2026-01-15"):
    conn.execute(
        """INSERT INTO pr_comments
           (id, repo_id, pr_id, parent_id, author, text,
            created_date, updated_date, severity, state, file_path,
            line, line_type, file_type)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (comment_id, repo_id, pr_id, parent_id, author, text,
         ms(created), ms(created), "NORMAL", "OPEN", file_path,
         None, None, None),
    )


def _insert_analysis(conn, comment_id: int, verdict: str,
                     judge=JUDGE, when="2026-04-01"):
    conn.execute(
        """INSERT OR REPLACE INTO comment_analysis
           (comment_id, judge_model, verdict, confidence, reasoning, analyzed_at)
           VALUES (?,?,?,?,?,?)""",
        (comment_id, judge, verdict, "high", "", ms(when)),
    )


def _insert_merge(conn, comment_id: int, verdict: str,
                  judge=JUDGE, when="2026-04-01", version="vTest"):
    conn.execute(
        """INSERT OR REPLACE INTO merge_analysis
           (comment_id, judge_model, analyzer_version, verdict,
            confidence, reasoning, analyzed_at)
           VALUES (?,?,?,?,?,?,?)""",
        (comment_id, judge, version, verdict, 1.0, "", ms(when)),
    )


@pytest.fixture
def synthetic_db(tmp_path):
    """A DB with two projects, 5 repos, and PRs across 2026-01..03.

    Layout:
      PRJA/a-svc  (id=1)  — adopted in 2026-02, heavy in 2026-03
      PRJA/a-lib  (id=2)  — adopted only in 2026-03
      PRJA/a-old  (id=3)  — never adopted
      PRJB/b-api  (id=4)  — adopted across all months
      PRJB/b-util (id=5)  — never adopted
    """
    db_path = str(tmp_path / "synthetic.db")
    conn = open_db(db_path)

    upsert_project(conn, "PRJA", "Project A")
    upsert_project(conn, "PRJB", "Project B")
    upsert_repo(conn, 1, "PRJA", "a-svc", "A Service")
    upsert_repo(conn, 2, "PRJA", "a-lib", "A Library")
    upsert_repo(conn, 3, "PRJA", "a-old", "A Old")
    upsert_repo(conn, 4, "PRJB", "b-api", "B API")
    upsert_repo(conn, 5, "PRJB", "b-util", "B Utils")

    prs = [
        # ── 2026-01 — only a-old, b-util, b-api active. Only b-api has agent.
        _pr(1, 101, state="MERGED", created="2026-01-05", closed="2026-01-10"),  # a-svc no-agent
        _pr(3, 301, state="MERGED", created="2026-01-08", closed="2026-01-12"),  # a-old
        _pr(3, 302, state="DECLINED", created="2026-01-09", closed="2026-01-11"),  # a-old declined
        _pr(4, 401, state="MERGED", created="2026-01-10", closed="2026-01-12",
            reviewers=[AGENT]),                                                   # b-api +agent
        _pr(5, 501, state="MERGED", created="2026-01-11", closed="2026-01-13"),  # b-util
        # ── 2026-02 — a-svc adopts agent
        _pr(1, 102, state="MERGED", created="2026-02-05", closed="2026-02-08",
            reviewers=[AGENT]),                                                   # a-svc +agent
        _pr(1, 103, state="MERGED", created="2026-02-07", closed="2026-02-09"),  # a-svc no-agent
        _pr(3, 303, state="MERGED", created="2026-02-10", closed="2026-02-15"),  # a-old
        _pr(4, 402, state="MERGED", created="2026-02-12", closed="2026-02-14",
            reviewers=[AGENT]),                                                   # b-api +agent
        _pr(4, 403, state="DECLINED", created="2026-02-13", closed="2026-02-15"),  # b-api decl
        # ── 2026-03 — wide adoption, a-lib joins, a-svc heavy
        _pr(1, 104, state="MERGED", created="2026-03-02", closed="2026-03-04",
            reviewers=[AGENT]),
        _pr(1, 105, state="MERGED", created="2026-03-05", closed="2026-03-07",
            reviewers=[AGENT]),
        _pr(1, 106, state="MERGED", created="2026-03-06", closed="2026-03-08"),  # a-svc no-agent
        _pr(2, 201, state="MERGED", created="2026-03-10", closed="2026-03-12",
            reviewers=[AGENT]),                                                   # a-lib +agent
        _pr(3, 304, state="MERGED", created="2026-03-15", closed="2026-03-17"),  # a-old
        _pr(4, 404, state="MERGED", created="2026-03-18", closed="2026-03-20",
            reviewers=[AGENT]),
        _pr(5, 502, state="MERGED", created="2026-03-22", closed="2026-03-24"),  # b-util
    ]
    for p in prs:
        _insert_pr(conn, p)

    # Comments by agent — distribute across PRs that have +agent reviewer.
    # a-svc#102 (Feb): 1 inline comment, with reaction
    _insert_comment(conn, comment_id=1001, repo_id=1, pr_id=102,
                    file_path="src/main.py", created="2026-02-06")
    conn.execute(
        "INSERT INTO comment_reactions (comment_id, author, emoji) VALUES (?,?,?)",
        (1001, "dev", "+1"),
    )
    _insert_analysis(conn, 1001, "yes")
    _insert_merge(conn, 1001, "YES")

    # a-svc#104 (Mar): 1 inline comment, no reaction (no feedback)
    _insert_comment(conn, comment_id=1002, repo_id=1, pr_id=104,
                    file_path="src/util.py", created="2026-03-03")

    # a-svc#105 (Mar): 2 root comments, 1 with reply, 1 inline
    _insert_comment(conn, comment_id=1003, repo_id=1, pr_id=105,
                    file_path=None, created="2026-03-06")  # general
    _insert_comment(conn, comment_id=1004, repo_id=1, pr_id=105,
                    file_path="src/api.py", created="2026-03-06")
    _insert_comment(conn, comment_id=1005, repo_id=1, pr_id=105,
                    parent_id=1004, author="dev", created="2026-03-07")
    _insert_analysis(conn, 1003, "no")
    _insert_analysis(conn, 1004, "yes")
    _insert_merge(conn, 1004, "PARTIAL")

    # b-api#404 (Mar): 1 inline + verdict yes + merge YES
    _insert_comment(conn, comment_id=1006, repo_id=4, pr_id=404,
                    file_path="api.go", created="2026-03-19")
    _insert_analysis(conn, 1006, "yes")
    _insert_merge(conn, 1006, "YES")

    conn.commit()
    conn.close()
    return db_path


def _run_plot(db_path: str, **kwargs) -> dict:
    """Invoke cmd_plot with --type json, capture and parse stdout."""
    defaults = dict(
        db=db_path, plot_type="json",
        repos=None, projects="PRJA,PRJB", repos_file=None,
        since=None, until=None, state="MERGED",
        period="month", metrics="total_prs",
        layout="stack", split=None, group_by=None,
        reviewer=None, author=None, judge_model=JUDGE,
        output=None,
        log_level="WARNING",
    )
    defaults.update(kwargs)
    ns = argparse.Namespace(**defaults)

    buf = StringIO()
    sys.stdout, prev = buf, sys.stdout
    try:
        cmd_plot(ns, {})
    finally:
        sys.stdout = prev

    return json.loads(buf.getvalue())


def _series(result: dict, metric: str) -> dict[str, dict[str, float]]:
    """{series_label: {bucket: value}} for the named metric."""
    for m in result["metrics"]:
        if m["name"] == metric:
            return {s["label"]: s["buckets"] for s in m["series"]}
    raise AssertionError(f"metric {metric!r} not in result")


# ── basic counts ──────────────────────────────────────────────────────────────

def test_total_prs_per_repo(synthetic_db):
    r = _run_plot(synthetic_db, projects="PRJA", metrics="total_prs",
                  state="MERGED")
    s = _series(r, "total_prs")
    # a-svc MERGED: 101(Jan), 102+103(Feb), 104+105+106(Mar) = 1, 2, 3
    assert s["PRJA/a-svc"] == {"2026-01": 1, "2026-02": 2, "2026-03": 3}
    # a-old MERGED: 301(Jan), 303(Feb), 304(Mar) — 302 is DECLINED, excluded
    assert s["PRJA/a-old"] == {"2026-01": 1, "2026-02": 1, "2026-03": 1}


def test_total_prs_state_declined(synthetic_db):
    r = _run_plot(synthetic_db, metrics="total_prs", state="DECLINED")
    s = _series(r, "total_prs")
    assert s["PRJA/a-old"] == {"2026-01": 1}
    assert s["PRJB/b-api"] == {"2026-02": 1}


def test_throughput_counts_only_merged(synthetic_db):
    r = _run_plot(synthetic_db, projects="PRJA", metrics="throughput",
                  state="DECLINED")  # state arg ignored for throughput
    s = _series(r, "throughput")
    # a-old: 301 + 303 + 304 = 3 MERGED
    assert sum(s["PRJA/a-old"].values()) == 3


def test_acceptance_rate(synthetic_db):
    r = _run_plot(synthetic_db, projects="PRJA", metrics="acceptance_rate")
    s = _series(r, "acceptance_rate")
    # a-old Jan: 1 MERGED + 1 DECLINED = 50%
    assert s["PRJA/a-old"]["2026-01"] == pytest.approx(50.0)
    # a-old Feb/Mar: only MERGED → 100%
    assert s["PRJA/a-old"]["2026-02"] == pytest.approx(100.0)


# ── unique repos ──────────────────────────────────────────────────────────────

def test_total_repos_count_per_period(synthetic_db):
    r = _run_plot(synthetic_db, metrics="total_repos", split="total")
    s = _series(r, "total_repos")
    # Jan active MERGED repos: a-svc, a-old, b-api, b-util = 4
    # Feb: a-svc, a-old, b-api = 3
    # Mar: a-svc, a-lib, a-old, b-api, b-util = 5
    assert s["Total"] == {"2026-01": 4, "2026-02": 3, "2026-03": 5}


# ── repo-level split: + cohort gets ALL PRs of adopted repos ──────────────────

def test_repo_level_split_reviewer_total_prs(synthetic_db):
    r = _run_plot(synthetic_db, metrics="total_prs",
                  split=f"reviewer:{AGENT}", group_by="project")
    s = _series(r, "total_prs")
    # PRJA adopted repos (≥1 MERGED PR with agent reviewer): a-svc, a-lib
    #   a-svc total MERGED: 1+2+3 = 6
    #   a-lib total MERGED: 0+0+1 = 1
    # PRJA + cohort:  Jan=1, Feb=2, Mar=4
    assert s[f"PRJA / + {AGENT}"] == {"2026-01": 1, "2026-02": 2, "2026-03": 4}
    # PRJA - cohort: a-old (1+1+1)
    assert s[f"PRJA / - {AGENT}"] == {"2026-01": 1, "2026-02": 1, "2026-03": 1}
    # PRJB + cohort: b-api  (Jan=1, Feb=1 MERGED, Mar=1)
    assert s[f"PRJB / + {AGENT}"] == {"2026-01": 1, "2026-02": 1, "2026-03": 1}
    # PRJB - cohort: b-util (Jan=1, Mar=1)
    assert s[f"PRJB / - {AGENT}"] == {"2026-01": 1, "2026-03": 1}


def test_repo_level_split_classification_state_aware(synthetic_db):
    """A repo should NOT be in '+' just because of a DECLINED agent PR
    when --state MERGED is set. b-api has a DECLINED+agent PR in Feb but
    MERGED+agent PRs in all months, so it stays in '+' regardless.
    But if we only had a DECLINED agent PR, it should fall to '-'."""
    # Sanity: with --state MERGED, b-api is correctly "+" because of MERGED agent PRs
    r = _run_plot(synthetic_db, metrics="total_prs",
                  split=f"reviewer:{AGENT}", group_by="project")
    plus = _series(r, "total_prs")[f"PRJB / + {AGENT}"]
    assert sum(plus.values()) == 3  # all 3 MERGED b-api PRs


# ── adoption_rate (PR-based, group-by project) ────────────────────────────────

def test_adoption_rate_pr_based_per_project(synthetic_db):
    r = _run_plot(synthetic_db, metrics="adoption_rate",
                  split=f"reviewer:{AGENT}", group_by="project")
    s = _series(r, "adoption_rate")
    # PRJA Jan: 0 agent / 2 total = 0%   (a-svc#101, a-old#301)
    # PRJA Feb: 1 agent / 3 total = 33.33%  (102 agent; 103, 303 not)
    # PRJA Mar: 3 agent / 5 total = 60%  (104, 105, 201 agent; 106, 304 not)
    assert s["PRJA"]["2026-01"] == pytest.approx(0.0)
    assert s["PRJA"]["2026-02"] == pytest.approx(100 / 3, rel=1e-3)
    assert s["PRJA"]["2026-03"] == pytest.approx(60.0)
    # PRJB Jan: 1 agent / 2 total = 50%
    # PRJB Feb: 1 agent / 1 total = 100%  (only 402 MERGED; 403 DECLINED excluded)
    # PRJB Mar: 1 agent / 2 total = 50%
    assert s["PRJB"]["2026-01"] == pytest.approx(50.0)
    assert s["PRJB"]["2026-02"] == pytest.approx(100.0)
    assert s["PRJB"]["2026-03"] == pytest.approx(50.0)


def test_adoption_rate_requires_split(synthetic_db):
    with pytest.raises(SystemExit) as exc:
        _run_plot(synthetic_db, metrics="adoption_rate")
    assert exc.value.code == 1


# ── feedback_acceptance_rate (judge verdicts) ─────────────────────────────────

def test_feedback_acceptance_rate(synthetic_db):
    r = _run_plot(synthetic_db, metrics="feedback_acceptance_rate",
                  author=AGENT, judge_model=JUDGE)
    s = _series(r, "feedback_acceptance_rate")
    # Verdicts (MERGED PRs only): 1001=yes(Feb), 1003=no(Mar), 1004=yes(Mar), 1006=yes(Mar)
    # Feb: 1 yes / 1 total = 100%
    # Mar: 2 yes / 3 total = 66.67%
    label = next(iter(s.keys()))
    assert s[label]["2026-02"] == pytest.approx(100.0)
    assert s[label]["2026-03"] == pytest.approx(200 / 3, rel=1e-3)


# ── merge_acceptance_rate (analyzer verdicts) ─────────────────────────────────

def test_merge_acceptance_rate(synthetic_db):
    r = _run_plot(synthetic_db, metrics="merge_acceptance_rate",
                  author=AGENT, judge_model=JUDGE)
    s = _series(r, "merge_acceptance_rate")
    # Merge verdicts: 1001=YES(Feb), 1004=PARTIAL(Mar), 1006=YES(Mar)
    # Feb: (1.0)/1 = 100%
    # Mar: (1.0 + 0.5)/2 = 75%
    label = next(iter(s.keys()))
    assert s[label]["2026-02"] == pytest.approx(100.0)
    assert s[label]["2026-03"] == pytest.approx(75.0)


# ── feedback_yes / merge_yes (absolute count metrics) ─────────────────────────

def test_feedback_yes_count(synthetic_db):
    r = _run_plot(synthetic_db, metrics="feedback_yes",
                  author=AGENT, judge_model=JUDGE)
    s = _series(r, "feedback_yes")
    label = next(iter(s.keys()))
    assert s[label]["2026-02"] == 1
    assert s[label]["2026-03"] == 2  # 1004 + 1006
