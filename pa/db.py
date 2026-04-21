from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projects (
    key TEXT PRIMARY KEY,
    name TEXT,
    cache_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS repos (
    id INTEGER PRIMARY KEY,
    project_key TEXT,
    slug TEXT,
    name TEXT,
    FOREIGN KEY(project_key) REFERENCES projects(key)
);

CREATE TABLE IF NOT EXISTS pull_requests (
    repo_id INTEGER,
    pr_id INTEGER,
    title TEXT,
    author TEXT,
    created_date INTEGER,
    closed_date INTEGER,
    updated_date INTEGER,
    state TEXT,
    reviewers TEXT,
    PRIMARY KEY (repo_id, pr_id)
);
CREATE INDEX IF NOT EXISTS idx_pr_state_created ON pull_requests(state, created_date);
CREATE INDEX IF NOT EXISTS idx_pr_reviewers ON pull_requests(reviewers);

CREATE TABLE IF NOT EXISTS pr_comments (
    id INTEGER PRIMARY KEY,
    repo_id INTEGER,
    pr_id INTEGER,
    parent_id INTEGER,
    author TEXT,
    text TEXT,
    created_date INTEGER,
    updated_date INTEGER,
    severity TEXT,
    state TEXT,
    file_path TEXT,
    line INTEGER,
    line_type TEXT,
    file_type TEXT,
    FOREIGN KEY(repo_id, pr_id) REFERENCES pull_requests(repo_id, pr_id)
);
CREATE INDEX IF NOT EXISTS idx_comments_author ON pr_comments(author);
CREATE INDEX IF NOT EXISTS idx_comments_pr ON pr_comments(repo_id, pr_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON pr_comments(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_state ON pr_comments(state);

CREATE TABLE IF NOT EXISTS comment_reactions (
    comment_id INTEGER,
    author TEXT,
    emoji TEXT,
    PRIMARY KEY (comment_id, author, emoji),
    FOREIGN KEY(comment_id) REFERENCES pr_comments(id)
);
CREATE INDEX IF NOT EXISTS idx_reactions_comment ON comment_reactions(comment_id);

CREATE TABLE IF NOT EXISTS comment_analysis (
    comment_id   INTEGER NOT NULL,
    judge_model  TEXT    NOT NULL,
    verdict      TEXT    NOT NULL CHECK(verdict IN ('yes','no','unclear')),
    confidence   TEXT    CHECK(confidence IN ('high','medium','low')),
    reasoning    TEXT,
    analyzed_at  INTEGER NOT NULL,
    PRIMARY KEY (comment_id, judge_model),
    FOREIGN KEY (comment_id) REFERENCES pr_comments(id)
);
CREATE INDEX IF NOT EXISTS idx_analysis_comment ON comment_analysis(comment_id);

CREATE TABLE IF NOT EXISTS pr_diff_stats (
    repo_id           INTEGER NOT NULL,
    pr_id             INTEGER NOT NULL,
    lines_added       INTEGER,
    lines_deleted     INTEGER,
    files_changed     INTEGER,
    test_config_ratio REAL,
    fetched_at        INTEGER NOT NULL,
    PRIMARY KEY (repo_id, pr_id),
    FOREIGN KEY (repo_id, pr_id) REFERENCES pull_requests(repo_id, pr_id)
);

CREATE TABLE IF NOT EXISTS comment_classification (
    comment_id       INTEGER NOT NULL,
    classifier_model TEXT    NOT NULL,
    comment_type     TEXT,
    depth            INTEGER CHECK(depth IN (1,2,3)),
    confidence       REAL,
    classified_at    INTEGER NOT NULL,
    PRIMARY KEY (comment_id, classifier_model),
    FOREIGN KEY (comment_id) REFERENCES pr_comments(id)
);
CREATE INDEX IF NOT EXISTS idx_classification_comment ON comment_classification(comment_id);

CREATE TABLE IF NOT EXISTS pr_scores (
    repo_id            INTEGER NOT NULL,
    pr_id              INTEGER NOT NULL,
    scorer_model       TEXT    NOT NULL,
    unique_types       INTEGER,
    avg_depth          REAL,
    diversity_score    REAL,
    depth_score        REAL,
    change_score_ratio REAL,
    style_noise_score  REAL,
    size_score         REAL,
    total_score        REAL,
    verdict            TEXT    CHECK(verdict IN ('GOLD','SILVER','REJECT')),
    verdict_reasoning  TEXT,
    scored_at          INTEGER NOT NULL,
    PRIMARY KEY (repo_id, pr_id, scorer_model),
    FOREIGN KEY (repo_id, pr_id) REFERENCES pull_requests(repo_id, pr_id)
);
CREATE INDEX IF NOT EXISTS idx_pr_scores_total ON pr_scores(total_score DESC);

CREATE TABLE IF NOT EXISTS merge_analysis (
    comment_id       INTEGER NOT NULL,
    judge_model      TEXT    NOT NULL,
    analyzer_version TEXT    NOT NULL DEFAULT 'v0',
    verdict          TEXT    NOT NULL CHECK(verdict IN ('YES','PARTIAL','NO')),
    confidence       REAL,
    reasoning        TEXT,
    analyzed_at      INTEGER NOT NULL,
    PRIMARY KEY (comment_id, judge_model, analyzer_version),
    FOREIGN KEY (comment_id) REFERENCES pr_comments(id)
);
CREATE INDEX IF NOT EXISTS idx_merge_analysis_comment ON merge_analysis(comment_id);
"""


def open_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    # Migrate: add dg tag columns if missing
    try:
        conn.execute("SELECT dg_gen FROM pr_comments LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE pr_comments ADD COLUMN dg_gen TEXT")
        conn.execute("ALTER TABLE pr_comments ADD COLUMN dg_hash TEXT")
        conn.execute("ALTER TABLE pr_comments ADD COLUMN dg_run TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_dg_hash ON pr_comments(dg_hash)")

    # Migrate: merge_analysis PK (comment_id, judge_model) → (comment_id, judge_model, analyzer_version)
    try:
        conn.execute("SELECT analyzer_version FROM merge_analysis LIMIT 0")
    except sqlite3.OperationalError:
        has_old_data = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='merge_analysis'"
        ).fetchone()[0]
        if has_old_data:
            conn.execute("ALTER TABLE merge_analysis RENAME TO _merge_analysis_old")
            conn.executescript("""
                CREATE TABLE merge_analysis (
                    comment_id       INTEGER NOT NULL,
                    judge_model      TEXT    NOT NULL,
                    analyzer_version TEXT    NOT NULL DEFAULT 'v0',
                    verdict          TEXT    NOT NULL CHECK(verdict IN ('YES','PARTIAL','NO')),
                    confidence       REAL,
                    reasoning        TEXT,
                    analyzed_at      INTEGER NOT NULL,
                    PRIMARY KEY (comment_id, judge_model, analyzer_version),
                    FOREIGN KEY (comment_id) REFERENCES pr_comments(id)
                );
                CREATE INDEX IF NOT EXISTS idx_merge_analysis_comment ON merge_analysis(comment_id);
                INSERT INTO merge_analysis (comment_id, judge_model, analyzer_version, verdict, confidence, reasoning, analyzed_at)
                    SELECT comment_id, judge_model, 'v0', verdict, confidence, reasoning, analyzed_at
                    FROM _merge_analysis_old;
                DROP TABLE _merge_analysis_old;
            """)

    conn.commit()
    return conn


def upsert_project(conn: sqlite3.Connection, key: str, name: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO projects(key, name, cache_date) VALUES(?,?,?)",
        (key, name, datetime.now(timezone.utc).isoformat()),
    )


def upsert_repo(conn: sqlite3.Connection, repo_id: int, project_key: str, slug: str, name: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO repos(id, project_key, slug, name) VALUES(?,?,?,?)",
        (repo_id, project_key, slug, name),
    )


def upsert_pr(conn: sqlite3.Connection, repo_id: int, pr: dict) -> None:
    reviewers = json.dumps([r["user"]["slug"] for r in pr.get("reviewers", [])])
    closed_date = None
    if pr.get("closedDate"):
        closed_date = pr["closedDate"]
    elif pr.get("state") in ("MERGED", "DECLINED") and pr.get("updatedDate"):
        closed_date = pr["updatedDate"]

    conn.execute(
        """INSERT OR REPLACE INTO pull_requests
           (repo_id, pr_id, title, author, created_date, closed_date, updated_date, state, reviewers)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (
            repo_id,
            pr["id"],
            pr.get("title", ""),
            pr.get("author", {}).get("user", {}).get("slug", ""),
            pr.get("createdDate"),
            closed_date,
            pr.get("updatedDate"),
            pr.get("state", ""),
            reviewers,
        ),
    )


def delete_pr_comments(conn: sqlite3.Connection, repo_id: int, pr_id: int) -> None:
    comment_ids = [
        row[0] for row in conn.execute(
            "SELECT id FROM pr_comments WHERE repo_id=? AND pr_id=?", (repo_id, pr_id)
        ).fetchall()
    ]
    if comment_ids:
        placeholders = ",".join("?" * len(comment_ids))
        conn.execute(f"DELETE FROM comment_reactions WHERE comment_id IN ({placeholders})", comment_ids)
        conn.execute(f"DELETE FROM comment_analysis WHERE comment_id IN ({placeholders})", comment_ids)
        conn.execute(f"DELETE FROM comment_classification WHERE comment_id IN ({placeholders})", comment_ids)
        conn.execute(f"DELETE FROM merge_analysis WHERE comment_id IN ({placeholders})", comment_ids)
    conn.execute("DELETE FROM pr_comments WHERE repo_id=? AND pr_id=?", (repo_id, pr_id))


def insert_comment(
    conn: sqlite3.Connection,
    repo_id: int,
    pr_id: int,
    comment: dict,
    parent_id: Optional[int],
    anchor: Optional[dict],
) -> None:
    # Extract diffgraph metadata tag if present
    from .dg_tag import extract_dg_tag
    text = comment.get("text", "")
    dg = extract_dg_tag(text)

    conn.execute(
        """INSERT OR REPLACE INTO pr_comments
           (id, repo_id, pr_id, parent_id, author, text, created_date, updated_date,
            severity, state, file_path, line, line_type, file_type,
            dg_gen, dg_hash, dg_run)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            comment["id"],
            repo_id,
            pr_id,
            parent_id,
            comment.get("author", {}).get("slug", ""),
            text,
            comment.get("createdDate"),
            comment.get("updatedDate"),
            comment.get("severity", "NORMAL"),
            comment.get("state", "OPEN"),
            anchor.get("path") if anchor else None,
            anchor.get("line") if anchor else None,
            anchor.get("lineType") if anchor else None,
            anchor.get("fileType") if anchor else None,
            dg["gen"] if dg else None,
            dg["hash"] if dg else None,
            dg["run"] if dg else None,
        ),
    )


def insert_reactions(conn: sqlite3.Connection, comment_id: int, comment: dict) -> None:
    for reaction in comment.get("properties", {}).get("reactions", []):
        emoji = reaction.get("emoticon", {}).get("shortcut", "")
        if not emoji:
            continue
        for user in reaction.get("users", []):
            slug = user.get("slug", "")
            if slug:
                conn.execute(
                    "INSERT OR IGNORE INTO comment_reactions(comment_id, author, emoji) VALUES(?,?,?)",
                    (comment_id, slug, emoji),
                )


def walk_comment_thread(
    conn: sqlite3.Connection,
    repo_id: int,
    pr_id: int,
    comment: dict,
    parent_id: Optional[int],
    anchor: Optional[dict],
) -> None:
    insert_comment(conn, repo_id, pr_id, comment, parent_id, anchor)
    if parent_id is None:
        insert_reactions(conn, comment["id"], comment)
    for child in comment.get("comments", []):
        walk_comment_thread(conn, repo_id, pr_id, child, comment["id"], anchor=None)
