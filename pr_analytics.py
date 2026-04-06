#!/usr/bin/env python3
"""
pr_analytics.py — Bitbucket Server PR analytics tool.

Config files (loaded in order, local overrides base):
    config.yaml        — base config, committed to VCS
    config.local.yaml  — local overrides, NOT committed

Environment variables (override config files):
    BB_TOKEN / BITBUCKET_SERVER_BEARER_TOKEN   Bearer token
    BB_URL                                     Bitbucket base URL
    BB_DB                                      SQLite DB path
    REQUESTS_CA_BUNDLE                         CA bundle path
    BITBUCKET_SERVER_CLIENT_CERT               Client PEM path (mTLS)

Values in YAML can reference env vars: token: "${BB_TOKEN}"
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

log = logging.getLogger("pr_analytics")

# ── constants ─────────────────────────────────────────────────────────────────

DEFAULT_DB = "bitbucket_cache.db"
CONFIG_YAML = "config.yaml"
CONFIG_LOCAL_YAML = "config.local.yaml"
DEFAULT_CONCURRENCY = 4
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

POSITIVE_EMOJIS = {"+1", "thumbsup", "heart", "tada"}
NEGATIVE_EMOJIS = {"-1", "thumbsdown"}

# ── DB schema ─────────────────────────────────────────────────────────────────

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
"""

# ── config loading ────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _expand_env(obj: Any) -> Any:
    """Recursively expand ${VAR} references in string values."""
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    if isinstance(obj, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    return obj


def load_config() -> dict:
    import yaml

    cfg: dict = {}
    base = Path(CONFIG_YAML)
    if base.exists():
        with open(base) as f:
            cfg = yaml.safe_load(f) or {}

    local = Path(CONFIG_LOCAL_YAML)
    if local.exists():
        with open(local) as f:
            cfg = _deep_merge(cfg, yaml.safe_load(f) or {})

    return _expand_env(cfg)


def _bb(cfg: dict) -> dict:
    """Shortcut to cfg['bitbucket'] section."""
    return cfg.get("bitbucket", {})


def _cache_cfg(cfg: dict) -> dict:
    return cfg.get("cache", {})


def resolve_token(args_token: Optional[str], cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BB_TOKEN")
        or os.environ.get("BITBUCKET_SERVER_BEARER_TOKEN")
        or os.environ.get("BITBUCKET_SERVER__BEARER_TOKEN")
        or args_token
        or _bb(cfg).get("token")
    ) or None


def resolve_url(args_url: Optional[str], cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BB_URL")
        or args_url
        or _bb(cfg).get("url")
    ) or None


def resolve_db(args_db: Optional[str], cfg: dict) -> str:
    return (
        os.environ.get("BB_DB")
        or args_db
        or _cache_cfg(cfg).get("db")
        or DEFAULT_DB
    )


def resolve_ca_bundle(cfg: dict) -> Optional[str]:
    return (
        os.environ.get("REQUESTS_CA_BUNDLE")
        or _bb(cfg).get("ca_bundle")
    ) or None


def resolve_client_cert(cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BITBUCKET_SERVER_CLIENT_CERT")
        or os.environ.get("BITBUCKET_SERVER__CLIENT_CERT")
        or _bb(cfg).get("client_cert")
    ) or None


# ── HTTP session ──────────────────────────────────────────────────────────────

def make_session(token: str, cfg: dict) -> requests.Session:
    ca_bundle = resolve_ca_bundle(cfg)
    client_cert = resolve_client_cert(cfg)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    if ca_bundle:
        session.verify = ca_bundle
    if client_cert:
        session.cert = client_cert
    return session


def api_get(session: requests.Session, url: str) -> dict:
    log.debug("GET %s", url)
    t0 = time.monotonic()
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            elapsed = time.monotonic() - t0
            log.debug("  → %d  %.2fs", resp.status_code, elapsed)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                log.warning("Rate limited, waiting %ds", retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code in (401, 403):
                log.error("Authentication error %d: %s", resp.status_code, url)
                sys.exit(2)

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                log.warning("Timeout on %s, retry %d/%d in %ds", url, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
            else:
                log.error("Timeout after %d retries: %s", MAX_RETRIES, url)
                sys.exit(3)
        except requests.exceptions.ConnectionError as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                log.warning("Connection error: %s, retry in %ds", e, wait)
                time.sleep(wait)
            else:
                log.error("Connection failed after %d retries: %s", MAX_RETRIES, e)
                sys.exit(3)

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {url}")


def paginate(session: requests.Session, url: str, limit: int = 25) -> list:
    results = []
    start = 0
    base_url = url + ("&" if "?" in url else "?")
    while True:
        page_url = f"{base_url}start={start}&limit={limit}"
        data = api_get(session, page_url)
        values = data.get("values", [])
        results.extend(values)
        if data.get("isLastPage", True):
            break
        start = data.get("nextPageStart", start + limit)
    return results


# ── DB helpers ────────────────────────────────────────────────────────────────

def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
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
    conn.execute("DELETE FROM pr_comments WHERE repo_id=? AND pr_id=?", (repo_id, pr_id))


def insert_comment(
    conn: sqlite3.Connection,
    repo_id: int,
    pr_id: int,
    comment: dict,
    parent_id: Optional[int],
    anchor: Optional[dict],
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO pr_comments
           (id, repo_id, pr_id, parent_id, author, text, created_date, updated_date,
            severity, state, file_path, line, line_type, file_type)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            comment["id"],
            repo_id,
            pr_id,
            parent_id,
            comment.get("author", {}).get("slug", ""),
            comment.get("text", ""),
            comment.get("createdDate"),
            comment.get("updatedDate"),
            comment.get("severity", "NORMAL"),
            comment.get("state", "OPEN"),
            anchor.get("path") if anchor else None,
            anchor.get("line") if anchor else None,
            anchor.get("lineType") if anchor else None,
            anchor.get("fileType") if anchor else None,
        ),
    )


def insert_reactions(conn: sqlite3.Connection, comment_id: int, comment: dict) -> None:
    reactions = comment.get("properties", {}).get("reactions", [])
    for reaction in reactions:
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


# ── cache command ─────────────────────────────────────────────────────────────

def cmd_cache(args: argparse.Namespace, cfg: dict) -> None:
    token = resolve_token(getattr(args, "token", None), cfg)
    url = resolve_url(getattr(args, "url", None), cfg)
    db_path = resolve_db(getattr(args, "db", None), cfg)

    if not token:
        log.error("No token provided. Use --token, BB_TOKEN, or config file.")
        sys.exit(1)
    if not url:
        log.error("No URL provided. Use --url, BB_URL, or config file.")
        sys.exit(1)

    url = url.rstrip("/")
    concurrency = getattr(args, "concurrency", None) or _cache_cfg(cfg).get("concurrency", DEFAULT_CONCURRENCY)
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    no_comments = getattr(args, "no_comments", False)

    session = make_session(token, cfg)
    conn = open_db(db_path)

    # Determine repos to cache
    repos_to_cache: list[tuple[str, str]] = []  # [(project_key, slug)]

    if args.repos:
        for entry in args.repos.split(","):
            entry = entry.strip()
            if "/" in entry:
                proj, slug = entry.split("/", 1)
                repos_to_cache.append((proj.strip(), slug.strip()))
            else:
                log.warning("Invalid repo spec (expected PROJ/repo): %s", entry)
    elif args.projects:
        project_keys = [p.strip() for p in args.projects.split(",")]
        for proj_key in project_keys:
            print(f"  Project: {proj_key}", flush=True)
            proj_repos = fetch_project_repos(session, url, proj_key)
            upsert_project(conn, proj_key, proj_key)
            for repo in proj_repos:
                upsert_repo(conn, repo["id"], proj_key, repo["slug"], repo["name"])
                repos_to_cache.append((proj_key, repo["slug"]))
            conn.commit()
    else:
        # Fetch all accessible projects
        projects = fetch_all_projects(session, url)
        for proj in projects:
            print(f"  Project: {proj['key']}", flush=True)
            upsert_project(conn, proj["key"], proj["name"])
            proj_repos = fetch_project_repos(session, url, proj["key"])
            for repo in proj_repos:
                upsert_repo(conn, repo["id"], proj["key"], repo["slug"], repo["name"])
                repos_to_cache.append((proj["key"], repo["slug"]))
        conn.commit()

    if not repos_to_cache:
        log.warning("No repositories found to cache.")
        return

    print(f"Caching {len(repos_to_cache)} repositories with concurrency={concurrency}", flush=True)

    def cache_repo(proj_key: str, repo_slug: str) -> int:
        repo_row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
        ).fetchone()
        if not repo_row:
            # Repo not in DB yet (--repos mode skipped project fetch)
            repo_data = api_get(
                session,
                f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}",
            )
            upsert_project(conn, proj_key, proj_key)
            upsert_repo(conn, repo_data["id"], proj_key, repo_data["slug"], repo_data["name"])
            conn.commit()
            repo_id = repo_data["id"]
        else:
            repo_id = repo_row["id"]

        pr_url = f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}/pull-requests"
        params = "state=ALL"
        if since_ts:
            params += f"&start=0"
        prs = paginate(session, f"{pr_url}?{params}")

        # Apply date filters
        filtered_prs = []
        for pr in prs:
            cd = pr.get("createdDate", 0)
            if since_ts and cd < since_ts:
                continue
            if until_ts and cd > until_ts:
                continue
            filtered_prs.append(pr)

        pr_count = 0
        for pr in filtered_prs:
            upsert_pr(conn, repo_id, pr)
            pr_count += 1

            if not no_comments:
                delete_pr_comments(conn, repo_id, pr["id"])
                activities_url = (
                    f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}"
                    f"/pull-requests/{pr['id']}/activities"
                )
                activities = paginate(session, activities_url)
                for activity in activities:
                    if activity.get("action") != "COMMENTED":
                        continue
                    comment = activity.get("comment", {})
                    anchor = activity.get("commentAnchor")
                    if comment.get("id"):
                        walk_comment_thread(conn, repo_id, pr["id"], comment, None, anchor)

        conn.commit()
        label = f"{proj_key}/{repo_slug}"
        print(f"  [{label}] {pr_count} PRs cached", flush=True)
        return pr_count

    total_prs = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(cache_repo, proj_key, repo_slug): (proj_key, repo_slug)
            for proj_key, repo_slug in repos_to_cache
        }
        for future in as_completed(futures):
            proj_key, repo_slug = futures[future]
            try:
                total_prs += future.result()
            except Exception as exc:
                log.error("Error caching %s/%s: %s", proj_key, repo_slug, exc)

    print(f"\nDone. Total PRs cached: {total_prs}", flush=True)
    conn.close()


def fetch_all_projects(session: requests.Session, url: str) -> list[dict]:
    return paginate(session, f"{url}/rest/api/1.0/projects", limit=100)


def fetch_project_repos(session: requests.Session, url: str, project_key: str) -> list[dict]:
    return paginate(session, f"{url}/rest/api/1.0/projects/{project_key}/repos", limit=100)


def date_to_ms(date_str: str, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# ── plot command ──────────────────────────────────────────────────────────────

def cmd_plot(args: argparse.Namespace, cfg: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    repos = collect_repos_from_args(args, conn)
    if not repos:
        log.error("No repositories specified.")
        sys.exit(1)

    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", "MERGED")
    reviewer = getattr(args, "reviewer", None)
    output = getattr(args, "output", "chart.png")

    data_per_repo: dict[str, list[float]] = {}

    for proj_key, repo_slug in repos:
        repo_row = conn.execute(
            "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
        ).fetchone()
        if not repo_row:
            log.warning("Repo not in cache: %s/%s", proj_key, repo_slug)
            continue
        repo_id = repo_row["id"]

        query = """
            SELECT created_date, closed_date, reviewers
            FROM pull_requests
            WHERE repo_id=? AND state=? AND closed_date IS NOT NULL
        """
        params: list[Any] = [repo_id, state]

        if since_ts:
            query += " AND created_date >= ?"
            params.append(since_ts)
        if until_ts:
            query += " AND created_date <= ?"
            params.append(until_ts)

        rows = conn.execute(query, params).fetchall()

        if reviewer:
            mode, username = reviewer.split(":", 1)
            filtered = []
            for row in rows:
                reviewers_list = json.loads(row["reviewers"] or "[]")
                if mode == "include" and username in reviewers_list:
                    filtered.append(row)
                elif mode == "exclude" and username not in reviewers_list:
                    filtered.append(row)
            rows = filtered

        cycle_times = [
            (row["closed_date"] - row["created_date"]) / 3_600_000
            for row in rows
            if row["closed_date"] and row["created_date"]
        ]

        label = f"{proj_key}/{repo_slug}"
        if not cycle_times:
            log.warning("No data for %s in the specified range/state — skipping.", label)
            continue
        data_per_repo[label] = cycle_times

    conn.close()

    if not data_per_repo:
        log.error("No data to plot.")
        sys.exit(4)

    labels = list(data_per_repo.keys())
    values = [data_per_repo[l] for l in labels]

    fig, ax = plt.subplots(figsize=(max(8, len(labels) * 1.5), 6))
    bp = ax.boxplot(values, labels=labels, patch_artist=True)

    for patch in bp["boxes"]:
        patch.set_facecolor("#4A90D9")
        patch.set_alpha(0.7)

    for i, (label, times) in enumerate(data_per_repo.items(), 1):
        median = sorted(times)[len(times) // 2]
        ax.annotate(
            f"{median:.1f}h",
            xy=(i, median),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color="darkred",
        )

    ax.set_ylabel("Cycle Time (hours)")
    ax.set_xlabel("Repository")
    ax.set_title(f"Cycle Time Distribution ({state})")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    out_path = Path(output)
    suffix = out_path.suffix.lower()
    if suffix == ".html":
        try:
            import plotly.graph_objects as go
            fig_plotly = go.Figure()
            for label, times in data_per_repo.items():
                fig_plotly.add_trace(go.Box(y=times, name=label))
            fig_plotly.update_layout(
                yaxis_title="Cycle Time (hours)",
                xaxis_title="Repository",
                title=f"Cycle Time Distribution ({state})",
            )
            fig_plotly.write_html(str(out_path))
        except ImportError:
            log.warning("plotly not installed, saving as PNG instead.")
            out_path = out_path.with_suffix(".png")
            fig.savefig(str(out_path), dpi=150)
    else:
        fig.savefig(str(out_path), dpi=150)

    print(f"Chart saved to {out_path}", flush=True)
    plt.close(fig)


def collect_repos_from_args(args: argparse.Namespace, conn: sqlite3.Connection) -> list[tuple[str, str]]:
    repos: list[tuple[str, str]] = []

    repos_file = getattr(args, "repos_file", None)
    if repos_file:
        with open(repos_file) as f:
            for line in f:
                line = line.strip()
                if line and "/" in line:
                    proj, slug = line.split("/", 1)
                    repos.append((proj.strip(), slug.strip()))
        return repos

    repos_arg = getattr(args, "repos", None)
    if repos_arg:
        for entry in repos_arg.split(","):
            entry = entry.strip()
            if "/" in entry:
                proj, slug = entry.split("/", 1)
                repos.append((proj.strip(), slug.strip()))
        return repos

    projects_arg = getattr(args, "projects", None)
    if projects_arg:
        for proj_key in projects_arg.split(","):
            proj_key = proj_key.strip()
            rows = conn.execute(
                "SELECT slug FROM repos WHERE project_key=?", (proj_key,)
            ).fetchall()
            for row in rows:
                repos.append((proj_key, row["slug"]))
        return repos

    return repos


# ── find-repos command ────────────────────────────────────────────────────────

def cmd_find_repos(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    reviewer = args.reviewer
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)

    query = """
        SELECT DISTINCT r.project_key || '/' || r.slug AS repo
        FROM pull_requests pr
        JOIN repos r ON r.id = pr.repo_id
        WHERE EXISTS (
            SELECT 1 FROM json_each(pr.reviewers) WHERE value = ?
        )
    """
    params: list[Any] = [reviewer]

    if since_ts:
        query += " AND pr.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND pr.created_date <= ?"
        params.append(until_ts)
    if state:
        query += " AND pr.state = ?"
        params.append(state)

    query += " ORDER BY repo"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No repositories found for the given filters.", flush=True)
        sys.exit(4)

    lines = [row["repo"] for row in rows]
    output = getattr(args, "output", None)

    if output:
        Path(output).write_text("\n".join(lines) + "\n")
        print(f"{len(lines)} repositories written to {output}", flush=True)
    else:
        for line in lines:
            print(line)


# ── sql command ───────────────────────────────────────────────────────────────

def cmd_sql(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)

    query = getattr(args, "query", None)
    sql_file = getattr(args, "file", None)
    if sql_file:
        query = Path(sql_file).read_text()
    if not query:
        log.error("No query provided. Use --query or --file.")
        sys.exit(1)

    # Only allow SELECT
    normalized = query.strip().lstrip(";").strip().upper()
    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        log.error("Only SELECT queries are allowed.")
        sys.exit(5)
    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "REPLACE"):
        if forbidden in normalized:
            log.error("Modifying SQL operations are not allowed.")
            sys.exit(5)

    conn = open_db(db_path)
    limit = getattr(args, "limit", 10000)
    fmt = getattr(args, "format", "table")

    if limit and limit > 0:
        query_with_limit = f"SELECT * FROM ({query}) LIMIT {limit}"
    else:
        query_with_limit = query

    rows = conn.execute(query_with_limit).fetchall()
    conn.close()

    if not rows:
        print("No results.", flush=True)
        return

    columns = rows[0].keys()
    data = [dict(row) for row in rows]

    output = getattr(args, "output", None)
    result_text = format_output(data, columns, fmt)

    if output:
        Path(output).write_text(result_text)
        print(f"{len(data)} rows written to {output}", flush=True)
    else:
        print(result_text)


# ── status command ────────────────────────────────────────────────────────────

def cmd_status(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    db_file = Path(db_path)

    if not db_file.exists():
        print(f"Database not found: {db_path}")
        sys.exit(4)

    conn = open_db(db_path)

    projects = conn.execute("SELECT key, name, cache_date FROM projects ORDER BY key").fetchall()
    total_repos = conn.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
    total_prs = conn.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
    total_comments = conn.execute("SELECT COUNT(*) FROM pr_comments").fetchone()[0]
    total_reactions = conn.execute("SELECT COUNT(*) FROM comment_reactions").fetchone()[0]

    date_range = conn.execute(
        "SELECT MIN(created_date), MAX(created_date) FROM pull_requests"
    ).fetchone()

    conn.close()

    db_size = db_file.stat().st_size / (1024 * 1024)

    print(f"Database: {db_path}  ({db_size:.1f} MB)")
    print(f"Projects: {len(projects)}  Repos: {total_repos}  PRs: {total_prs}")
    print(f"Comments: {total_comments}  Reactions: {total_reactions}")

    if date_range[0] and date_range[1]:
        d_from = ms_to_date(date_range[0])
        d_to = ms_to_date(date_range[1])
        print(f"PR date range: {d_from} — {d_to}")

    if projects:
        print("\nProjects:")
        for p in projects:
            repos_count = 0
            print(f"  {p['key']} ({p['name']})  cached: {p['cache_date'] or 'unknown'}")
    else:
        print("\nNo projects cached yet.")


def ms_to_date(ms: Optional[int]) -> str:
    if ms is None:
        return "N/A"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


# ── review-feedback command ───────────────────────────────────────────────────

def cmd_review_feedback(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)
    conn = open_db(db_path)

    author = args.author
    since_ts = date_to_ms(args.since) if args.since else None
    until_ts = date_to_ms(args.until, end_of_day=True) if args.until else None
    state = getattr(args, "state", None)
    min_reactions = getattr(args, "min_reactions", 0)
    fmt = getattr(args, "format", "table")
    output = getattr(args, "output", None)

    # Build repo filter
    repo_ids: Optional[list[int]] = None
    repos = collect_repos_from_args(args, conn)
    if repos:
        repo_ids = []
        for proj_key, repo_slug in repos:
            row = conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
            ).fetchone()
            if row:
                repo_ids.append(row["id"])

    # Query root comments by author
    query = """
        SELECT
            c.id AS comment_id,
            c.repo_id,
            c.pr_id,
            c.created_date,
            c.file_path,
            c.line,
            c.severity,
            c.text AS comment_text,
            pr.title AS pr_title,
            pr.state AS pr_state,
            r.project_key || '/' || r.slug AS repo
        FROM pr_comments c
        JOIN pull_requests pr ON pr.repo_id = c.repo_id AND pr.pr_id = c.pr_id
        JOIN repos r ON r.id = c.repo_id
        WHERE c.author = ? AND c.parent_id IS NULL
    """
    params: list[Any] = [author]

    if since_ts:
        query += " AND c.created_date >= ?"
        params.append(since_ts)
    if until_ts:
        query += " AND c.created_date <= ?"
        params.append(until_ts)
    if state:
        query += " AND pr.state = ?"
        params.append(state)
    if repo_ids is not None:
        if not repo_ids:
            print("Нет данных", flush=True)
            conn.close()
            return
        placeholders = ",".join("?" * len(repo_ids))
        query += f" AND c.repo_id IN ({placeholders})"
        params.extend(repo_ids)

    query += " ORDER BY c.created_date DESC"

    comment_rows = conn.execute(query, params).fetchall()

    if not comment_rows:
        print("Нет данных", flush=True)
        conn.close()
        return

    results = []
    for row in comment_rows:
        comment_id = row["comment_id"]

        # Fetch reactions
        reaction_rows = conn.execute(
            "SELECT emoji, author FROM comment_reactions WHERE comment_id=?",
            (comment_id,),
        ).fetchall()

        reactions_pos = 0
        reactions_neg = 0
        reactions_other = 0
        reactions_detail: dict[str, int] = {}

        for r in reaction_rows:
            emoji = r["emoji"]
            reactions_detail[emoji] = reactions_detail.get(emoji, 0) + 1
            if emoji in POSITIVE_EMOJIS:
                reactions_pos += 1
            elif emoji in NEGATIVE_EMOJIS:
                reactions_neg += 1
            else:
                reactions_other += 1

        total_reactions = reactions_pos + reactions_neg + reactions_other
        if min_reactions > 0 and total_reactions < min_reactions:
            continue

        # Fetch replies (children by non-author)
        reply_rows = conn.execute(
            """SELECT author, text, created_date FROM pr_comments
               WHERE parent_id=? AND author != ?
               ORDER BY created_date""",
            (comment_id, author),
        ).fetchall()

        replies = [
            {
                "author": r["author"],
                "text": r["text"],
                "created_date": r["created_date"],
            }
            for r in reply_rows
        ]

        results.append({
            "repo": row["repo"],
            "pr_id": row["pr_id"],
            "pr_title": row["pr_title"],
            "comment_id": comment_id,
            "created_date": ms_to_date(row["created_date"]),
            "file_path": row["file_path"],
            "line_from": row["line"],
            "line_to": row["line"],
            "severity": row["severity"],
            "comment_text": row["comment_text"],
            "reactions_positive": reactions_pos,
            "reactions_negative": reactions_neg,
            "reactions_other": reactions_other,
            "reactions_detail": reactions_detail,
            "replies_count": len(replies),
            "replies": replies,
        })

    conn.close()

    if not results:
        print("Нет данных", flush=True)
        return

    if fmt == "json":
        result_text = json.dumps(results, ensure_ascii=False, indent=2)
    else:
        # Flatten for table/csv output
        flat_results = []
        for r in results:
            flat = dict(r)
            flat["reactions_detail"] = json.dumps(r["reactions_detail"], ensure_ascii=False)
            flat["replies"] = json.dumps(r["replies"], ensure_ascii=False)
            flat_results.append(flat)

        columns = [
            "repo", "pr_id", "pr_title", "comment_id", "created_date",
            "file_path", "line_from", "line_to", "severity", "comment_text",
            "reactions_positive", "reactions_negative", "reactions_other",
            "reactions_detail", "replies_count", "replies",
        ]
        result_text = format_output(flat_results, columns, fmt)

    output_path = getattr(args, "output", None)
    if output_path:
        Path(output_path).write_text(result_text, encoding="utf-8")
        print(f"{len(results)} comments written to {output_path}", flush=True)
    else:
        print(result_text)


# ── output formatting ─────────────────────────────────────────────────────────

def format_output(data: list[dict], columns: list, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(data, ensure_ascii=False, indent=2)

    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
        return buf.getvalue()

    # table (default)
    try:
        from tabulate import tabulate
        rows = [[row.get(col, "") for col in columns] for row in data]
        return tabulate(rows, headers=list(columns), tablefmt="simple")
    except ImportError:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(columns), extrasaction="ignore", delimiter="\t")
        writer.writeheader()
        writer.writerows(data)
        return buf.getvalue()


# ── CLI argument parser ───────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pr_analytics.py",
        description="Bitbucket Server PR analytics tool",
    )
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── cache ──────────────────────────────────────────────────────────────────
    p_cache = subparsers.add_parser("cache", help="Fetch and cache PRs from Bitbucket Server")
    p_cache.add_argument("--token", help="Personal Access Token")
    p_cache.add_argument("--url", help="Bitbucket Server base URL")
    p_cache.add_argument("--since", help="Cache PRs from this date (YYYY-MM-DD)")
    p_cache.add_argument("--until", help="Cache PRs until this date (YYYY-MM-DD)")
    p_cache.add_argument("--projects", help="Comma-separated project keys")
    p_cache.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p_cache.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                         help=f"Parallel threads (default: {DEFAULT_CONCURRENCY})")
    p_cache.add_argument("--no-comments", action="store_true", dest="no_comments",
                         help="Skip loading comments and reactions")
    p_cache.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── plot ───────────────────────────────────────────────────────────────────
    p_plot = subparsers.add_parser("plot", help="Plot Cycle Time boxplot")
    p_plot.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p_plot.add_argument("--projects", help="Comma-separated project keys")
    p_plot.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p_plot.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p_plot.add_argument("--until", help="End date (YYYY-MM-DD)")
    p_plot.add_argument("--state", default="MERGED",
                        choices=["MERGED", "DECLINED", "OPEN"],
                        help="PR state filter (default: MERGED)")
    p_plot.add_argument("--reviewer", help="include:<slug> or exclude:<slug>")
    p_plot.add_argument("--output", default="chart.png", help="Output file (.png/.svg/.html)")
    p_plot.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── find-repos ─────────────────────────────────────────────────────────────
    p_find = subparsers.add_parser("find-repos", help="Find repos where user was a reviewer")
    p_find.add_argument("--reviewer", required=True, help="Reviewer slug")
    p_find.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p_find.add_argument("--until", help="End date (YYYY-MM-DD)")
    p_find.add_argument("--state", choices=["MERGED", "DECLINED", "OPEN"],
                        help="PR state filter")
    p_find.add_argument("--output", help="Output file path")
    p_find.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── sql ────────────────────────────────────────────────────────────────────
    p_sql = subparsers.add_parser("sql", help="Run arbitrary SELECT query")
    p_sql.add_argument("--query", help="SQL SELECT query string")
    p_sql.add_argument("--file", help="Path to .sql file")
    p_sql.add_argument("--output", help="Output file path")
    p_sql.add_argument("--format", default="table", choices=["table", "csv", "json"])
    p_sql.add_argument("--limit", type=int, default=10000,
                       help="Row limit (0 = unlimited, default: 10000)")
    p_sql.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── status ─────────────────────────────────────────────────────────────────
    p_status = subparsers.add_parser("status", help="Show cache status")
    p_status.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    # ── review-feedback ────────────────────────────────────────────────────────
    p_rf = subparsers.add_parser("review-feedback",
                                 help="Export AI-agent comments with reactions and replies")
    p_rf.add_argument("--author", required=True, help="Author slug (AI agent)")
    p_rf.add_argument("--since", help="Start date (YYYY-MM-DD)")
    p_rf.add_argument("--until", help="End date (YYYY-MM-DD)")
    p_rf.add_argument("--repos", help="Comma-separated PROJ/repo entries")
    p_rf.add_argument("--projects", help="Comma-separated project keys")
    p_rf.add_argument("--repos-file", dest="repos_file", help="File with one PROJ/repo per line")
    p_rf.add_argument("--state", choices=["MERGED", "DECLINED", "OPEN"],
                      help="PR state filter")
    p_rf.add_argument("--min-reactions", type=int, default=0, dest="min_reactions",
                      help="Minimum total reactions to include (default: 0)")
    p_rf.add_argument("--output", help="Output file path")
    p_rf.add_argument("--format", default="table", choices=["table", "csv", "json"])
    p_rf.add_argument("--db", help=f"SQLite DB path (default: {DEFAULT_DB})")

    return parser


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    cfg = load_config()

    commands = {
        "cache": cmd_cache,
        "plot": cmd_plot,
        "find-repos": cmd_find_repos,
        "sql": cmd_sql,
        "status": cmd_status,
        "review-feedback": cmd_review_feedback,
    }

    fn = commands.get(args.command)
    if fn:
        fn(args, cfg)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
