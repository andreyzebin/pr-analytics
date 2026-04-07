from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pa.api import api_get, fetch_all_projects, fetch_project_repos, make_session, paginate
from pa.config import DEFAULT_CONCURRENCY, resolve_db, resolve_token, resolve_url, _cache_cfg
from pa.db import (
    delete_pr_comments, open_db, upsert_pr, upsert_project, upsert_repo, walk_comment_thread,
)
from pa.utils import date_to_ms

log = logging.getLogger(__name__)


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m{s % 60:02d}s"


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

    # ── discover repos ─────────────────────────────────────────────────────────
    repos_to_cache: list[tuple[str, str]] = []

    if args.repos:
        for entry in args.repos.split(","):
            entry = entry.strip()
            if "/" in entry:
                proj, slug = entry.split("/", 1)
                repos_to_cache.append((proj.strip(), slug.strip()))
            else:
                log.warning("Invalid repo spec (expected PROJ/repo): %s", entry)
    elif args.projects:
        temp_conn = open_db(db_path)
        for proj_key in [p.strip() for p in args.projects.split(",")]:
            print(f"  Project: {proj_key}", flush=True)
            proj_repos = fetch_project_repos(session, url, proj_key)
            upsert_project(temp_conn, proj_key, proj_key)
            for repo in proj_repos:
                upsert_repo(temp_conn, repo["id"], proj_key, repo["slug"], repo["name"])
                repos_to_cache.append((proj_key, repo["slug"]))
            temp_conn.commit()
        temp_conn.close()
    else:
        temp_conn = open_db(db_path)
        for proj in fetch_all_projects(session, url):
            print(f"  Project: {proj['key']}", flush=True)
            upsert_project(temp_conn, proj["key"], proj["name"])
            for repo in fetch_project_repos(session, url, proj["key"]):
                upsert_repo(temp_conn, repo["id"], proj["key"], repo["slug"], repo["name"])
                repos_to_cache.append((proj["key"], repo["slug"]))
        temp_conn.commit()
        temp_conn.close()

    if not repos_to_cache:
        log.warning("No repositories found to cache.")
        return

    n_total = len(repos_to_cache)
    print(f"Caching {n_total} repositories with concurrency={concurrency}\n", flush=True)

    # ── progress tracking ──────────────────────────────────────────────────────
    _lock = threading.Lock()
    _active: dict[str, tuple[float, str]] = {}
    _done_times: list[float] = []
    _global_start = time.monotonic()

    def _tprint(msg: str) -> None:
        with _lock:
            print(msg, flush=True)

    def _set_status(label: str, status: str) -> None:
        with _lock:
            if label in _active:
                _active[label] = (_active[label][0], status)

    def _repo_start(label: str) -> None:
        with _lock:
            _active[label] = (time.monotonic(), "starting…")
        _tprint(f"  → {label}")

    def _repo_done(label: str, pr_count: int, comment_count: int) -> None:
        with _lock:
            t0 = _active.pop(label, (time.monotonic(), ""))[0]
            elapsed = time.monotonic() - t0
            _done_times.append(elapsed)
            n_done = len(_done_times)
            n_remaining = n_total - n_done
            avg = sum(_done_times) / n_done
        eta = avg * n_remaining / max(concurrency, 1)
        elapsed_total = time.monotonic() - _global_start
        suffix = (
            f"~{_fmt_elapsed(eta)} remaining"
            if n_remaining > 0
            else f"all done in {_fmt_elapsed(elapsed_total)}"
        )
        _tprint(
            f"  ✓ {label}  {pr_count} PRs, {comment_count} comments"
            f"  [{_fmt_elapsed(elapsed)}]"
            f"  ({n_done}/{n_total}, {suffix})"
        )

    def _repo_error(label: str, exc: Exception) -> None:
        with _lock:
            _active.pop(label, None)
        _tprint(f"  ✗ {label}  ERROR: {exc}")

    _stop_hb = threading.Event()

    def _heartbeat() -> None:
        while not _stop_hb.wait(timeout=8):
            with _lock:
                if not _active:
                    continue
                now = time.monotonic()
                parts = [
                    f"{lbl} ({_fmt_elapsed(now - t0)}, {st})"
                    for lbl, (t0, st) in _active.items()
                ]
            if parts:
                elapsed_total = time.monotonic() - _global_start
                print(f"  ⟳ [{_fmt_elapsed(elapsed_total)}]  " + "  |  ".join(parts), flush=True)

    hb = threading.Thread(target=_heartbeat, daemon=True)
    hb.start()

    # ── per-repo worker ────────────────────────────────────────────────────────

    def cache_repo(proj_key: str, repo_slug: str) -> int:
        label = f"{proj_key}/{repo_slug}"
        _repo_start(label)
        thread_conn = open_db(db_path)
        try:
            repo_row = thread_conn.execute(
                "SELECT id FROM repos WHERE project_key=? AND slug=?", (proj_key, repo_slug)
            ).fetchone()
            if not repo_row:
                repo_data = api_get(
                    session, f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}"
                )
                upsert_project(thread_conn, proj_key, proj_key)
                upsert_repo(thread_conn, repo_data["id"], proj_key, repo_data["slug"], repo_data["name"])
                thread_conn.commit()
                repo_id = repo_data["id"]
            else:
                repo_id = repo_row["id"]

            _set_status(label, "fetching PRs…")
            prs = paginate(session, f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}/pull-requests?state=ALL")
            filtered_prs = [
                pr for pr in prs
                if (not since_ts or pr.get("createdDate", 0) >= since_ts)
                and (not until_ts or pr.get("createdDate", 0) <= until_ts)
            ]
            n_prs = len(filtered_prs)
            _set_status(label, f"{n_prs} PRs found, saving…")

            comment_count = 0
            for i, pr in enumerate(filtered_prs):
                upsert_pr(thread_conn, repo_id, pr)

                if not no_comments:
                    _set_status(label, f"comments PR {i + 1}/{n_prs}…")
                    delete_pr_comments(thread_conn, repo_id, pr["id"])
                    activities = paginate(
                        session,
                        f"{url}/rest/api/1.0/projects/{proj_key}/repos/{repo_slug}"
                        f"/pull-requests/{pr['id']}/activities",
                    )
                    for activity in activities:
                        if activity.get("action") != "COMMENTED":
                            continue
                        comment = activity.get("comment", {})
                        anchor = activity.get("commentAnchor")
                        if comment.get("id"):
                            walk_comment_thread(thread_conn, repo_id, pr["id"], comment, None, anchor)
                            comment_count += 1 + len(comment.get("comments", []))

                thread_conn.commit()  # release write lock after each PR
            _repo_done(label, n_prs, comment_count)
            return n_prs

        except Exception as exc:
            _repo_error(label, exc)
            raise
        finally:
            thread_conn.close()

    # ── run ────────────────────────────────────────────────────────────────────
    total_prs = 0
    interrupted = False
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(cache_repo, proj_key, repo_slug): (proj_key, repo_slug)
                for proj_key, repo_slug in repos_to_cache
            }
            for future in as_completed(futures):
                proj_key, repo_slug = futures[future]
                try:
                    total_prs += future.result()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    log.debug("cache_repo %s/%s raised: %s", proj_key, repo_slug, exc)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        _stop_hb.set()
        hb.join(timeout=1)

    elapsed_total = time.monotonic() - _global_start
    if interrupted:
        print(f"\nInterrupted after {_fmt_elapsed(elapsed_total)}. Partial results saved to {db_path}", flush=True)
    else:
        print(f"\nDone. {total_prs} PRs cached in {_fmt_elapsed(elapsed_total)}.  DB: {db_path}", flush=True)
