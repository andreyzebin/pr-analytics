from __future__ import annotations

import logging
import sys
import time

import requests

from pa.config import MAX_RETRIES, REQUEST_TIMEOUT, resolve_ca_bundle, resolve_client_cert

log = logging.getLogger(__name__)


def make_session(token: str, cfg: dict) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    ca_bundle = resolve_ca_bundle(cfg)
    client_cert = resolve_client_cert(cfg)
    if ca_bundle:
        session.verify = ca_bundle
    if client_cert:
        session.cert = client_cert
    return session


def api_get(session: requests.Session, url: str, allow_404: bool = False) -> dict | None:
    log.debug("GET %s", url)
    t0 = time.monotonic()
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            log.debug("  → %d  %.2fs", resp.status_code, time.monotonic() - t0)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                log.warning("Rate limited, waiting %ds", retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code in (401, 403):
                log.error("Authentication error %d: %s", resp.status_code, url)
                sys.exit(2)

            if allow_404 and resp.status_code == 404:
                return None

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


def api_get_text(session: requests.Session, url: str) -> str | None:
    """GET request returning raw text (for diffs). Returns None on 404."""
    log.debug("GET (text) %s", url)
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT,
                               headers={"Accept": "text/plain"})
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                log.warning("Rate limited, waiting %ds", retry_after)
                time.sleep(retry_after)
                continue
            if resp.status_code in (401, 403):
                log.error("Authentication error %d: %s", resp.status_code, url)
                sys.exit(2)
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                log.error("Timeout after %d retries: %s", MAX_RETRIES, url)
                return None
        except requests.exceptions.ConnectionError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                log.error("Connection failed: %s", e)
                return None
    return None


def paginate(session: requests.Session, url: str, limit: int = 25) -> list:
    results = []
    start = 0
    base_url = url + ("&" if "?" in url else "?")
    while True:
        data = api_get(session, f"{base_url}start={start}&limit={limit}")
        results.extend(data.get("values", []))
        if data.get("isLastPage", True):
            break
        start = data.get("nextPageStart", start + limit)
    return results


def fetch_all_projects(session: requests.Session, url: str) -> list[dict]:
    return paginate(session, f"{url}/rest/api/1.0/projects", limit=100)


def fetch_project_repos(session: requests.Session, url: str, project_key: str) -> list[dict]:
    return paginate(session, f"{url}/rest/api/1.0/projects/{project_key}/repos", limit=100)
