from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def date_to_ms(date_str: str, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_date(ms: Optional[int]) -> str:
    if ms is None:
        return "N/A"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def format_output(data: list[dict], columns: list, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(data, ensure_ascii=False, indent=2)

    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
        return buf.getvalue()

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


def collect_repos_from_args(
    args: argparse.Namespace, conn: sqlite3.Connection
) -> list[tuple[str, str]]:
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
