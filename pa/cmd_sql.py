from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pa.config import resolve_db
from pa.db import open_db
from pa.utils import format_output

log = logging.getLogger(__name__)


def cmd_sql(args: argparse.Namespace, cfg: dict) -> None:
    db_path = resolve_db(getattr(args, "db", None), cfg)

    query = getattr(args, "query", None)
    sql_file = getattr(args, "file", None)
    if sql_file:
        query = Path(sql_file).read_text()
    if not query:
        log.error("No query provided. Use --query or --file.")
        sys.exit(1)

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

    query_with_limit = f"SELECT * FROM ({query}) LIMIT {limit}" if limit and limit > 0 else query
    rows = conn.execute(query_with_limit).fetchall()
    conn.close()

    if not rows:
        print("No results.", flush=True)
        return

    columns = rows[0].keys()
    data = [dict(row) for row in rows]
    result_text = format_output(data, columns, fmt)

    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(result_text)
        print(f"{len(data)} rows written to {output}", flush=True)
    else:
        print(result_text)
