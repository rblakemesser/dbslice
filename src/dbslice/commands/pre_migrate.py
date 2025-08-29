#!/usr/bin/env python3
from __future__ import annotations

"""Pre-migrate command helper.

Truncates configured tables with CASCADE and executes arbitrary SQL statements
against the destination schema. Designed to run before any migration work.

Inputs
- conn: psycopg connection
- cfg: normalized config dict with 'pre_migrate' section

Outputs
- Dict with {result, truncated: [..], skipped_missing: [..], sql_executed: int}

Failure policy
- Raise on preconditions failure; do not suppress errors.
"""

from typing import Dict, List

from ..dbutil import table_exists


def run_pre_migrate(conn, cfg: Dict[str, object]) -> Dict[str, object]:
    dest_schema = cfg.get('dest_schema')
    if not isinstance(dest_schema, str) or not dest_schema:
        raise ValueError('dest_schema must be set in config for pre_migrate')

    pm = cfg.get('pre_migrate') or {}
    if not isinstance(pm, dict):
        pm = {}

    trunc_list = pm.get('truncate') or []
    sql_list = pm.get('sql') or []

    truncated: List[str] = []
    skipped: List[str] = []

    # Truncate tables with CASCADE. Accept fully-qualified or bare names.
    for item in list(trunc_list):
        if not isinstance(item, str) or not item.strip():
            continue
        item = item.strip()
        if '.' in item:
            sch, tbl = item.split('.', 1)
            sch = sch.strip('"')
            tbl = tbl.strip('"')
        else:
            sch, tbl = dest_schema, item
        if table_exists(conn, sch, tbl):
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{sch}"."{tbl}" CASCADE')
            conn.commit()
            truncated.append(f'{sch}.{tbl}')
        else:
            skipped.append(f'{sch}.{tbl}')

    # Execute provided SQL statements as-is, one per entry
    executed = 0
    for stmt in list(sql_list):
        if not isinstance(stmt, str) or not stmt.strip():
            continue
        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.commit()
        executed += 1

    return {
        "result": "pre_migrate_done",
        "truncated": truncated,
        "skipped_missing": skipped,
        "sql_executed": executed,
    }

