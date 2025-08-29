#!/usr/bin/env python3
from __future__ import annotations

"""Reset command helpers.

Drops dest/tmp tables and shard artifacts for specific table_groups without altering others.

Inputs
- conn: psycopg connection
- cfg: normalized config dict (used for schema names and family definitions)
- requested: list of table_group names to reset

Outputs
- Dict with {result, table_groups}

Failure policy
- Raise if a table_group name does not exist (validation occurs in CLI before calling).
"""

from typing import List

from ..dbutil import list_relations_like, drop_tables_if_exists


def run_reset(conn, cfg: dict, requested: List[str]) -> dict:
    dst = str(cfg.get('dest_schema'))
    tmp = str(cfg.get('tmp_schema', 'tmp'))
    shards = str(cfg.get('shards_schema', 'shards'))
    fams = cfg.get('table_groups') or []
    defined = {str(f.get('name')): f for f in fams if f.get('name')}
    for name in requested:
        fam = defined[name]
        root = str((fam.get('root') or {}).get('table'))
        dep_tables = [str(d.get('table')) for d in (fam.get('deps') or [])]
        tables = [t for t in [root] + dep_tables if t]
        drop_tables_if_exists(conn, [f"{dst}.\"{t}\"" for t in tables])
        drop_tables_if_exists(conn, [f"{tmp}.\"{t}\"" for t in tables])
        shard_rels: list[str] = []
        for t in tables:
            for pat in (f"{t}_sh%", f"{t}_pmsh%"):
                shard_rels.extend(list_relations_like(conn, shards, pat))
        drop_tables_if_exists(conn, [f"{shards}.\"{r}\"" for r in shard_rels])
    return {"result": "reset", "table_groups": requested}
