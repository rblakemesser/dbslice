#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List
import psycopg

from .dbutil import ensure_schemas, create_schema_only_table, full_copy_table


def migrate_precopy(conn, cfg: Dict[str, object]) -> Dict[str, List[str]]:
    """Run the precopy phase per config: create schema-only and full-copy tables.

    Returns a summary dict with created lists.
    """
    source_schema = str(cfg.get('source_schema'))
    dest_schema = str(cfg.get('dest_schema'))

    ensure_schemas(conn, [dest_schema])

    created_schema_only: List[str] = []
    created_full_copy: List[str] = []
    precopy = cfg.get('precopy') or {}
    so_list: List[str] = list((precopy or {}).get('schema_only') or [])  # type: ignore[assignment]
    fc_list: List[str] = list((precopy or {}).get('full_copy') or [])    # type: ignore[assignment]

    for tbl in so_list:
        try:
            create_schema_only_table(conn, source_schema=source_schema, dest_schema=dest_schema, table=tbl)
            created_schema_only.append(tbl)
        except Exception:
            conn.rollback()
            raise

    for tbl in fc_list:
        try:
            full_copy_table(conn, source_schema=source_schema, dest_schema=dest_schema, table=tbl)
            created_full_copy.append(tbl)
        except Exception:
            conn.rollback()
            raise

    return {"schema_only": created_schema_only, "full_copy": created_full_copy}

