#!/usr/bin/env python3
from __future__ import annotations

from .ddl import ensure_schemas


def migrate_functions(conn, *, src_schema: str = 'public', dst_schema: str = 'stage') -> dict:
    migrated = 0
    ensure_schemas(conn, [dst_schema])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.oid, p.proname, pg_get_functiondef(p.oid) AS fndef
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s
            """,
            (src_schema,),
        )
        rows = cur.fetchall() or []
    for _oid, _name, fndef in rows:
        if not fndef:
            continue
        fn = str(fndef)
        fn = fn.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
        fn = fn.replace(f"{src_schema}.", f"{dst_schema}.")
        try:
            with conn.cursor() as cur:
                cur.execute(fn)
            conn.commit()
            migrated += 1
        except Exception:
            conn.rollback()
            continue
    return {"migrated": migrated}

