#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict
import re

from .introspect import table_exists, list_tables_in_schema


def _fetch_triggers_defs(cur, schema: str, table: str) -> Dict[str, str]:
    cur.execute(
        """
        SELECT t.tgname, pg_get_triggerdef(t.oid, true) AS tgdef
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
        ORDER BY t.tgname
        """,
        (schema, table),
    )
    return {str(name): str(defn) for name, defn in (cur.fetchall() or [])}


def _prepare_triggerdef_for_dst(tgdef: str, *, src_schema: str, dst_schema: str, table: str) -> str:
    out = tgdef
    out = re.sub(
        r"\bON\b[\s\S]*?\bFOR\s+EACH\b",
        f'ON "{dst_schema}"."{table}" FOR EACH',
        out,
        count=1,
        flags=re.IGNORECASE,
    )
    out = re.sub(rf"EXECUTE\s+FUNCTION\s+{re.escape(src_schema)}\.", f'EXECUTE FUNCTION {dst_schema}.', out, flags=re.IGNORECASE)
    if not re.search(r"EXECUTE\s+FUNCTION\s+(?:\"?[A-Za-z_][\w$]*\"?)\s*\.\s*(\"?[A-Za-z_][\w$]*\"?)", out, flags=re.IGNORECASE):
        out = re.sub(r"EXECUTE\s+FUNCTION\s+(\"?[A-Za-z_][\w$]*\"?)\s*\(", f'EXECUTE FUNCTION {dst_schema}.\\1(', out, count=1, flags=re.IGNORECASE)
    return out


def reconcile_table_triggers(
    conn,
    table: str,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
) -> Dict[str, int]:
    if not table_exists(conn, src_schema, table) or not table_exists(conn, dst_schema, table):
        return {"created": 0, "dropped": 0}
    created = 0
    dropped = 0
    with conn.cursor() as cur:
        src_tr = _fetch_triggers_defs(cur, src_schema, table)
        dst_tr = _fetch_triggers_defs(cur, dst_schema, table)

    for name, tgdef in src_tr.items():
        if name not in dst_tr:
            stmt = _prepare_triggerdef_for_dst(tgdef, src_schema=src_schema, dst_schema=dst_schema, table=table)
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
                created += 1
            except Exception:
                conn.rollback()
                continue

    for name in dst_tr.keys() - src_tr.keys():
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TRIGGER IF EXISTS "{name}" ON "{dst_schema}"."{table}" CASCADE')
            conn.commit()
            dropped += 1
        except Exception:
            conn.rollback()
            continue

    return {"created": created, "dropped": dropped}


def reconcile_all_triggers(
    conn,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
) -> Dict[str, int]:
    totals = {"created": 0, "dropped": 0}
    for tbl in list_tables_in_schema(conn, src_schema):
        if not table_exists(conn, dst_schema, tbl):
            continue
        res = reconcile_table_triggers(conn, tbl, src_schema=src_schema, dst_schema=dst_schema)
        totals["created"] += res.get("created", 0)
        totals["dropped"] += res.get("dropped", 0)
    return totals

