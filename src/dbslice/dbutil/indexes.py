#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict
import re

from .introspect import table_exists, list_tables_in_schema


def recreate_regular_indexes(conn, dest_schema: str, dest_table: str, *, source_schema: str, source_table: str | None = None) -> None:
    if source_table is None:
        source_table = dest_table
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            """,
            (source_schema, source_table),
        )
        rows = cur.fetchall() or []

    def _prepare(defn: str) -> str:
        out = defn
        out = re.sub(rf"\bON\s+{re.escape(source_schema)}\.{re.escape(source_table)}\b", f"ON {dest_schema}.{dest_table}", out)
        out = re.sub(rf"\bON\s+\"{re.escape(source_schema)}\"\.\"{re.escape(source_table)}\"\b", f'ON "{dest_schema}"."{dest_table}"', out)
        if out.startswith("CREATE UNIQUE INDEX "):
            out = out.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
        elif out.startswith("CREATE INDEX "):
            out = out.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
        return out

    for name, defn in rows:
        if name.endswith('_pkey'):
            continue
        stmt = _prepare(defn)
        with conn.cursor() as cur2:
            cur2.execute(stmt)
        conn.commit()


def _fetch_indexes(cur, schema: str, table: str) -> Dict[str, str]:
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname
        """,
        (schema, table),
    )
    return {str(name): str(defn) for name, defn in (cur.fetchall() or [])}


def _prepare_indexdef_for_dst(idxdef: str, *, src_schema: str, dst_schema: str, table: str) -> str:
    out = idxdef
    out = re.sub(
        rf"\bON\s+{re.escape(src_schema)}\.{re.escape(table)}\b",
        f'ON {dst_schema}."{table}"',
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        rf"\bON\s+\"{re.escape(src_schema)}\"\s*\.\s*\"{re.escape(table)}\"\b",
        f'ON "{dst_schema}"."{table}"',
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"\bON\s+(\"?[A-Za-z_][\w$]*\"?)\s*\.\s*(\"?[A-Za-z_][\w$]*\"?)",
        f'ON "{dst_schema}"."{table}"',
        out,
        count=1,
        flags=re.IGNORECASE,
    )
    if out.startswith("CREATE UNIQUE INDEX "):
        out = out.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
    elif out.startswith("CREATE INDEX "):
        out = out.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
    return out


def reconcile_table_indexes(
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
        src_idx = _fetch_indexes(cur, src_schema, table)
        dst_idx = _fetch_indexes(cur, dst_schema, table)

    src_idx = {k: v for k, v in src_idx.items() if not k.endswith('_pkey')}
    dst_idx = {k: v for k, v in dst_idx.items() if not k.endswith('_pkey')}

    for name, idxdef in src_idx.items():
        if name not in dst_idx:
            stmt = _prepare_indexdef_for_dst(idxdef, src_schema=src_schema, dst_schema=dst_schema, table=table)
            stmt = stmt.replace(" IF NOT EXISTS", "")
            stmt = re.sub(r'^(CREATE\s+UNIQUE\s+INDEX\s+)(\S+)', r'\1"' + name + '"', stmt)
            stmt = re.sub(r'^(CREATE\s+INDEX\s+)(\S+)', r'\1"' + name + '"', stmt)
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
                created += 1
            except Exception:
                conn.rollback()

    def _norm_for_compare(defn: str) -> str:
        d = re.sub(r"\s+", " ", defn.strip())
        d = d.replace(" IF NOT EXISTS", "")
        return d

    for name in set(src_idx.keys()) & set(dst_idx.keys()):
        src_def_prepared = _prepare_indexdef_for_dst(src_idx[name], src_schema=src_schema, dst_schema=dst_schema, table=table)
        src_norm = _norm_for_compare(src_def_prepared)
        dst_norm = _norm_for_compare(dst_idx[name])
        if src_norm != dst_norm:
            try:
                with conn.cursor() as cur:
                    cur.execute(f'DROP INDEX IF EXISTS "{dst_schema}"."{name}"')
                conn.commit()
            except Exception:
                conn.rollback()
            stmt = _prepare_indexdef_for_dst(src_idx[name], src_schema=src_schema, dst_schema=dst_schema, table=table)
            stmt = stmt.replace(" IF NOT EXISTS", "")
            stmt = re.sub(r'^(CREATE\s+UNIQUE\s+INDEX\s+)(\S+)', r'\1"' + name + '"', stmt)
            stmt = re.sub(r'^(CREATE\s+INDEX\s+)(\S+)', r'\1"' + name + '"', stmt)
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
                created += 1
            except Exception:
                conn.rollback()

    for name in dst_idx.keys() - src_idx.keys():
        if name.endswith('_pkey'):
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP INDEX IF EXISTS "{dst_schema}"."{name}"')
            conn.commit()
            dropped += 1
        except Exception:
            conn.rollback()

    return {"created": created, "dropped": dropped}


def reconcile_all_indexes(
    conn,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
) -> Dict[str, int]:
    totals = {"created": 0, "dropped": 0}
    for tbl in list_tables_in_schema(conn, src_schema):
        if not table_exists(conn, dst_schema, tbl):
            continue
        res = reconcile_table_indexes(conn, tbl, src_schema=src_schema, dst_schema=dst_schema)
        totals["created"] += res.get("created", 0)
        totals["dropped"] += res.get("dropped", 0)
    return totals

