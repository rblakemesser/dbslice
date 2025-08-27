#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple
import re

import psycopg


def ensure_schemas(conn, schemas: Sequence[str]) -> None:
    if not schemas:
        return
    with conn.cursor() as cur:
        for s in schemas:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    conn.commit()


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def get_primary_key(conn, schema: str, table: str) -> Tuple[str, List[str]] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall() or []
    if not rows:
        return None
    cname = rows[0][0]
    cols = [r[1] for r in rows]
    return cname, cols


def has_primary_key(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_type = 'PRIMARY KEY'
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def add_primary_key(conn, schema: str, table: str, columns: Sequence[str], constraint_name: str | None = None) -> None:
    if not columns:
        return
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    cname = constraint_name or f"{table}_pkey"
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{cname}" PRIMARY KEY ({cols_sql})')
    conn.commit()


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


def move_to_schema(conn, qualified_table: str, target_schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {qualified_table} SET SCHEMA {target_schema}")
    conn.commit()


def analyze_table(conn, qualified_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"ANALYZE {qualified_table}")
    conn.commit()


def set_logged(conn, qualified_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {qualified_table} SET LOGGED")
    conn.commit()


def drop_table_if_exists(conn, qualified_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {qualified_table} CASCADE")
    conn.commit()


def list_relations_like(conn, schema: str, like_pattern: str) -> List[str]:
    """Return relation names in schema where relname LIKE pattern (any relkind)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname LIKE %s
            ORDER BY c.relname
            """,
            (schema, like_pattern),
        )
        rows = cur.fetchall() or []
    return [str(r[0]) for r in rows]


def drop_tables_if_exists(conn, qualified_tables: List[str]) -> None:
    tables = [t for t in qualified_tables if t]
    if not tables:
        return
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(tables) + " CASCADE")
    conn.commit()


def create_schema_only_table(conn, *, source_schema: str, dest_schema: str, table: str) -> None:
    if table_exists(conn, dest_schema, table):
        return
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
    # PK
    pk = get_primary_key(conn, source_schema, table)
    if pk:
        cname, cols = pk
        try:
            add_primary_key(conn, dest_schema, table, cols, cname)
        except Exception:
            conn.rollback()
    # Indexes
    try:
        recreate_regular_indexes(conn, dest_schema, table, source_schema=source_schema, source_table=table)
    except Exception:
        conn.rollback()


def full_copy_table(conn, *, source_schema: str, dest_schema: str, table: str) -> None:
    if table_exists(conn, dest_schema, table):
        return
    with conn.cursor() as cur:
        cur.execute(f"CREATE UNLOGGED TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
        cur.execute(f"INSERT INTO {dest_schema}.{table} SELECT * FROM {source_schema}.{table}")
    conn.commit()
    # PK + indexes
    pk = get_primary_key(conn, source_schema, table)
    if pk:
        cname, cols = pk
        try:
            add_primary_key(conn, dest_schema, table, cols, cname)
        except Exception:
            conn.rollback()
    try:
        recreate_regular_indexes(conn, dest_schema, table, source_schema=source_schema, source_table=table)
    except Exception:
        conn.rollback()
