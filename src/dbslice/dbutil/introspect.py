#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple


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


def column_exists(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def get_column_char_max_length(conn, schema: str, table: str, column: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        row = cur.fetchone()
    if not row:
        return None
    return int(row[0]) if row[0] is not None else None


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


def list_tables_in_schema(conn, schema: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        rows = cur.fetchall() or []
    return [str(r[0]) for r in rows]


def list_unlogged_tables(conn, schema: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r' AND c.relpersistence = 'u'
            ORDER BY c.relname
            """,
            (schema,),
        )
        rows = cur.fetchall() or []
    return [str(r[0]) for r in rows]


def is_unlogged_table(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relpersistence = 'u'
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
            """,
            (schema, table),
        )
        row = cur.fetchone()
    return bool(row[0]) if row else False


def schema_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.schemata WHERE schema_name = %s
            """,
            (schema,),
        )
        return cur.fetchone() is not None

