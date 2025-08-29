#!/usr/bin/env python3
from __future__ import annotations

from typing import List, Sequence

from .introspect import (
    schema_exists,
    table_exists,
    get_primary_key,
    is_unlogged_table,
)


def ensure_schemas(conn, schemas: Sequence[str]) -> None:
    if not schemas:
        return
    with conn.cursor() as cur:
        for s in schemas:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    conn.commit()


def rename_schema(conn, old: str, new: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'ALTER SCHEMA "{old}" RENAME TO "{new}"')
    conn.commit()


def refresh_all_matviews(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = %s
            ORDER BY matviewname
            """,
            (schema,),
        )
        mvs = [r[0] for r in (cur.fetchall() or [])]
    for mv in mvs:
        with conn.cursor() as cur:
            cur.execute(f'REFRESH MATERIALIZED VIEW "{schema}"."{mv}"')
        conn.commit()


def swap_schemas(conn, *, dest_schema: str, old_schema: str = 'old') -> None:
    if not schema_exists(conn, dest_schema):
        raise RuntimeError(f'dest_schema "{dest_schema}" does not exist; nothing to swap')
    if schema_exists(conn, old_schema):
        raise RuntimeError(f'old schema "{old_schema}" already exists; aborting swap')
    rename_schema(conn, 'public', old_schema)
    rename_schema(conn, dest_schema, 'public')
    try:
        refresh_all_matviews(conn, 'public')
    except Exception:
        conn.rollback()


def unswap_schemas(conn, *, dest_schema: str, old_schema: str = 'old') -> None:
    if not schema_exists(conn, old_schema):
        raise RuntimeError(f'old schema "{old_schema}" does not exist; cannot unswap')
    if schema_exists(conn, dest_schema):
        raise RuntimeError(f'dest_schema "{dest_schema}" already exists; cannot unswap into existing schema')
    rename_schema(conn, 'public', dest_schema)
    rename_schema(conn, old_schema, 'public')
    try:
        refresh_all_matviews(conn, 'public')
    except Exception:
        conn.rollback()


def reset_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema}"')
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


def drop_tables_if_exists(conn, qualified_tables: List[str]) -> None:
    tables = [t for t in qualified_tables if t]
    if not tables:
        return
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(tables) + " CASCADE")
    conn.commit()


def list_relations_like(conn, schema: str, like_pattern: str) -> List[str]:
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


def add_primary_key(conn, schema: str, table: str, columns: Sequence[str], constraint_name: str | None = None) -> None:
    if not columns:
        return
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    cname = constraint_name or f"{table}_pkey"
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{cname}" PRIMARY KEY ({cols_sql})')
    conn.commit()


def create_schema_only_table(conn, *, source_schema: str, dest_schema: str, table: str) -> None:
    if table_exists(conn, dest_schema, table):
        return
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
    pk = get_primary_key(conn, source_schema, table)
    if pk:
        cname, cols = pk
        try:
            add_primary_key(conn, dest_schema, table, cols, cname)
        except Exception:
            conn.rollback()


def full_copy_table(conn, *, source_schema: str, dest_schema: str, table: str) -> None:
    if table_exists(conn, dest_schema, table):
        if is_unlogged_table(conn, dest_schema, table):
            set_logged(conn, f'"{dest_schema}"."{table}"')
            # Ensure PK exists after switching to LOGGED
            from .introspect import has_primary_key
            if not has_primary_key(conn, dest_schema, table):
                pk = get_primary_key(conn, source_schema, table)
                if pk:
                    cname, cols = pk
                    try:
                        add_primary_key(conn, dest_schema, table, cols, cname)
                    except Exception:
                        conn.rollback()
        return
    with conn.cursor() as cur:
        cur.execute(f"CREATE UNLOGGED TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
        cur.execute(f"INSERT INTO {dest_schema}.{table} SELECT * FROM {source_schema}.{table}")
    conn.commit()
    # Switch to LOGGED, then add PK (if present on source)
    set_logged(conn, f'"{dest_schema}"."{table}"')
    pk = get_primary_key(conn, source_schema, table)
    if pk:
        cname, cols = pk
        try:
            add_primary_key(conn, dest_schema, table, cols, cname)
        except Exception:
            conn.rollback()
