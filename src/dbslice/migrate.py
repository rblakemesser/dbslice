#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Any
import asyncio
import psycopg

from .dbutil import ensure_schemas


def migrate_precopy(conn, cfg: Dict[str, object], *, dsn: str | None = None, fanout_parallel: int | None = None) -> Dict[str, List[str]]:
    """Run the precopy phase concurrently via async connections.

    - Schema-only: create table LIKE src, add PK only.
    - Full-copy: create UNLOGGED LIKE src, bulk INSERT, add PK only, then SET LOGGED.
    - No non-PK indexes here; defer to migrate-indexes.
    """
    source_schema = str(cfg.get('source_schema'))
    dest_schema = str(cfg.get('dest_schema'))

    ensure_schemas(conn, [dest_schema])

    created_schema_only: List[str] = []
    created_full_copy: List[str] = []
    precopy = cfg.get('precopy') or {}
    so_list: List[str] = list((precopy or {}).get('schema_only') or [])  # type: ignore[assignment]
    fc_list: List[str] = list((precopy or {}).get('full_copy') or [])    # type: ignore[assignment]

    par = max(1, int(fanout_parallel or 8))
    if not dsn:
        raise RuntimeError('DSN is required for precopy parallelization')

    async def _table_exists(aconn, schema: str, table: str) -> bool:
        async with aconn.cursor() as cur:
            await cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
                LIMIT 1
                """,
                (schema, table),
            )
            row = await cur.fetchone()
            return row is not None

    async def _is_unlogged(aconn, schema: str, table: str) -> bool:
        async with aconn.cursor() as cur:
            await cur.execute(
                """
                SELECT c.relpersistence = 'u'
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
                """,
                (schema, table),
            )
            row = await cur.fetchone()
            return bool(row[0]) if row else False

    async def _get_pk(aconn, schema: str, table: str) -> tuple[str, list[str]] | None:
        async with aconn.cursor() as cur:
            await cur.execute(
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
            rows = await cur.fetchall() or []
        if not rows:
            return None
        cname = rows[0][0]
        cols = [r[1] for r in rows]
        return str(cname), [str(c) for c in cols]

    async def _has_pk(aconn, schema: str, table: str) -> bool:
        pk = await _get_pk(aconn, schema, table)
        return pk is not None

    async def _add_pk(aconn, schema: str, table: str, cname: str, cols: list[str]) -> None:
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        async with aconn.cursor() as cur:
            await cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{cname}" PRIMARY KEY ({cols_sql})')
        await aconn.commit()

    async def _set_logged(aconn, qualified: str) -> None:
        async with aconn.cursor() as cur:
            await cur.execute(f"ALTER TABLE {qualified} SET LOGGED")
        await aconn.commit()

    async def _schema_only(table: str) -> tuple[str, bool]:
        async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
            if await _table_exists(aconn, dest_schema, table):
                return table, False
            async with aconn.cursor() as cur:
                await cur.execute(f"CREATE TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
            await aconn.commit()
            pk = await _get_pk(aconn, source_schema, table)
            if pk:
                try:
                    cname, cols = pk
                    await _add_pk(aconn, dest_schema, table, cname, cols)
                except Exception:
                    await aconn.rollback()
            return table, True

    async def _full_copy(table: str) -> tuple[str, bool]:
        async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
            if await _table_exists(aconn, dest_schema, table):
                if await _is_unlogged(aconn, dest_schema, table):
                    await _set_logged(aconn, f'"{dest_schema}"."{table}"')
                    # Ensure PK at the time of switching to LOGGED if missing
                    if not await _has_pk(aconn, dest_schema, table):
                        src_pk = await _get_pk(aconn, source_schema, table)
                        if src_pk:
                            try:
                                cname, cols = src_pk
                                await _add_pk(aconn, dest_schema, table, cname, cols)
                            except Exception:
                                await aconn.rollback()
                                raise
                return table, False
            async with aconn.cursor() as cur:
                await cur.execute(f"CREATE UNLOGGED TABLE {dest_schema}.{table} (LIKE {source_schema}.{table} INCLUDING DEFAULTS)")
                await cur.execute(f"INSERT INTO {dest_schema}.{table} SELECT * FROM {source_schema}.{table}")
            await aconn.commit()
            # Switch to LOGGED, then add PK (if present on source)
            await _set_logged(aconn, f'"{dest_schema}"."{table}"')
            pk = await _get_pk(aconn, source_schema, table)
            if pk:
                try:
                    cname, cols = pk
                    await _add_pk(aconn, dest_schema, table, cname, cols)
                except Exception:
                    await aconn.rollback()
            return table, True

    async def _runner(items: list[str], fn):
        sem = asyncio.Semaphore(par)
        results: list[tuple[str, bool]] = []
        errors: list[Exception] = []
        async def _bounded(name: str):
            async with sem:
                try:
                    results.append(await fn(name))
                except Exception as e:
                    errors.append(e)
        tasks = [asyncio.create_task(_bounded(t)) for t in items]
        await asyncio.gather(*tasks)
        if errors:
            raise errors[0]
        return results

    # Run schema-only and full-copy in parallel groups (each internally bounded)
    if so_list:
        so_res = asyncio.run(_runner(so_list, _schema_only))
        created_names = {name for name, created in so_res if created}
        created_schema_only = [t for t in so_list if t in created_names]
    if fc_list:
        fc_res = asyncio.run(_runner(fc_list, _full_copy))
        created_names = {name for name, created in fc_res if created}
        created_full_copy = [t for t in fc_list if t in created_names]

    return {"schema_only": created_schema_only, "full_copy": created_full_copy}
