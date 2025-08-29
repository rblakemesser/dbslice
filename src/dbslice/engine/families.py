#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List
import asyncio
import psycopg

from ..dbutil import (
    ensure_schemas,
    table_exists,
    analyze_table,
    set_logged,
    drop_table_if_exists,
    get_primary_key,
    add_primary_key,
    reset_schema,
)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def run_families(
    conn,
    cfg: Dict[str, Any],
    *,
    dsn: str | None = None,
    fanout_parallel: int | None = None,
    include_defaults: bool = True,
    add_primary_keys_on_log: bool = True,
) -> List[str]:
    DEFAULT_FANOUT_PARALLEL = 8
    created: List[str] = []
    src = str(cfg.get('source_schema'))
    dst = str(cfg.get('dest_schema'))
    tmp = dst
    ensure_schemas(conn, [dst])

    families = cfg.get('table_groups') or []
    roots_cfg = {str(r.get('name')): r for r in (cfg.get('roots') or []) if r.get('name')}

    defaults_sql = " INCLUDING DEFAULTS" if include_defaults else ""

    used_shards = False
    for fam in families:
        root = fam.get('root') or {}
        root_table = str(root.get('table'))
        root_id_col = str(root.get('id_col') or 'id')
        sel_name = root.get('selection')
        root_join = str(root.get('join') or 'd.id = p.id')
        fam_tables = [root_table] + [str(d.get('table')) for d in (fam.get('deps') or [])]
        fam_tables = [t for t in fam_tables if t]
        if fam_tables and all(table_exists(conn, dst, t) for t in fam_tables):
            continue

        sharded = False
        shard_cfg = (roots_cfg.get(str(sel_name)) or {}).get('shard') if sel_name else None
        shard_count = 0
        if shard_cfg:
            try:
                shard_count = int((shard_cfg or {}).get('count') or 0)
            except Exception:
                shard_count = 0
            sharded = shard_count > 1

        if sharded:
            shards_schema = str(cfg.get('shards_schema', 'shards'))
            ensure_schemas(conn, [shards_schema])
            used_shards = True
            par = max(1, int(fanout_parallel or DEFAULT_FANOUT_PARALLEL))
            if not dsn:
                raise RuntimeError('DSN is required for parallel fanout')
            if par >= 1:
                async def _build_one(i: int) -> None:
                    sel_srcs = (cfg.get('_selection_sources') or {}).get(str(sel_name), {})
                    shard_sqls = sel_srcs.get('shards') or []
                    if i >= len(shard_sqls):
                        return
                    sel_tbl_subq = shard_sqls[i]
                    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                        async with aconn.cursor() as cur:
                            await cur.execute(f"DROP TABLE IF EXISTS {shards_schema}.{_quote_ident(root_table + f'_sh{i}')} ")
                            await cur.execute(
                                f"""
                                CREATE UNLOGGED TABLE {shards_schema}.{_quote_ident(root_table + f'_sh{i}')} AS
                                SELECT d.*
                                FROM {src}.{_quote_ident(root_table)} d
                                JOIN ({sel_tbl_subq}) p ON {root_join}
                                """
                            )
                        await aconn.commit()

                async def _runner_build() -> None:
                    sem = asyncio.Semaphore(par)
                    errors: list[Exception] = []
                    async def _bounded(i: int) -> None:
                        async with sem:
                            try:
                                await _build_one(i)
                            except Exception as e:
                                errors.append(e)
                    tasks = [asyncio.create_task(_bounded(i)) for i in range(shard_count)]
                    await asyncio.gather(*tasks)
                    if errors:
                        raise errors[0]

                asyncio.run(_runner_build())
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {dst}.{_quote_ident(root_table)}")
                cur.execute(f"CREATE UNLOGGED TABLE {dst}.{_quote_ident(root_table)} (LIKE {src}.{_quote_ident(root_table)}{defaults_sql})")
            conn.commit()
            # Insert shards in parallel if enabled
            if not dsn:
                raise RuntimeError('DSN is required for parallel fanout')
            if par >= 1:
                async def _insert_one(i: int) -> None:
                    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                        async with aconn.cursor() as cur:
                            await cur.execute(
                                f"INSERT INTO {dst}.{_quote_ident(root_table)} SELECT * FROM {shards_schema}.{_quote_ident(root_table + f'_sh{i}')}"
                            )
                        await aconn.commit()

                async def _runner_insert() -> None:
                    sem = asyncio.Semaphore(par)
                    errors: list[Exception] = []
                    async def _bounded(i: int) -> None:
                        async with sem:
                            try:
                                await _insert_one(i)
                            except Exception as e:
                                errors.append(e)
                    tasks = [asyncio.create_task(_bounded(i)) for i in range(shard_count)]
                    await asyncio.gather(*tasks)
                    if errors:
                        raise errors[0]

                asyncio.run(_runner_insert())
        else:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {dst}.{_quote_ident(root_table)}")
                if sel_name:
                    sel_srcs = (cfg.get('_selection_sources') or {}).get(str(sel_name), {})
                    sel_subq = sel_srcs.get('sql') or "SELECT NULL::bigint AS id WHERE FALSE"
                    cur.execute(
                        f"""
                        CREATE UNLOGGED TABLE {dst}.{_quote_ident(root_table)} AS
                        SELECT d.*
                        FROM {src}.{_quote_ident(root_table)} d
                        JOIN ({sel_subq}) p ON {root_join}
                        """
                    )
                else:
                    cur.execute(f"CREATE UNLOGGED TABLE {dst}.{_quote_ident(root_table)} (LIKE {src}.{_quote_ident(root_table)}{defaults_sql}) WITH NO DATA")

        for dep in (fam.get('deps') or []):
            dep_table = str(dep.get('table'))
            parent_table = str(dep.get('parent_table'))
            parent_schema = str(dep.get('parent_schema', dst))
            join_expr = str(dep.get('join'))
            where_expr = str(dep.get('where')) if dep.get('where') else ""
            # Avoid SELECT DISTINCT d.* because some tables have json columns
            # and json lacks an equality operator. We'll deduplicate after inserts.
            select_prefix = "SELECT d.*"
            dep_shard_by = str(dep.get('shard_by') or '').lower() if dep.get('shard_by') else None
            dep_shard_key = str(dep.get('shard_key')) if dep.get('shard_key') else None
            dep_shard_count = dep.get('shard_count')

            sources = dep.get('sources') or []
            if sources:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {dst}.{_quote_ident(dep_table)}")
                    cur.execute(f"CREATE UNLOGGED TABLE {dst}.{_quote_ident(dep_table)} (LIKE {src}.{_quote_ident(dep_table)}{defaults_sql})")
                conn.commit()
                # Insert from each source. Apply select_prefix and deduplicate within-source.
                # After all sources are inserted, if distinct is requested, perform a global de-dup across sources.
                for s in sources:
                    s_join = str(s.get('join') or '')
                    s_where = str(s.get('where')) if s.get('where') else ""
                    s_parent_schema = str(s.get('parent_schema', dst))
                    if s.get('selection'):
                        sel = str(s.get('selection'))
                        if sharded and (roots_cfg.get(sel) or {}).get('shard'):
                            sc = shard_count
                            par = max(1, int(fanout_parallel or DEFAULT_FANOUT_PARALLEL))
                            if not dsn:
                                raise RuntimeError('DSN is required for parallel fanout')
                            if par >= 1:
                                async def _ins_one(i: int) -> None:
                                    sel_srcs = (cfg.get('_selection_sources') or {}).get(sel, {})
                                    shard_sqls = sel_srcs.get('shards') or []
                                    if i >= len(shard_sqls):
                                        return
                                    sel_tbl = shard_sqls[i]
                                    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                                        async with aconn.cursor() as cur:
                                            await cur.execute(
                                                f"""
                                                INSERT INTO {dst}.{_quote_ident(dep_table)}
                                                SELECT d.*
                                                FROM {src}.{_quote_ident(dep_table)} d
                                                JOIN ({sel_tbl}) p ON {s_join}
                                                {f'WHERE {s_where}' if s_where else ''}
                                                """
                                            )
                                        await aconn.commit()

                                async def _runner() -> None:
                                    sem = asyncio.Semaphore(par)
                                    errors: list[Exception] = []
                                    async def _bounded(i: int) -> None:
                                        async with sem:
                                            try:
                                                await _ins_one(i)
                                            except Exception as e:
                                                errors.append(e)
                                    tasks = [asyncio.create_task(_bounded(i)) for i in range(sc)]
                                    await asyncio.gather(*tasks)
                                    if errors:
                                        raise errors[0]

                                asyncio.run(_runner())
                        else:
                            sel_srcs = (cfg.get('_selection_sources') or {}).get(sel, {})
                            sel_tbl = sel_srcs.get('sql') or "SELECT NULL::bigint AS id WHERE FALSE"
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    INSERT INTO {dst}.{_quote_ident(dep_table)}
                                    SELECT d.*
                                    FROM {src}.{_quote_ident(dep_table)} d
                                    JOIN ({sel_tbl}) p ON {s_join}
                                    {f'WHERE {s_where}' if s_where else ''}
                                    """
                                )
                    else:
                        s_parent = str(s.get('parent_table'))
                        if sharded and s_parent == root_table:
                            sc = shard_count
                            shards_schema = str(cfg.get('shards_schema', 'shards'))
                            par = max(1, int(fanout_parallel or DEFAULT_FANOUT_PARALLEL))
                            if not dsn:
                                raise RuntimeError('DSN is required for parallel fanout')
                            if par >= 1:
                                async def _ins_one(i: int) -> None:
                                    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                                        async with aconn.cursor() as cur:
                                            await cur.execute(
                                                f"""
                                                INSERT INTO {dst}.{_quote_ident(dep_table)}
                                                SELECT d.*
                                                FROM {src}.{_quote_ident(dep_table)} d
                                                JOIN {shards_schema}.{_quote_ident(s_parent + f'_sh{i}')} p ON {s_join}
                                                {f'WHERE {s_where}' if s_where else ''}
                                                """
                                            )
                                        await aconn.commit()

                                async def _runner() -> None:
                                    sem = asyncio.Semaphore(par)
                                    errors: list[Exception] = []
                                    async def _bounded(i: int) -> None:
                                        async with sem:
                                            try:
                                                await _ins_one(i)
                                            except Exception as e:
                                                errors.append(e)
                                    tasks = [asyncio.create_task(_bounded(i)) for i in range(sc)]
                                    await asyncio.gather(*tasks)
                                    if errors:
                                        raise errors[0]

                                asyncio.run(_runner())
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    INSERT INTO {dst}.{_quote_ident(dep_table)}
                                    SELECT d.*
                                    FROM {src}.{_quote_ident(dep_table)} d
                                    JOIN {s_parent_schema}.{_quote_ident(s_parent)} p ON {s_join}
                                    {f'WHERE {s_where}' if s_where else ''}
                                    """
                                )
                # Global dedup across all sources if requested
                if dep.get('distinct'):
                    pk = get_primary_key(conn, src, dep_table)
                    with conn.cursor() as cur:
                        if pk:
                            _cname, cols = pk
                            keys = ", ".join([f"d.{_quote_ident(c)}" for c in cols])
                            cur.execute(
                                f"""
                                CREATE TEMP TABLE _dbslice_distinct AS
                                SELECT DISTINCT ON ({keys}) d.*
                                FROM {dst}.{_quote_ident(dep_table)} d
                                ORDER BY {keys}
                                """
                            )
                        else:
                            cur.execute(
                                f"""
                                CREATE TEMP TABLE _dbslice_distinct AS
                                SELECT DISTINCT ON (md5(to_jsonb(d)::text)) d.*
                                FROM {dst}.{_quote_ident(dep_table)} d
                                ORDER BY md5(to_jsonb(d)::text)
                                """
                            )
                        cur.execute(f"TRUNCATE {dst}.{_quote_ident(dep_table)}")
                        cur.execute(f"INSERT INTO {dst}.{_quote_ident(dep_table)} SELECT * FROM _dbslice_distinct")
                        cur.execute("DROP TABLE _dbslice_distinct")
                    conn.commit()
                continue
            if dep_shard_by == 'pk_mod':
                if not dep_shard_key:
                    raise ValueError(f"Dependency {dep_table} requires shard_key when shard_by=pk_mod")
                sc = int(dep_shard_count) if dep_shard_count is not None else (shard_count if sharded else 0)
                if sc <= 1:
                    raise ValueError(f"Dependency {dep_table} requires shard_count>1 for pk_mod sharding")

                def _is_integer_col(col_schema: str, tbl: str, col: str) -> bool:
                    with conn.cursor() as c2:
                        c2.execute(
                            """
                            SELECT data_type, udt_name
                            FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s AND column_name = %s
                            """,
                            (col_schema, tbl, col),
                        )
                        row = c2.fetchone()
                    if not row:
                        raise ValueError(f"Column not found: {col_schema}.{tbl}.{col}")
                    dt, udt = str(row[0] or ''), str(row[1] or '')
                    return dt in ('integer', 'bigint', 'smallint') or udt in ('int4', 'int8', 'int2')

                is_int = _is_integer_col(src, dep_table, dep_shard_key)
                shards_schema = str(cfg.get('shards_schema', 'shards'))
                ensure_schemas(conn, [shards_schema])
                par = max(1, int(fanout_parallel or DEFAULT_FANOUT_PARALLEL))
                if not dsn:
                    raise RuntimeError('DSN is required for parallel fanout')
                if par >= 1:
                    async def _build_pmsh(i: int) -> None:
                        pred = f"(d.{_quote_ident(dep_shard_key)} % {sc}) = {i}" if is_int else f"(abs(hashtext(d.{_quote_ident(dep_shard_key)}::text)) % {sc}) = {i}"
                        async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                            async with aconn.cursor() as cur:
                                await cur.execute(f"DROP TABLE IF EXISTS {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')} ")
                                await cur.execute(
                                    f"""
                                    CREATE UNLOGGED TABLE {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')} AS
                                    SELECT d.*
                                    FROM {src}.{_quote_ident(dep_table)} d
                                    JOIN {dst}.{_quote_ident(parent_table)} p ON {join_expr}
                                    {f'WHERE {where_expr}' if where_expr else ''}
                                    AND {pred}
                                    """
                                )
                            await aconn.commit()

                    async def _runner_pmsh() -> None:
                        sem = asyncio.Semaphore(par)
                        errors: list[Exception] = []
                        async def _bounded(i: int) -> None:
                            async with sem:
                                try:
                                    await _build_pmsh(i)
                                except Exception as e:
                                    errors.append(e)
                        tasks = [asyncio.create_task(_bounded(i)) for i in range(sc)]
                        await asyncio.gather(*tasks)
                        if errors:
                            raise errors[0]

                    asyncio.run(_runner_pmsh())
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {dst}.{_quote_ident(dep_table)}")
                    cur.execute(f"CREATE UNLOGGED TABLE {dst}.{_quote_ident(dep_table)} (LIKE {src}.{_quote_ident(dep_table)}{defaults_sql})")
                conn.commit()
                if not dsn:
                    raise RuntimeError('DSN is required for parallel fanout')
                if par >= 1:
                    async def _insert_pmsh(i: int) -> None:
                        async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                            async with aconn.cursor() as cur:
                                await cur.execute(
                                    f"INSERT INTO {dst}.{_quote_ident(dep_table)} SELECT * FROM {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')}"
                                )
                            await aconn.commit()

                    async def _runner_ins_pmsh() -> None:
                        sem = asyncio.Semaphore(par)
                        errors: list[Exception] = []
                        async def _bounded(i: int) -> None:
                            async with sem:
                                try:
                                    await _insert_pmsh(i)
                                except Exception as e:
                                    errors.append(e)
                        tasks = [asyncio.create_task(_bounded(i)) for i in range(sc)]
                        await asyncio.gather(*tasks)
                        if errors:
                            raise errors[0]

                    asyncio.run(_runner_ins_pmsh())
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE UNLOGGED TABLE {dst}.{_quote_ident(dep_table)} AS
                        SELECT d.*
                        FROM {src}.{_quote_ident(dep_table)} d
                        JOIN {parent_schema}.{_quote_ident(parent_table)} p ON {join_expr}
                        {f'WHERE {where_expr}' if where_expr else ''}
                        """
                    )

            # Deduplicate after building dep table if requested (pk_mod / non-sources path)
            if dep.get('distinct') and not sources:
                pk = get_primary_key(conn, src, dep_table)
                with conn.cursor() as cur:
                    if pk:
                        _cname, cols = pk
                        keys = ", ".join([f"d.{_quote_ident(c)}" for c in cols])
                        cur.execute(
                            f"""
                            CREATE TEMP TABLE _dbslice_distinct AS
                            SELECT DISTINCT ON ({keys}) d.*
                            FROM {dst}.{_quote_ident(dep_table)} d
                            ORDER BY {keys}
                            """
                        )
                    else:
                        cur.execute(
                            f"""
                            CREATE TEMP TABLE _dbslice_distinct AS
                            SELECT DISTINCT ON (md5(to_jsonb(d)::text)) d.*
                            FROM {dst}.{_quote_ident(dep_table)} d
                            ORDER BY md5(to_jsonb(d)::text)
                            """
                        )
                    cur.execute(f"TRUNCATE {dst}.{_quote_ident(dep_table)}")
                    cur.execute(f"INSERT INTO {dst}.{_quote_ident(dep_table)} SELECT * FROM _dbslice_distinct")
                    cur.execute("DROP TABLE _dbslice_distinct")
                conn.commit()

        for table in [root_table] + [str(d.get('table')) for d in (fam.get('deps') or [])]:
            analyze_table(conn, f"{dst}.{_quote_ident(table)}")
            set_logged(conn, f"{dst}.{_quote_ident(table)}")
            if add_primary_keys_on_log:
                pk = get_primary_key(conn, src, table)
                if pk:
                    cname, cols = pk
                    try:
                        add_primary_key(conn, dst, table, cols, cname)
                    except psycopg.Error as e:
                        conn.rollback()
                        raise RuntimeError(f"Failed to add primary key on {dst}.{table}: {e}") from e
            created.append(f"{dst}.{table}")
    # Fast cleanup: drop the entire shards schema if used
    if used_shards:
        shards_schema = str(cfg.get('shards_schema', 'shards'))
        # Safety guard: never drop primary application schemas
        banned = {src, dst, str(cfg.get('tmp_schema', 'tmp')), 'public', 'pg_catalog', 'information_schema'}
        if shards_schema in banned:
            # Fallback: do nothing if misconfigured to a critical schema
            pass
        else:
            reset_schema(conn, shards_schema)

    return created
