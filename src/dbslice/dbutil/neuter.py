#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Any

import asyncio
import psycopg
from psycopg import AsyncConnection

from .introspect import (
    schema_exists,
    table_exists,
    column_exists,
    get_column_char_max_length,
)


def neuter_data(conn, cfg: Dict[str, object], *, only_table: str | None = None) -> bool:
    dest_schema = cfg.get('dest_schema')
    if not isinstance(dest_schema, str) or not dest_schema:
        raise ValueError('dest_schema must be set in config for neuter')
    if not schema_exists(conn, dest_schema):
        raise RuntimeError(f'dest_schema "{dest_schema}" does not exist')
    target_schema = dest_schema

    neuter_cfg = cfg.get('neuter')
    if not isinstance(neuter_cfg, dict) or neuter_cfg.get('enabled') is False:
        return False
    parallel = 1
    try:
        if isinstance(neuter_cfg.get('parallel'), int):
            parallel = max(1, int(neuter_cfg.get('parallel')))
    except Exception:
        parallel = 1
    targets = neuter_cfg.get('targets') if isinstance(neuter_cfg.get('targets'), dict) else None
    if not isinstance(targets, dict):
        return False

    any_changes = False
    for table, rules in targets.items():
        t = str(table)
        if only_table and t != only_table:
            continue
        if not table_exists(conn, target_schema, t):
            continue
        if not isinstance(rules, list):
            continue
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            c = str(rule.get('column') or '')
            strat = str(rule.get('strategy') or '')
            val = rule.get('value')
            skip_pattern = rule.get('skip_pattern')
            if not c or not strat or val is None:
                continue
            if not column_exists(conn, target_schema, t, c):
                continue
            if strat == 'prefix':
                extra_pred = ''
                params: List[object] = []
                sps = []
                if isinstance(skip_pattern, str) and skip_pattern:
                    sps = [skip_pattern]
                elif isinstance(rule.get('skip_patterns'), list):
                    sps = [str(x) for x in rule.get('skip_patterns') if isinstance(x, (str, bytes))]
                for _ in sps:
                    extra_pred += f' AND {c} NOT ILIKE %s'
                params.extend(sps)
                max_len = get_column_char_max_length(conn, target_schema, t, c)
                sval = str(val)
                # Optional sharding via modulo on a key column for parallelism
                shard_cfg = rule.get('shard') if isinstance(rule.get('shard'), dict) else None
                if shard_cfg and parallel > 1:
                    shard_col = str(shard_cfg.get('column') or 'id')
                    modulo = int(shard_cfg.get('modulo') or shard_cfg.get('parts') or parallel)
                    if modulo < 1:
                        modulo = parallel
                    if not column_exists(conn, target_schema, t, shard_col):
                        raise ValueError(f'shard column {shard_col} does not exist on {target_schema}.{t}')
                    dsn = None
                    try:
                        dsn = conn.info.dsn
                    except Exception:
                        dsn = None
                    if not dsn:
                        raise RuntimeError('DSN is required for parallel neuter but was not available')

                    async def _run_shard(shard_idx: int) -> None:
                        async with await AsyncConnection.connect(dsn) as aconn:
                            async with aconn.cursor() as cur:
                                if max_len is None:
                                    await cur.execute(
                                        f"""
                                        UPDATE "{target_schema}"."{t}"
                                        SET {c} = %s || {c}
                                        WHERE {c} IS NOT NULL AND {c} <> ''
                                          {extra_pred}
                                          AND {c} NOT ILIKE %s
                                          AND ({shard_col} % %s) = %s
                                        """,
                                        (sval, *params, sval + '%', modulo, shard_idx),
                                    )
                                else:
                                    await cur.execute(
                                        f"""
                                        UPDATE "{target_schema}"."{t}"
                                        SET {c} = left(%s || {c}, %s)
                                        WHERE {c} IS NOT NULL AND {c} <> ''
                                          {extra_pred}
                                          AND {c} NOT ILIKE %s
                                          AND ({shard_col} % %s) = %s
                                        """,
                                        (sval, int(max_len), *params, sval + '%', modulo, shard_idx),
                                    )
                            await aconn.commit()

                    async def _run_all() -> None:
                        limit = min(parallel, modulo)
                        sem = asyncio.Semaphore(limit)
                        async def _guarded(i: int):
                            async with sem:
                                await _run_shard(i)
                        await asyncio.gather(*[_guarded(i) for i in range(modulo)])

                    asyncio.run(_run_all())
                    any_changes = True
                else:
                    with conn.cursor() as cur:
                        if max_len is None:
                            cur.execute(
                                f"""
                                UPDATE "{target_schema}"."{t}"
                                SET {c} = %s || {c}
                                WHERE {c} IS NOT NULL AND {c} <> ''
                                  {extra_pred}
                                  AND {c} NOT ILIKE %s
                                """,
                                (sval, *params, sval + '%'),
                            )
                        else:
                            cur.execute(
                                f"""
                                UPDATE "{target_schema}"."{t}"
                                SET {c} = left(%s || {c}, %s)
                                WHERE {c} IS NOT NULL AND {c} <> ''
                                  {extra_pred}
                                  AND {c} NOT ILIKE %s
                                """,
                                (sval, int(max_len), *params, sval + '%'),
                            )
                    conn.commit()
                    any_changes = True
            elif strat == 'replace':
                shard_cfg = rule.get('shard') if isinstance(rule.get('shard'), dict) else None
                if shard_cfg and parallel > 1:
                    shard_col = str(shard_cfg.get('column') or 'id')
                    modulo = int(shard_cfg.get('modulo') or shard_cfg.get('parts') or parallel)
                    if modulo < 1:
                        modulo = parallel
                    if not column_exists(conn, target_schema, t, shard_col):
                        raise ValueError(f'shard column {shard_col} does not exist on {target_schema}.{t}')
                    dsn = None
                    try:
                        dsn = conn.info.dsn
                    except Exception:
                        dsn = None
                    if not dsn:
                        raise RuntimeError('DSN is required for parallel neuter but was not available')

                    async def _run_shard(shard_idx: int) -> None:
                        async with await AsyncConnection.connect(dsn) as aconn:
                            async with aconn.cursor() as cur:
                                await cur.execute(
                                    f'UPDATE "{target_schema}"."{t}" SET {c} = %s WHERE ({shard_col} % %s) = %s',
                                    (val, modulo, shard_idx),
                                )
                            await aconn.commit()

                    async def _run_all() -> None:
                        limit = min(parallel, modulo)
                        sem = asyncio.Semaphore(limit)
                        async def _guarded(i: int):
                            async with sem:
                                await _run_shard(i)
                        await asyncio.gather(*[_guarded(i) for i in range(modulo)])

                    asyncio.run(_run_all())
                    any_changes = True
                else:
                    with conn.cursor() as cur:
                        cur.execute(f'UPDATE "{target_schema}"."{t}" SET {c} = %s', (val,))
                    conn.commit()
                    any_changes = True
            else:
                raise ValueError(f'Unsupported neuter strategy: {strat}')

    return any_changes
