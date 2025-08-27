#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List, Sequence

import psycopg

from .dbutil import (
    ensure_schemas,
    table_exists,
    get_primary_key,
    add_primary_key,
    recreate_regular_indexes,
    move_to_schema,
    analyze_table,
    set_logged,
    drop_table_if_exists,
)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _create_selection_table(conn, name: str, ids: Sequence[Any], tmp_schema: str = 'tmp') -> None:
    sel_tbl = f"{tmp_schema}._sel_{name}_ids"
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {sel_tbl}")
        cur.execute(f"CREATE UNLOGGED TABLE {sel_tbl} (id bigint)")
        if ids:
            # Bulk insert using VALUES list (fine for small sets in tests); could switch to COPY later
            values = ",".join(["(%s)" for _ in ids])
            cur.execute(f"INSERT INTO {sel_tbl} (id) VALUES {values}", list(ids))
    conn.commit()


def _select_ids(conn, root: Dict[str, Any]) -> List[int]:
    sel = root.get('selector') or {}
    mode = (sel.get('mode') or 'list').lower()
    ensure_list = list(root.get('ensure') or [])
    ids: List[int] = []
    if mode == 'list':
        ids = [int(x) for x in (sel.get('ids') or [])]
    elif mode == 'sql':
        sql = str(sel.get('sql') or '')
        params = sel.get('params') or {}
        with conn.cursor() as cur:
            cur.execute(sql, params if isinstance(params, dict) else None)
            ids = [int(r[0]) for r in (cur.fetchall() or [])]
    else:
        raise ValueError(f"Unsupported selector mode: {mode}")
    # Ensure list additions
    for i in ensure_list:
        ii = int(i)
        if ii not in ids:
            ids.append(ii)
    return ids


def build_selections(conn, cfg: Dict[str, Any]) -> Dict[str, List[int]]:
    """Resolve roots and materialize tmp._sel_<name>_ids tables.

    Also handles sharding metadata: if a root declares shard.count>1, we also create
    per-shard selection tables tmp._sel_<name>_sh{i}_ids. Returns selected ids per root.
    """
    tmp_schema = str(cfg.get('tmp_schema', 'tmp'))
    ensure_schemas(conn, [tmp_schema])
    roots = cfg.get('roots') or []
    selections: Dict[str, List[int]] = {}
    for root in roots:
        name = str(root.get('name'))
        if not name:
            raise ValueError('root requires a name')
        ids = _select_ids(conn, root)
        selections[name] = ids
        _create_selection_table(conn, name, ids, tmp_schema=tmp_schema)
        # Optional sharding: create per-shard selection tables
        shard = (root.get('shard') or {})
        count = int(shard.get('count') or 0)
        if count and count > 1:
            strategy = (shard.get('strategy') or 'round_robin').lower()
            weights_sql = shard.get('weights_sql')
            shards: List[List[int]] = [[] for _ in range(count)]
            if strategy == 'weighted' and weights_sql:
                weights_map: Dict[int, int] = {}
                with conn.cursor() as cur:
                    cur.execute(str(weights_sql))
                    for rid, w in (cur.fetchall() or []):
                        try:
                            weights_map[int(rid)] = int(w)
                        except Exception:
                            continue
                # Greedy balance by weight
                items = [(i, int(weights_map.get(i, 1))) for i in ids]
                items.sort(key=lambda t: t[1], reverse=True)
                totals = [0] * count
                for rid, w in items:
                    k = min(range(count), key=lambda idx: totals[idx])
                    shards[k].append(rid)
                    totals[k] += w
            else:
                # round_robin default
                for idx, rid in enumerate(ids):
                    shards[idx % count].append(rid)
            # materialize shard selection tables
            for i, shard_ids in enumerate(shards):
                _create_selection_table(conn, f"{name}_sh{i}", shard_ids, tmp_schema=tmp_schema)
    return selections


def run_families(conn, cfg: Dict[str, Any]) -> List[str]:
    """Execute configured families (unsharded). Returns list of dest tables created.

    Currently supports:
      - Root: table, id_col, selection name, join (optional; defaults to d.id=p.id)
      - Deps: table, parent_table, join (SQL using d/p aliases)
    """
    created: List[str] = []
    src = str(cfg.get('source_schema'))
    dst = str(cfg.get('dest_schema'))
    tmp = str(cfg.get('tmp_schema', 'tmp'))
    ensure_schemas(conn, [tmp, dst])

    families = cfg.get('families') or []
    # Build lookup for roots by name
    roots_cfg = {str(r.get('name')): r for r in (cfg.get('roots') or []) if r.get('name')}

    for fam in families:
        root = fam.get('root') or {}
        root_table = str(root.get('table'))
        root_id_col = str(root.get('id_col') or 'id')
        sel_name = root.get('selection')
        root_join = str(root.get('join') or 'd.id = p.id')
        # Skip family if all target tables already exist in dest
        fam_tables = [root_table] + [str(d.get('table')) for d in (fam.get('deps') or [])]
        if all(table_exists(conn, dst, t) for t in fam_tables if t):
            # All tables for this family are already present in dest; skip
            continue
        # Determine sharding for this root
        sharded = False
        shard_count = 0
        if sel_name and sel_name in roots_cfg:
            shard_cfg = (roots_cfg[sel_name].get('shard') or {})
            shard_count = int(shard_cfg.get('count') or 0)
            sharded = shard_count > 1

        if sharded:
            ensure_schemas(conn, [str(cfg.get('shards_schema', 'shards'))])
            shards_schema = str(cfg.get('shards_schema', 'shards'))
            # Build per-shard root tables
            for i in range(shard_count):
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {shards_schema}.{_quote_ident(root_table + f'_sh{i}')} ")
                    sel_tbl = f"{tmp}._sel_{sel_name}_sh{i}_ids"
                    cur.execute(
                        f"""
                        CREATE UNLOGGED TABLE {shards_schema}.{_quote_ident(root_table + f'_sh{i}')} AS
                        SELECT d.*
                        FROM {src}.{_quote_ident(root_table)} d
                        JOIN {sel_tbl} p ON {root_join}
                        """
                    )
            # Union shards into tmp root
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(root_table)}")
                cur.execute(f"CREATE UNLOGGED TABLE {tmp}.{_quote_ident(root_table)} (LIKE {src}.{_quote_ident(root_table)} INCLUDING DEFAULTS)")
                for i in range(shard_count):
                    cur.execute(
                        f"INSERT INTO {tmp}.{_quote_ident(root_table)} SELECT * FROM {shards_schema}.{_quote_ident(root_table + f'_sh{i}')}"
                    )
        else:
            # Build tmp root via selection join if selection provided
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(root_table)}")
                if sel_name:
                    sel_tbl = f"{tmp}._sel_{sel_name}_ids"
                    cur.execute(
                        f"""
                        CREATE UNLOGGED TABLE {tmp}.{_quote_ident(root_table)} AS
                        SELECT d.*
                        FROM {src}.{_quote_ident(root_table)} d
                        JOIN {sel_tbl} p ON {root_join}
                        """
                    )
                else:
                    cur.execute(f"CREATE UNLOGGED TABLE {tmp}.{_quote_ident(root_table)} (LIKE {src}.{_quote_ident(root_table)} INCLUDING DEFAULTS) WITH NO DATA")
        # PK and indexes
        pk = get_primary_key(conn, src, root_table)
        if pk:
            cname, cols = pk
            try:
                add_primary_key(conn, tmp, root_table, cols, cname)
            except Exception:
                conn.rollback()
        try:
            recreate_regular_indexes(conn, tmp, root_table, source_schema=src, source_table=root_table)
        except Exception:
            conn.rollback()

        # Dependents
        for dep in (fam.get('deps') or []):
            dep_table = str(dep.get('table'))
            parent_table = str(dep.get('parent_table'))
            join_expr = str(dep.get('join'))
            where_expr = str(dep.get('where')) if dep.get('where') else ""
            select_prefix = "SELECT DISTINCT d.*" if dep.get('distinct') else "SELECT d.*"
            dep_shard_by = str(dep.get('shard_by') or '').lower() if dep.get('shard_by') else None
            dep_shard_key = str(dep.get('shard_key')) if dep.get('shard_key') else None
            dep_shard_count = dep.get('shard_count')

            # Multi-source union support: dep can specify multiple 'sources' entries
            sources = dep.get('sources') or []
            if sources:
                # Create empty tmp dep table and add PK/indexes, then insert from each source with ON CONFLICT DO NOTHING
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(dep_table)}")
                    cur.execute(f"CREATE UNLOGGED TABLE {tmp}.{_quote_ident(dep_table)} (LIKE {src}.{_quote_ident(dep_table)} INCLUDING DEFAULTS)")
                pk = get_primary_key(conn, src, dep_table)
                if pk:
                    cname, cols = pk
                    try:
                        add_primary_key(conn, tmp, dep_table, cols, cname)
                    except Exception:
                        conn.rollback()
                # Insert from each source
                for s in sources:
                    s_join = str(s.get('join') or '')
                    s_where = str(s.get('where')) if s.get('where') else ""
                    if s.get('selection'):
                        sel = str(s.get('selection'))
                        if sharded and (roots_cfg.get(sel) or {}).get('shard'):
                            sc = shard_count
                            for i in range(sc):
                                sel_tbl = f"{tmp}._sel_{sel}_sh{i}_ids"
                                with conn.cursor() as cur:
                                    cur.execute(
                                        f"""
                                        INSERT INTO {tmp}.{_quote_ident(dep_table)}
                                        SELECT d.*
                                        FROM {src}.{_quote_ident(dep_table)} d
                                        JOIN {sel_tbl} p ON {s_join}
                                        {f'WHERE {s_where}' if s_where else ''}
                                        ON CONFLICT DO NOTHING
                                        """
                                    )
                        else:
                            sel_tbl = f"{tmp}._sel_{sel}_ids"
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    INSERT INTO {tmp}.{_quote_ident(dep_table)}
                                    SELECT d.*
                                    FROM {src}.{_quote_ident(dep_table)} d
                                    JOIN {sel_tbl} p ON {s_join}
                                    {f'WHERE {s_where}' if s_where else ''}
                                    ON CONFLICT DO NOTHING
                                    """
                                )
                    else:
                        # parent_table source
                        s_parent = str(s.get('parent_table'))
                        if sharded and s_parent == root_table:
                            # Read from per-shard parent
                            sc = shard_count
                            shards_schema = str(cfg.get('shards_schema', 'shards'))
                            for i in range(sc):
                                with conn.cursor() as cur:
                                    cur.execute(
                                        f"""
                                        INSERT INTO {tmp}.{_quote_ident(dep_table)}
                                        SELECT d.*
                                        FROM {src}.{_quote_ident(dep_table)} d
                                        JOIN {shards_schema}.{_quote_ident(s_parent + f'_sh{i}')} p ON {s_join}
                                        {f'WHERE {s_where}' if s_where else ''}
                                        ON CONFLICT DO NOTHING
                                        """
                                    )
                        else:
                            # Join to tmp parent
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    INSERT INTO {tmp}.{_quote_ident(dep_table)}
                                    SELECT d.*
                                    FROM {src}.{_quote_ident(dep_table)} d
                                    JOIN {tmp}.{_quote_ident(s_parent)} p ON {s_join}
                                    {f'WHERE {s_where}' if s_where else ''}
                                    ON CONFLICT DO NOTHING
                                    """
                                )
                # Continue with next dep (skip standard paths)
                # Add non-PK indexes later when finalizing below
                continue
            if dep_shard_by == 'pk_mod':
                # Primary-key modulo sharding on the dependent table (balanced fanout)
                if not dep_shard_key:
                    raise ValueError(f"Dependency {dep_table} requires shard_key when shard_by=pk_mod")
                # Determine shard count: explicit required if root isn't sharded
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
                for i in range(sc):
                    with conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')} ")
                        pred = f"(d.{_quote_ident(dep_shard_key)} % {sc}) = {i}" if is_int else f"(abs(hashtext(d.{_quote_ident(dep_shard_key)}::text)) % {sc}) = {i}"
                        cur.execute(
                            f"""
                            CREATE UNLOGGED TABLE {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')} AS
                            {select_prefix}
                            FROM {src}.{_quote_ident(dep_table)} d
                            JOIN {tmp}.{_quote_ident(parent_table)} p ON {join_expr}
                            {f'WHERE {where_expr} AND ' if where_expr else 'WHERE '}{pred}
                            """
                        )
                # Union shards into tmp dep
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(dep_table)}")
                    cur.execute(f"CREATE UNLOGGED TABLE {tmp}.{_quote_ident(dep_table)} (LIKE {src}.{_quote_ident(dep_table)} INCLUDING DEFAULTS)")
                    for i in range(sc):
                        cur.execute(
                            f"INSERT INTO {tmp}.{_quote_ident(dep_table)} SELECT * FROM {shards_schema}.{_quote_ident(dep_table + f'_pmsh{i}')}"
                        )
            elif sharded:
                shards_schema = str(cfg.get('shards_schema', 'shards'))
                # build per-shard dep from per-shard parent
                for i in range(shard_count):
                    with conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {shards_schema}.{_quote_ident(dep_table + f'_sh{i}')} ")
                        cur.execute(
                            f"""
                            CREATE UNLOGGED TABLE {shards_schema}.{_quote_ident(dep_table + f'_sh{i}')} AS
                            {select_prefix}
                            FROM {src}.{_quote_ident(dep_table)} d
                            JOIN {shards_schema}.{_quote_ident(parent_table + f'_sh{i}')} p ON {join_expr}
                            {f'WHERE {where_expr}' if where_expr else ''}
                            """
                        )
                # union into tmp
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(dep_table)}")
                    cur.execute(f"CREATE UNLOGGED TABLE {tmp}.{_quote_ident(dep_table)} (LIKE {src}.{_quote_ident(dep_table)} INCLUDING DEFAULTS)")
                    for i in range(shard_count):
                        cur.execute(
                            f"INSERT INTO {tmp}.{_quote_ident(dep_table)} SELECT * FROM {shards_schema}.{_quote_ident(dep_table + f'_sh{i}')}"
                        )
            else:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {tmp}.{_quote_ident(dep_table)}")
                    cur.execute(
                        f"""
                        CREATE UNLOGGED TABLE {tmp}.{_quote_ident(dep_table)} AS
                        {select_prefix}
                        FROM {src}.{_quote_ident(dep_table)} d
                        JOIN {tmp}.{_quote_ident(parent_table)} p ON {join_expr}
                        {f'WHERE {where_expr}' if where_expr else ''}
                        """
                    )
            pk = get_primary_key(conn, src, dep_table)
            if pk:
                cname, cols = pk
                try:
                    add_primary_key(conn, tmp, dep_table, cols, cname)
                except Exception:
                    conn.rollback()
            try:
                recreate_regular_indexes(conn, tmp, dep_table, source_schema=src, source_table=dep_table)
            except Exception:
                conn.rollback()

        # Finalize: index, analyze, set logged, move tmp root/dep to dest
        for table in [root_table] + [str(d.get('table')) for d in (fam.get('deps') or [])]:
            # Drop existing dest table and move
            # Recreate non-PK indexes on tmp (ensures identical names/defs)
            recreate_regular_indexes(conn, tmp, table, source_schema=src, source_table=table)
            analyze_table(conn, f"{tmp}.{_quote_ident(table)}")
            set_logged(conn, f"{tmp}.{_quote_ident(table)}")
            drop_table_if_exists(conn, f"{dst}.{_quote_ident(table)}")
            move_to_schema(conn, f"{tmp}.{_quote_ident(table)}", dst)
            created.append(f"{dst}.{table}")

        # Cleanup shard artifacts if any
        if sharded:
            shards_schema = str(cfg.get('shards_schema', 'shards'))
            for table in [root_table] + [str(d.get('table')) for d in (fam.get('deps') or [])]:
                for i in range(shard_count):
                    drop_table_if_exists(conn, f"{shards_schema}.{_quote_ident(table + f'_sh{i}')} ")

        # Cleanup pk_mod artifacts for deps
        shards_schema = str(cfg.get('shards_schema', 'shards'))
        for dep in (fam.get('deps') or []):
            dep_table = str(dep.get('table'))
            if str(dep.get('shard_by') or '').lower() == 'pk_mod':
                # Drop any *_pmsh* tables for this dep
                # We cannot enumerate the exact count here; use LIKE match and drop via list_relations_like equivalent
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT c.relname
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = %s AND c.relname LIKE %s
                        """,
                        (shards_schema, f"{dep_table}_pmsh%"),
                    )
                    rows = [r[0] for r in (cur.fetchall() or [])]
                for rel in rows:
                    drop_table_if_exists(conn, f"{shards_schema}.\"{rel}\"")

    return created
