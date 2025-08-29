#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List, Sequence

from ..dbutil import ensure_schemas, table_exists
from ..dbutil import column_exists as _column_exists


def _values_sql(ids: Sequence[int]) -> str:
    if not ids:
        return "SELECT NULL::bigint AS id WHERE FALSE"
    values = ",".join(f"({int(i)})" for i in ids)
    return f"SELECT id FROM (VALUES {values}) AS v(id)"


def _select_ids(conn, root: Dict[str, Any], cfg: Dict[str, Any]) -> tuple[List[int], str]:
    sel = root.get('selector') or {}
    mode = (sel.get('mode') or 'list').lower()
    ensure_list = list(root.get('ensure') or [])
    ids: List[int] = []
    if mode == 'list':
        ids = [int(x) for x in (sel.get('ids') or [])]
        sel_sql = _values_sql(ids)
    elif mode == 'sql':
        sql = str(sel.get('sql') or '')
        params = sel.get('params') or {}
        with conn.cursor() as cur:
            cur.execute(sql, params if isinstance(params, dict) else None)
            ids = [int(r[0]) for r in (cur.fetchall() or [])]
        sel_sql = f"SELECT id FROM ({sql}) AS src(id)"
    elif mode == 'referenced_by':
        refs = sel.get('refs') or []
        if not isinstance(refs, list):
            refs = []
        dest_schema = str(cfg.get('dest_schema', 'stage'))
        parts: List[str] = []
        for r in refs:
            if not isinstance(r, dict):
                continue
            schema = str(r.get('schema') or dest_schema)
            table = str(r.get('table') or '')
            column = str(r.get('column') or '')
            if not table or not column:
                continue
            if table_exists(conn, schema, table):
                parts.append(f'SELECT DISTINCT {column} AS id FROM "{schema}"."{table}"')
        if parts:
            union_sql = ' UNION '.join(parts)
            with conn.cursor() as cur:
                cur.execute(f"SELECT id FROM ({union_sql}) AS r WHERE id IS NOT NULL")
                ids = [int(r[0]) for r in (cur.fetchall() or [])]
            sel_sql = f"SELECT id FROM ({union_sql}) AS r WHERE id IS NOT NULL"
        else:
            ids = []
            sel_sql = "SELECT NULL::bigint AS id WHERE FALSE"
    elif mode == 'fk_in_stage':
        src_schema = str(cfg.get('source_schema', 'public'))
        dest_schema = str(cfg.get('dest_schema', 'stage'))
        table = str(root.get('table') or '')
        fk_col = str(sel.get('fk_column') or '')
        stage_table = str(sel.get('stage_table') or '')
        stage_id_col = str(sel.get('stage_id_col') or 'id')
        if table and fk_col and stage_table and table_exists(conn, src_schema, table) and table_exists(conn, dest_schema, stage_table):
            q = (
                f"SELECT DISTINCT d.id FROM \"{src_schema}\".\"{table}\" d "
                f"JOIN \"{dest_schema}\".\"{stage_table}\" s ON s.\"{stage_id_col}\" = d.\"{fk_col}\" "
                f"WHERE d.\"{fk_col}\" IS NOT NULL"
            )
            with conn.cursor() as cur:
                cur.execute(q)
                ids = [int(r[0]) for r in (cur.fetchall() or [])]
            sel_sql = q
        else:
            ids = []
            sel_sql = "SELECT NULL::bigint AS id WHERE FALSE"
    elif mode == 'refers_to_stage':
        src_schema = str(cfg.get('source_schema', 'public'))
        dest_schema = str(cfg.get('dest_schema', 'stage'))
        table = str(root.get('table') or '')
        targets = [t for t in (sel.get('targets') or []) if isinstance(t, dict)]
        clauses: List[str] = []
        for t in targets:
            st = str(t.get('stage_table') or '')
            lc = str(t.get('local_column') or '')
            sid = str(t.get('stage_id_col') or 'id')
            if st and lc and table_exists(conn, dest_schema, st):
                clauses.append(f'EXISTS (SELECT 1 FROM "{dest_schema}"."{st}" x WHERE x."{sid}" = d."{lc}")')
        if table and table_exists(conn, src_schema, table) and clauses:
            where = ' OR '.join(clauses)
            q = f"SELECT DISTINCT d.id FROM \"{src_schema}\".\"{table}\" d WHERE {where}"
            with conn.cursor() as cur:
                cur.execute(q)
                ids = [int(r[0]) for r in (cur.fetchall() or [])]
            sel_sql = q
        else:
            ids = []
            sel_sql = "SELECT NULL::bigint AS id WHERE FALSE"
    elif mode == 'referenced_by_column':
        dest_schema = str(cfg.get('dest_schema', 'stage'))
        schema = str((sel.get('schema') or dest_schema))
        col = str(sel.get('column') or '')
        if not col:
            ids = []
        else:
            tables: List[str] = []
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND column_name = %s
                    ORDER BY table_name
                    """,
                    (schema, col),
                )
                tables = [str(r[0]) for r in (cur.fetchall() or [])]
            parts: List[str] = []
            parts.extend([f'SELECT DISTINCT "{col}" AS id FROM "{schema}"."{t}"' for t in tables])
            extra = sel.get('extra_refs') or []
            if isinstance(extra, list):
                for r in extra:
                    if not isinstance(r, dict):
                        continue
                    sch = str(r.get('schema') or dest_schema)
                    tbl = str(r.get('table') or '')
                    c = str(r.get('column') or '')
                    if tbl and c and table_exists(conn, sch, tbl):
                        if not _column_exists(conn, sch, tbl, c):
                            continue
                        parts.append(f'SELECT DISTINCT "{c}" AS id FROM "{sch}"."{tbl}"')
            if parts:
                unions = ' UNION '.join(parts)
                with conn.cursor() as cur:
                    cur.execute(f"SELECT id FROM ({unions}) u WHERE id IS NOT NULL")
                    ids = [int(r[0]) for r in (cur.fetchall() or [])]
                sel_sql = f"SELECT id FROM ({unions}) u WHERE id IS NOT NULL"
            else:
                ids = []
                sel_sql = "SELECT NULL::bigint AS id WHERE FALSE"
    elif mode == 'scope_or_exists':
        src_schema = str(cfg.get('source_schema', 'public'))
        sel_schema = str(cfg.get('dest_schema', 'stage'))
        table = str(root.get('table') or '')
        scope_col = str(sel.get('scope_column') or '')
        scope_selection = str(sel.get('scope_selection') or '')
        exclude_values = sel.get('exclude_values') or []
        econf = sel.get('exists') or {}
        if not table or not scope_col or not scope_selection or not table_exists(conn, src_schema, table):
            ids = []
        else:
            params: List[Any] = []
            excl_sql = ''
            if isinstance(exclude_values, list) and exclude_values:
                ph = ','.join(['%s'] * len(exclude_values))
                excl_sql = f' AND d."{scope_col}" NOT IN ({ph})'
                params.extend([int(x) for x in exclude_values])
            scope_sql = (
                f' (d."{scope_col}" IN (SELECT id FROM "{sel_schema}"."_sel_{scope_selection}_ids")){excl_sql} '
            )

            exists_sql = ''
            require_not_null = bool(econf.get('require_local_not_null'))
            if isinstance(econf, dict) and econf.get('table') and econf.get('on') and econf.get('filter'):
                map_tbl = str(econf['table'])
                on_local = str((econf.get('on') or {}).get('local') or '')
                on_foreign = str((econf.get('on') or {}).get('foreign') or '')
                filt_col = str((econf.get('filter') or {}).get('column') or '')
                filt_sel = str((econf.get('filter') or {}).get('selection') or '')
                local_pred = econf.get('local_predicate') or {}
                lp_sql = ''
                if isinstance(local_pred, dict) and local_pred.get('column') is not None and local_pred.get('value') is not None:
                    lp_sql = f' AND d."{str(local_pred.get("column"))}" = %s'
                    params.append(int(local_pred.get('value')))
                if all([on_local, on_foreign, filt_col, filt_sel]) and table_exists(conn, src_schema, map_tbl):
                    nn = f' AND d."{on_local}" IS NOT NULL' if require_not_null else ''
                    exists_sql = (
                        f' (1=1{nn}{lp_sql} AND EXISTS (\n'
                        f'   SELECT 1 FROM "{src_schema}"."{map_tbl}" m\n'
                        f'   WHERE m."{on_foreign}" = d."{on_local}"\n'
                        f'     AND m."{filt_col}" IN (SELECT id FROM "{sel_schema}"."_sel_{filt_sel}_ids")\n'
                        f' )) '
                    )
            where_sql = scope_sql
            if exists_sql:
                where_sql = f'({scope_sql}) OR ({exists_sql})'
            q = f'SELECT DISTINCT d.id FROM "{src_schema}"."{table}" d WHERE {where_sql}'
            with conn.cursor() as cur:
                cur.execute(q, tuple(params) if params else None)
                ids = [int(r[0]) for r in (cur.fetchall() or [])]
            sel_sql = q
    else:
        raise ValueError(f"Unsupported selector mode: {mode}")
    for i in ensure_list:
        ii = int(i)
        if ii not in ids:
            ids.append(ii)
    return ids, sel_sql


def build_selections(conn, cfg: Dict[str, Any]) -> Dict[str, List[int]]:
    sel_schema = str(cfg.get('dest_schema', 'stage'))
    ensure_schemas(conn, [sel_schema])
    roots = cfg.get('roots') or []
    selections: Dict[str, List[int]] = {}
    cfg['_selection_sources'] = {}
    for root in roots:
        name = str(root.get('name'))
        if not name:
            raise ValueError('root requires a name')
        ids, sel_sql = _select_ids(conn, root, cfg)
        selections[name] = ids
        cfg['_selection_sources'][name] = {'sql': sel_sql}
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
                        except (TypeError, ValueError) as e:
                            raise ValueError(
                                f"Invalid weights row from weights_sql for root '{name}': rid={rid!r}, weight={w!r}"
                            ) from e
                items = [(i, int(weights_map.get(i, 1))) for i in ids]
                items.sort(key=lambda t: t[1], reverse=True)
                totals = [0] * count
                for rid, w in items:
                    k = min(range(count), key=lambda idx: totals[idx])
                    shards[k].append(rid)
                    totals[k] += w
            else:
                for idx, rid in enumerate(ids):
                    shards[idx % count].append(rid)
            cfg['_selection_sources'][name]['shards'] = [ _values_sql(s) for s in shards ]
    return selections
