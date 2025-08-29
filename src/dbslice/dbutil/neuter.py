#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List

from .introspect import (
    schema_exists,
    table_exists,
    column_exists,
    get_column_char_max_length,
)


def neuter_data(conn, cfg: Dict[str, object]) -> bool:
    dest_schema = cfg.get('dest_schema')
    if not isinstance(dest_schema, str) or not dest_schema:
        raise ValueError('dest_schema must be set in config for neuter')
    if not schema_exists(conn, dest_schema):
        raise RuntimeError(f'dest_schema "{dest_schema}" does not exist')
    target_schema = dest_schema

    neuter_cfg = cfg.get('neuter')
    if not isinstance(neuter_cfg, dict) or neuter_cfg.get('enabled') is False:
        return False
    targets = neuter_cfg.get('targets') if isinstance(neuter_cfg.get('targets'), dict) else None
    if not isinstance(targets, dict):
        return False

    any_changes = False
    for table, rules in targets.items():
        t = str(table)
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
                with conn.cursor() as cur:
                    cur.execute(f'UPDATE "{target_schema}"."{t}" SET {c} = %s', (val,))
                conn.commit()
                any_changes = True
            else:
                raise ValueError(f'Unsupported neuter strategy: {strat}')

    return any_changes

