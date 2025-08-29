#!/usr/bin/env python3
from __future__ import annotations

"""Table group data migration helper.

Given a config and requested table group names, reduces roots to prerequisites,
builds selections, runs table groups, and returns a compact summary.

Inputs
- conn: psycopg connection
- cfg: normalized config dict
- requested_names: list of table group names to migrate (empty for all)

Outputs
- Dict with {selections: {<root>: {count: N}}, table_groups_created: [schema.table,…]}
"""

from typing import List

from ..engine import build_selections, run_families


def _required_roots_for_families(cfg_all: dict, fams_sel: list[dict]) -> list[dict]:
    roots_all = cfg_all.get('roots') or []
    by_name = {str(r.get('name')): r for r in roots_all if r.get('name')}
    req: set[str] = set()
    for fam in fams_sel or []:
        root = fam.get('root') or {}
        sel = root.get('selection')
        if sel:
            req.add(str(sel))
        for dep in fam.get('deps') or []:
            for s in (dep.get('sources') or []):
                if s.get('selection'):
                    req.add(str(s.get('selection')))
    changed = True
    while changed:
        changed = False
        for name in list(req):
            r = by_name.get(name)
            if not r:
                continue
            sel = (r.get('selector') or {})
            if str(sel.get('mode') or '').lower() == 'scope_or_exists':
                dep_name = sel.get('scope_selection')
                if dep_name and str(dep_name) not in req:
                    req.add(str(dep_name))
                    changed = True
    return [by_name[n] for n in req if n in by_name]


def run_migrate_tables(
    conn,
    cfg: dict,
    requested_names: List[str],
    *,
    dsn: str | None = None,
    fanout_parallel: int | None = None,
) -> dict:
    fams = cfg.get('table_groups') or []
    filtered = fams if not requested_names else [f for f in fams if str(f.get('name')) in set(requested_names)]
    filtered_cfg = dict(cfg)
    filtered_cfg['table_groups'] = filtered
    filtered_roots = _required_roots_for_families(cfg, filtered_cfg.get('table_groups') or [])
    filtered_cfg['roots'] = filtered_roots
    selections = build_selections(conn, filtered_cfg)
    # For migrate-tables: omit defaults and add PKs at logging time
    created_tables = run_families(
        conn,
        filtered_cfg,
        dsn=dsn,
        fanout_parallel=fanout_parallel,
        include_defaults=False,
        add_primary_keys_on_log=True,
    ) if filtered_cfg.get('table_groups') else []
    sel_summary = {k: {"count": len(v)} for k, v in (selections or {}).items()}
    return {"selections": sel_summary, "table_groups_created": created_tables}
