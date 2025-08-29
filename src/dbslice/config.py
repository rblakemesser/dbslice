#!/usr/bin/env python3
from __future__ import annotations

import os
from typing import Any, Dict, List
import yaml


def load_config(path: str) -> Dict[str, Any]:
    with open(path, 'r') as f:
        data = yaml.safe_load(f) or {}
    # Normalize keys we care about
    cfg: Dict[str, Any] = {
        'source_schema': data.get('source_schema') or data.get('src_schema') or 'public',
        'dest_schema': data.get('dest_schema') or data.get('dst_schema') or 'stage',
        'tmp_schema': data.get('tmp_schema') or 'tmp',
        'shards_schema': data.get('shards_schema') or 'shards',
    }
    # Precopy lists
    precopy = data.get('precopy') or {}
    cfg['precopy'] = {
        'schema_only': list(precopy.get('schema_only') or []),
        'full_copy': list(precopy.get('full_copy') or []),
    }
    # Table groups (no backward-compat: single source of truth)
    cfg['phases'] = list(data.get('phases') or [])
    cfg['table_groups'] = list(data.get('table_groups') or [])

    # Derive roots from table_groups.root.selector (single source of truth)
    roots: List[Dict[str, Any]] = []
    for g in cfg['table_groups']:
        if not isinstance(g, dict):
            continue
        root = g.get('root') or {}
        if not isinstance(root, dict):
            continue
        # Use explicit selection alias if provided; otherwise default to group name
        sel_name = root.get('selection') or g.get('name')
        selector = root.get('selector') or None
        if not sel_name or not selector:
            # If no selector is defined here, this group depends on another root's selection
            continue
        r: Dict[str, Any] = {
            'name': str(sel_name),
            'table': str(root.get('table') or g.get('name') or ''),
            'id_col': str(root.get('id_col') or 'id'),
            'selector': selector,
        }
        if root.get('ensure') is not None:
            r['ensure'] = list(root.get('ensure') or [])
        if root.get('phase') is not None:
            r['phase'] = root.get('phase')
        if root.get('shard') is not None:
            r['shard'] = root.get('shard')
        roots.append(r)
    cfg['roots'] = roots
    # Neuter (redaction) config: pass through as-is (dict)
    neuter = data.get('neuter') or {}
    if isinstance(neuter, dict):
        cfg['neuter'] = neuter
    else:
        cfg['neuter'] = {}
    # Reconcile toggles (defaults enabled)
    rec = data.get('reconcile') or {}
    def _bool(key: str, default: bool = True) -> bool:
        v = rec.get(key)
        return bool(default if v is None else v)
    cfg['reconcile'] = {
        'sequences': _bool('sequences', True),
        'primary_keys': _bool('primary_keys', True),
        'indexes': _bool('indexes', True),
        'triggers': _bool('triggers', True),
        'column_settings': _bool('column_settings', True),
        'constraints': _bool('constraints', True),
        'views': _bool('views', True),
        'permissions': _bool('permissions', False),
    }
    return cfg
