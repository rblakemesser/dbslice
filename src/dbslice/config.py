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
    # Roots (generic; forward-looking)
    roots = data.get('roots') or []
    cfg['roots'] = list(roots)
    # Phases/families (forward-looking)
    cfg['phases'] = list(data.get('phases') or [])
    cfg['families'] = list(data.get('families') or [])
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
