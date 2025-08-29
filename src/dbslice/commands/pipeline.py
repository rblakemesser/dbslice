#!/usr/bin/env python3
from __future__ import annotations

"""Pipeline commands for full migration and restart flows.

These helpers encapsulate the high-level orchestration previously in the CLI:
- migrate_pipeline: precopy → selections → table_groups → neuter → sequences → functions → triggers → constraints
- restart_pipeline: resets the destination schema then runs the same pipeline

Inputs
- cfg: Normalized config dict from `load_config()` (source/dest/tmp/shards, roots, table_groups, neuter)
- url: DATABASE_URL to connect to (string)
- validate_parallel: Optional integer for FK validation concurrency (generic knob)

Outputs
- Dict with the same YAML-ready shape the CLI emitted previously.

Failure policy
- Fail loudly for unrecoverable errors; do not suppress exceptions at import or orchestration boundaries.
"""

from typing import Any, Dict, List
import psycopg

from ..migrate import migrate_precopy
from ..engine import build_selections, run_families


def migrate_pipeline(cfg: Dict[str, Any], url: str, *, validate_parallel: int | None = None, fanout_parallel: int | None = None) -> Dict[str, Any]:
    run: Dict[str, Any] = {}
    with psycopg.connect(url, connect_timeout=5) as conn:
        run["precopy"] = migrate_precopy(conn, cfg, dsn=url, fanout_parallel=fanout_parallel)

        roots = list(cfg.get('roots') or [])
        pre_roots = [r for r in roots if str(r.get('phase') or '').lower() != 'post']
        post_roots = [r for r in roots if str(r.get('phase') or '').lower() == 'post']
        pre_root_names = {str(r.get('name')) for r in pre_roots if r.get('name')}
        post_root_names = {str(r.get('name')) for r in post_roots if r.get('name')}

        fams = list(cfg.get('table_groups') or [])
        def _fam_root_sel_name(f: dict) -> str | None:
            root = f.get('root') or {}
            return str(root.get('selection')) if root.get('selection') else None

        pre_fams = [f for f in fams if (_fam_root_sel_name(f) in pre_root_names) or (_fam_root_sel_name(f) is None)]
        post_fams = [f for f in fams if _fam_root_sel_name(f) in post_root_names]

        cfg_pre = dict(cfg)
        cfg_pre['roots'] = pre_roots
        cfg_pre['table_groups'] = pre_fams

        cfg_post = dict(cfg)
        cfg_post['roots'] = post_roots
        cfg_post['table_groups'] = post_fams

        run["selections_pre"] = build_selections(conn, cfg_pre)
        created_pre = run_families(conn, cfg_pre, dsn=url, fanout_parallel=fanout_parallel) if cfg_pre.get('table_groups') else []
        run["families_pre_created"] = created_pre

        run["selections_post"] = build_selections(conn, cfg_post)
        created_post = run_families(conn, cfg_post, dsn=url, fanout_parallel=fanout_parallel) if cfg_post.get('table_groups') else []
        run["families_post_created"] = created_post

    if cfg.get('neuter'):
        from ..dbutil import neuter_data
        with psycopg.connect(url, connect_timeout=5) as conn:
            try:
                _ = neuter_data(conn, cfg)
                run["neuter"] = {"result": "applied"}
            except Exception as e:
                run["neuter"] = {"result": "error", "error": str(e)}

    from ..dbutil import reconcile_sequences, migrate_functions, reconcile_all_triggers, mirror_all_constraints
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            _seq_res = reconcile_sequences(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["sequences"] = _seq_res
        except Exception as e:
            run["sequences"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            fn_res = migrate_functions(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["functions"] = fn_res
        except Exception as e:
            run["functions"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            tg_res = reconcile_all_triggers(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["triggers"] = tg_res
        except Exception as e:
            run["triggers"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            created_all = run.get("families_pre_created", []) + run.get("families_post_created", [])
            created_names = [t.split('.', 1)[1] for t in created_all]
            kwargs = {
                'src_schema': str(cfg['source_schema']),
                'dst_schema': str(cfg['dest_schema']),
                'validate_fk_tables': created_names,
                'dsn': url,
            }
            if validate_parallel is not None:
                kwargs['validate_parallel'] = int(validate_parallel)
            _ = mirror_all_constraints(
                conn,
                **kwargs,
            )
            run["constraints"] = {"result": "mirrored"}
        except Exception as e:
            run["constraints"] = {"result": "error", "error": str(e)}
    return run


def restart_pipeline(cfg: Dict[str, Any], url: str, *, validate_parallel: int | None = None, fanout_parallel: int | None = None) -> Dict[str, Any]:
    run: Dict[str, Any] = {}
    from ..dbutil import reset_schema
    with psycopg.connect(url, connect_timeout=5) as conn:
        reset_schema(conn, str(cfg.get('dest_schema')))

        run["precopy"] = migrate_precopy(conn, cfg, dsn=url, fanout_parallel=fanout_parallel)

        roots = list(cfg.get('roots') or [])
        pre_roots = [r for r in roots if str(r.get('phase') or '').lower() != 'post']
        post_roots = [r for r in roots if str(r.get('phase') or '').lower() == 'post']
        pre_root_names = {str(r.get('name')) for r in pre_roots if r.get('name')}

        fams = list(cfg.get('table_groups') or [])
        def _fam_root_sel_name(f: dict) -> str | None:
            root = f.get('root') or {}
            return str(root.get('selection')) if root.get('selection') else None

        pre_fams = [f for f in fams if (_fam_root_sel_name(f) in pre_root_names) or (_fam_root_sel_name(f) is None)]
        post_fams = [f for f in fams if _fam_root_sel_name(f) not in pre_root_names and _fam_root_sel_name(f) is not None]

        cfg_pre = dict(cfg)
        cfg_pre['roots'] = pre_roots
        cfg_pre['table_groups'] = pre_fams

        cfg_post = dict(cfg)
        cfg_post['roots'] = post_roots
        cfg_post['table_groups'] = post_fams

        run["selections_pre"] = build_selections(conn, cfg_pre)
        created_pre = run_families(conn, cfg_pre, dsn=url, fanout_parallel=fanout_parallel) if cfg_pre.get('table_groups') else []
        run["families_pre_created"] = created_pre

        run["selections_post"] = build_selections(conn, cfg_post)
        created_post = run_families(conn, cfg_post, dsn=url, fanout_parallel=fanout_parallel) if cfg_post.get('table_groups') else []
        run["families_post_created"] = created_post

    if cfg.get('neuter'):
        from ..dbutil import neuter_data
        with psycopg.connect(url, connect_timeout=5) as conn:
            try:
                _ = neuter_data(conn, cfg)
                run["neuter"] = {"result": "applied"}
            except Exception as e:
                run["neuter"] = {"result": "error", "error": str(e)}

    from ..dbutil import reconcile_sequences, migrate_functions, reconcile_all_triggers, mirror_all_constraints
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            _seq_res = reconcile_sequences(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["sequences"] = _seq_res
        except Exception as e:
            run["sequences"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            fn_res = migrate_functions(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["functions"] = fn_res
        except Exception as e:
            run["functions"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            tg_res = reconcile_all_triggers(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
            run["triggers"] = tg_res
        except Exception as e:
            run["triggers"] = {"result": "error", "error": str(e)}
    with psycopg.connect(url, connect_timeout=5) as conn:
        try:
            created_all = run.get("families_pre_created", []) + run.get("families_post_created", [])
            created_names = [t.split('.', 1)[1] for t in created_all]
            kwargs = {
                'src_schema': str(cfg['source_schema']),
                'dst_schema': str(cfg['dest_schema']),
                'validate_fk_tables': created_names,
                'dsn': url,
            }
            if validate_parallel is not None:
                kwargs['validate_parallel'] = int(validate_parallel)
            _ = mirror_all_constraints(
                conn,
                **kwargs,
            )
            run["constraints"] = {"result": "mirrored"}
        except Exception as e:
            run["constraints"] = {"result": "error", "error": str(e)}
    return run
