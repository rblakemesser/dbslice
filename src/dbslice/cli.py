#!/usr/bin/env python3
import argparse
import os
import sys
from .env import load_dotenv
from .config import load_config
from .audit import audit_all_tables, audit_table
import yaml
from .migrate import migrate_precopy
from .engine import build_selections, run_families
import psycopg


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dbslice CLI (bootstrap)")
    p.add_argument('--env', default=os.path.join(os.getcwd(), '.env'), help='Path to .env file (default: ./\.env)')
    p.add_argument('--check-connection', action='store_true', help='Try connecting to DATABASE_URL')
    p.add_argument('--config', help='Path to YAML config (source/dest schema, etc.). If not provided, uses env DBSLICE_CONFIG')
    p.add_argument('--audit-tables', nargs='?', const='__ALL__', help='Audit table gaps (optional table name; defaults to all)')
    p.add_argument('--migrate', action='store_true', help='Run full migration pipeline (precopy + families)')
    p.add_argument('--migrate-tables', nargs='?', const='__ALL__', help='Migrate only family tables/data. Optional comma-separated list; defaults to all families when no value is given')
    p.add_argument('--reset', help='Comma-separated families to reset (drop dest/tmp/shards artifacts for each)')
    return p.parse_args()


def check_connection() -> int:
    url = os.environ.get('DATABASE_URL')
    if not url:
        print('DATABASE_URL is not set (load it via .env or environment)', file=sys.stderr)
        return 2
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                _ = cur.fetchone()
        finally:
            conn.close()
        print('Connection OK')
        return 0
    except Exception as e:
        print(f'Connection failed: {e}', file=sys.stderr)
        return 1


def main() -> int:
    args = parse_args()

    # Load .env silently; commands produce their own output
    _loaded, _env_path = load_dotenv(args.env)

    if args.check_connection:
        return check_connection()

    if args.reset is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            print('--config is required for --reset (or set DBSLICE_CONFIG in env).', file=sys.stderr)
            return 2
        cfg = load_config(cfg_path)
        fams = cfg.get('families') or []
        requested = [x.strip() for x in str(args.reset).split(',') if x.strip()]
        defined = {str(f.get('name')): f for f in fams if f.get('name')}
        invalid = [name for name in requested if name not in defined]
        if invalid:
            print(f"Unknown families for --reset: {', '.join(invalid)}", file=sys.stderr)
            return 2
        url = os.environ.get('DATABASE_URL')
        if not url:
            print('DATABASE_URL is not set (load it via .env or environment)', file=sys.stderr)
            return 2
        from .dbutil import drop_table_if_exists, list_relations_like, drop_tables_if_exists
        with psycopg.connect(url, connect_timeout=5) as conn:
            dst = str(cfg.get('dest_schema'))
            tmp = str(cfg.get('tmp_schema', 'tmp'))
            shards = str(cfg.get('shards_schema', 'shards'))
            for name in requested:
                fam = defined[name]
                root = str((fam.get('root') or {}).get('table'))
                dep_tables = [str(d.get('table')) for d in (fam.get('deps') or [])]
                tables = [t for t in [root] + dep_tables if t]
                # Batch drop dest and tmp tables
                drop_tables_if_exists(conn, [f"{dst}.\"{t}\"" for t in tables])
                drop_tables_if_exists(conn, [f"{tmp}.\"{t}\"" for t in tables])
                # Drop shard artifacts matching table_sh% and table_pmsh% in batches
                shard_rels: list[str] = []
                for t in tables:
                    for pat in (f"{t}_sh%", f"{t}_pmsh%"):
                        shard_rels.extend(list_relations_like(conn, shards, pat))
                drop_tables_if_exists(conn, [f"{shards}.\"{r}\"" for r in shard_rels])
        print(f"Reset families: {', '.join(requested)}")
        return 0

    if args.audit_tables is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            print('--config is required for --audit-tables (or set DBSLICE_CONFIG in env).', file=sys.stderr)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            print('DATABASE_URL is not set (load it via .env or environment)', file=sys.stderr)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            if args.audit_tables == '__ALL__':
                res = audit_all_tables(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
                print(yaml.safe_dump(res, sort_keys=False, default_flow_style=False))
                return 0
            else:
                rpt = audit_table(conn, str(args.audit_tables), src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
                print(yaml.safe_dump(rpt, sort_keys=False, default_flow_style=False))
                return 0

    if args.migrate:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            print('--config is required for --migrate (or set DBSLICE_CONFIG in env).', file=sys.stderr)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            print('DATABASE_URL is not set (load it via .env or environment)', file=sys.stderr)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            # Phase 1: Precopy
            precopy_summary = migrate_precopy(conn, cfg)
            # Phase 2: Root selections (if any)
            selections = build_selections(conn, cfg)
            # Phase 3: Families (unsharded) if defined
            created_tables = run_families(conn, cfg) if cfg.get('families') else []
        print(yaml.safe_dump({
            "precopy": precopy_summary,
            "selections": selections,
            "families_created": created_tables,
        }, sort_keys=False, default_flow_style=False))
        return 0

    if args.migrate_tables is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            print('--config is required for --migrate-tables (or set DBSLICE_CONFIG in env).', file=sys.stderr)
            return 2
        cfg = load_config(cfg_path)
        fams = cfg.get('families') or []
        requested = [] if args.migrate_tables == '__ALL__' else [x.strip() for x in str(args.migrate_tables).split(',') if x.strip()]
        defined_names = [str(f.get('name')) for f in fams if f.get('name')]
        invalid = [n for n in requested if n and n not in defined_names]
        if invalid:
            print(f"Unknown families for --migrate-tables: {', '.join(invalid)}", file=sys.stderr)
            return 2
        # Filter families, preserving config order and only including requested names
        filtered = fams if not requested else [f for f in fams if str(f.get('name')) in set(requested)]
        filtered_cfg = dict(cfg)
        filtered_cfg['families'] = filtered
        url = os.environ.get('DATABASE_URL')
        if not url:
            print('DATABASE_URL is not set (load it via .env or environment)', file=sys.stderr)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            # Only families/data: skip precopy
            selections = build_selections(conn, filtered_cfg)
            created_tables = run_families(conn, filtered_cfg) if filtered_cfg.get('families') else []
        print(yaml.safe_dump({
            "selections": selections,
            "families_created": created_tables,
        }, sort_keys=False, default_flow_style=False))
        return 0

    print('dbslice ready (no action requested). Try --audit-tables or --migrate.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
