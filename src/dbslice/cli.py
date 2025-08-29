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
from .commands.pipeline import migrate_pipeline, restart_pipeline
from .commands.swap import run_swap
from .commands.reset import run_reset
from .commands.neuter import run_neuter
from .commands.audit_cmds import run_audit_tables, run_audit_sequences
from .commands.migrate_objs import (
    run_migrate_sequences,
    run_migrate_functions,
    run_migrate_triggers,
    run_migrate_indexes,
    run_migrate_constraints,
)
from .commands.migrate_tables_cmd import run_migrate_tables
from .commands.pipeline import migrate_pipeline, restart_pipeline
import psycopg
import time
from .logsetup import setup_logging, install_psycopg_query_logging


def _fmt_duration(seconds: float) -> str:
    total = int(seconds)
    mins, secs = divmod(total, 60)
    hours, mins = divmod(mins, 60)
    return f"{hours}h{mins}m{secs}s" if hours else f"{mins}m{secs}s"


def _emit(request: dict, run: dict, start_ts: float) -> None:
    """Print a uniform YAML envelope with request/run/runtime (runtime last)."""
    env = {}
    env["request"] = request
    env["run"] = run
    env["runtime"] = _fmt_duration(time.time() - start_ts)
    print(yaml.safe_dump(env, sort_keys=False, default_flow_style=False))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dbslice CLI (bootstrap)")
    p.add_argument('--env', default=os.path.join(os.getcwd(), '.env'), help='Path to .env file (default: ./\.env)')
    p.add_argument('--check-connection', action='store_true', help='Try connecting to DATABASE_URL')
    p.add_argument('--config', help='Path to YAML config (source/dest schema, etc.). If not provided, uses env DBSLICE_CONFIG')
    p.add_argument('--audit-tables', nargs='?', const='__ALL__', help='Audit table gaps (optional table name; defaults to all)')
    p.add_argument('--audit-sequences', action='store_true', help='Audit sequences across schemas and exit')
    p.add_argument('--migrate-sequences', action='store_true', help='Reconcile sequences and defaults from source->dest and exit')
    p.add_argument('--precopy-only', action='store_true', help='Run only the precopy phase (schema-only + full-copy) and exit')
    p.add_argument('--migrate-functions', action='store_true', help='Migrate (create or replace) functions from source->dest and exit')
    p.add_argument('--migrate-triggers', nargs='?', const='__ALL__', help='Reconcile triggers from source->dest (optional table name; defaults to all)')
    p.add_argument('--swap', action='store_true', help='Swap schemas: public->old, dest_schema->public')
    p.add_argument('--unswap', action='store_true', help='Unswap schemas: public->dest_schema, old->public')
    p.add_argument('--migrate-indexes', nargs='?', const='__ALL__', help='Reconcile non-PK indexes (optional table name; defaults to all)')
    p.add_argument('--migrate-constraints', nargs='?', const='__ALL__', help='Mirror UNIQUE/CHECK/EXCLUSION/FK constraints from source->dest (optional table name; defaults to all)')
    p.add_argument('--validate-parallel', type=int, help='Global concurrency for FK validation across tables (default: 16; use 1 to run sequentially)')
    p.add_argument('--skip-validate-fk', action='store_true', help='Skip validating NOT VALID foreign keys (adds as NOT VALID only)')
    p.add_argument('--fanout-parallel', type=int, help='Global concurrency for shard fanout (CTAS and inserts) using async connections')
    p.add_argument('--neuter-only', action='store_true', help='Run neuter step only (per config) against dest schema')
    p.add_argument('--migrate', action='store_true', help='Run full migration pipeline (precopy + table groups)')
    p.add_argument('--migrate-tables', nargs='?', const='__ALL__', help='Migrate only table groups (root+deps). Optional comma-separated list; defaults to all table groups when no value is given')
    p.add_argument('--restart', action='store_true', help='Drop and recreate dest schema only; combine with --migrate or --migrate-tables to proceed')
    p.add_argument('--reset', help='Comma-separated table groups to reset (drop dest/tmp/shards artifacts for each)')
    return p.parse_args()


def check_connection(start_ts: float, *, env_path: str) -> int:
    url = os.environ.get('DATABASE_URL')
    request = {
        "action": "check_connection",
        "args": sys.argv[1:],
        "env_file": env_path,
        "database_url": url,
    }
    if not url:
        msg = 'DATABASE_URL is not set (load it via .env or environment)'
        print(msg, file=sys.stderr)
        _emit(request, {"result": "error", "error": msg}, start_ts)
        return 2
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                _ = cur.fetchone()
        finally:
            conn.close()
        _emit(request, {"result": "ok"}, start_ts)
        return 0
    except Exception as e:
        print(f'Connection failed: {e}', file=sys.stderr)
        _emit(request, {"result": "error", "error": str(e)}, start_ts)
        return 1


def main() -> int:
    args = parse_args()
    start_ts = time.time()
    
    # Load .env silently; commands produce their own output
    _loaded, _env_path = load_dotenv(args.env)

    # Initialize logging to file and install psycopg query logging
    logger = setup_logging()
    install_psycopg_query_logging(logger)

    if args.check_connection:
        return check_connection(start_ts, env_path=args.env)

    if args.swap or args.unswap:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        action = 'swap' if args.swap else 'unswap'
        if not cfg_path:
            msg = '--config is required for --swap/--unswap (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": action, "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": action, "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        dest_schema = str(cfg.get('dest_schema'))
        if not dest_schema:
            msg = 'dest_schema missing from config'
            print(msg, file=sys.stderr)
            _emit({"action": action, "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_swap(conn, dest_schema, do_swap=bool(args.swap))
        req = {"action": action, "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}
        _emit(req, run, start_ts)
        return 0

    if args.reset is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --reset (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "reset", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        fams = cfg.get('table_groups') or []
        requested = [x.strip() for x in str(args.reset).split(',') if x.strip()]
        defined = {str(f.get('name')): f for f in fams if f.get('name')}
        invalid = [name for name in requested if name not in defined]
        if invalid:
            msg = f"Unknown table groups for --reset: {', '.join(invalid)}"
            print(msg, file=sys.stderr)
            _emit({"action": "reset", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "reset", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            _ = run_reset(conn, cfg, requested)
        req = {"action": "reset", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}
        _emit(req, {"result": "reset", "table_groups": requested}, start_ts)
        return 0

    if args.neuter_only:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --neuter-only (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "neuter_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "neuter_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_neuter(conn, cfg)
        req = {"action": "neuter_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}
        _emit(req, run, start_ts)
        return 0

    if args.audit_tables is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --audit-tables (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "audit_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "audit_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        req = {
            "action": "audit_tables",
            "args": sys.argv[1:],
            "env_file": args.env,
            "config": cfg_path,
            "database_url": url,
            "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])},
        }
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_audit_tables(conn, cfg, str(args.audit_tables))
        _emit(req, run, start_ts)
        return 0

    if args.audit_sequences:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --audit-sequences (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "audit_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "audit_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            res = run_audit_sequences(conn, cfg)
        req = {"action": "audit_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, res, start_ts)
        return 0

    if args.migrate_sequences:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-sequences (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_migrate_sequences(conn, cfg)
        req = {"action": "migrate_sequences", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.migrate_functions:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-functions (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_functions", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_functions", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_migrate_functions(conn, cfg)
        req = {"action": "migrate_functions", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.migrate_triggers is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-triggers (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_triggers", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_triggers", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_migrate_triggers(conn, cfg, str(args.migrate_triggers))
        req = {"action": "migrate_triggers", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.precopy_only:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --precopy-only (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "precopy_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "precopy_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            pc = migrate_precopy(conn, cfg, dsn=url, fanout_parallel=args.fanout_parallel)
        req = {"action": "precopy_only", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, {"precopy": pc}, start_ts)
        return 0

    # Handle restart combinations first so restart acts as a modifier
    if args.restart and args.migrate:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --restart with --migrate (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "restart+migrate", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "restart+migrate", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        # Reset destination schema, then run full migrate pipeline
        from .dbutil import reset_schema
        with psycopg.connect(url, connect_timeout=5) as conn:
            reset_schema(conn, str(cfg.get('dest_schema')))
        run = migrate_pipeline(cfg, url, validate_parallel=args.validate_parallel, fanout_parallel=args.fanout_parallel)
        req = {
            "action": "restart+migrate",
            "args": sys.argv[1:],
            "env_file": args.env,
            "config": cfg_path,
            "database_url": url,
            "schemas": {
                "src": str(cfg['source_schema']),
                "dst": str(cfg['dest_schema']),
                "tmp": str(cfg.get('tmp_schema', 'tmp')),
                "shards": str(cfg.get('shards_schema', 'shards')),
            },
        }
        _emit(req, run, start_ts)
        return 0

    if args.restart and args.migrate_tables is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --restart with --migrate-tables (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "restart+migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        fams = cfg.get('table_groups') or []
        requested = [] if args.migrate_tables == '__ALL__' else [x.strip() for x in str(args.migrate_tables).split(',') if x.strip()]
        defined_names = [str(f.get('name')) for f in fams if f.get('name')]
        invalid = [n for n in requested if n and n not in defined_names]
        if invalid:
            msg = f"Unknown table groups for --restart+--migrate-tables: {', '.join(invalid)}"
            print(msg, file=sys.stderr)
            _emit({"action": "restart+migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "restart+migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        # Reset destination schema, then run migrate-tables flow
        from .dbutil import reset_schema
        with psycopg.connect(url, connect_timeout=5) as conn:
            reset_schema(conn, str(cfg.get('dest_schema')))
            run = run_migrate_tables(conn, cfg, requested, dsn=url, fanout_parallel=args.fanout_parallel)
        req = {"action": "restart+migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.migrate:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        run = migrate_pipeline(cfg, url, validate_parallel=args.validate_parallel, fanout_parallel=args.fanout_parallel)
        req = {
            "action": "migrate",
            "args": sys.argv[1:],
            "env_file": args.env,
            "config": cfg_path,
            "database_url": url,
            "schemas": {
                "src": str(cfg['source_schema']),
                "dst": str(cfg['dest_schema']),
                "tmp": str(cfg.get('tmp_schema', 'tmp')),
                "shards": str(cfg.get('shards_schema', 'shards')),
            },
        }
        _emit(req, run, start_ts)
        return 0

    if args.migrate_indexes is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-indexes (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_indexes", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_indexes", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        src_schema = str(cfg.get('source_schema'))
        dst_schema = str(cfg.get('dest_schema'))
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_migrate_indexes(conn, {'source_schema': src_schema, 'dest_schema': dst_schema}, str(args.migrate_indexes))
        req = {"action": "migrate_indexes", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": src_schema, "dst": dst_schema}}
        _emit(req, run, start_ts)
        return 0

    if args.migrate_constraints is not None:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-constraints (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_constraints", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_constraints", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            only_tbls = None if args.migrate_constraints == '__ALL__' else [str(args.migrate_constraints)]
            run = run_migrate_constraints(conn, cfg, only_tbls, skip_validate_fk=bool(args.skip_validate_fk), validate_parallel=args.validate_parallel, dsn=url)
        req = {"action": "migrate_constraints", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.migrate_tables is not None and not args.restart:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --migrate-tables (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        fams = cfg.get('table_groups') or []
        requested = [] if args.migrate_tables == '__ALL__' else [x.strip() for x in str(args.migrate_tables).split(',') if x.strip()]
        defined_names = [str(f.get('name')) for f in fams if f.get('name')]
        invalid = [n for n in requested if n and n not in defined_names]
        if invalid:
            msg = f"Unknown table groups for --migrate-tables: {', '.join(invalid)}"
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        with psycopg.connect(url, connect_timeout=5) as conn:
            run = run_migrate_tables(conn, cfg, requested, dsn=url, fanout_parallel=args.fanout_parallel)
        req = {"action": "migrate_tables", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    if args.restart:
        cfg_path = args.config or os.environ.get('DBSLICE_CONFIG')
        if not cfg_path:
            msg = '--config is required for --restart (or set DBSLICE_CONFIG in env).'
            print(msg, file=sys.stderr)
            _emit({"action": "restart", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": os.environ.get('DATABASE_URL')}, {"result": "error", "error": msg}, start_ts)
            return 2
        cfg = load_config(cfg_path)
        url = os.environ.get('DATABASE_URL')
        if not url:
            msg = 'DATABASE_URL is not set (load it via .env or environment)'
            print(msg, file=sys.stderr)
            _emit({"action": "restart", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url}, {"result": "error", "error": msg}, start_ts)
            return 2
        # Only drop/recreate destination schema, do not run migrate steps unless combined above
        from .dbutil import reset_schema
        with psycopg.connect(url, connect_timeout=5) as conn:
            reset_schema(conn, str(cfg.get('dest_schema')))
        run = {"result": "reset", "schema": str(cfg.get('dest_schema'))}
        req = {"action": "restart", "args": sys.argv[1:], "env_file": args.env, "config": cfg_path, "database_url": url, "schemas": {"src": str(cfg['source_schema']), "dst": str(cfg['dest_schema'])}}
        _emit(req, run, start_ts)
        return 0

    _emit({"action": "noop", "args": sys.argv[1:], "env_file": args.env, "database_url": os.environ.get('DATABASE_URL')}, {"result": "noop", "message": 'dbslice ready (no action requested). Try --audit-tables or --migrate.'}, start_ts)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
