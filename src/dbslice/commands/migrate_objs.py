#!/usr/bin/env python3
from __future__ import annotations

"""Object-level migration commands.

Encapsulates standalone migration commands for:
- sequences, functions
- triggers (all or one table)
- indexes (all or one table)
- constraints (with validation controls)

Inputs
- conn: psycopg connection
- cfg: normalized config dict
- table_or_all: string table name or '__ALL__' where applicable
- only_tables: list of table names for constraints when not '__ALL__'
- skip_validate_fk, validate_parallel, dsn: generic knobs for constraints

Outputs
- Dicts shaped like the CLI’s previous YAML outputs
"""

from typing import Optional

from ..dbutil import (
    reconcile_sequences,
    migrate_functions as _migrate_functions,
    reconcile_all_triggers, reconcile_table_triggers,
    reconcile_all_indexes, reconcile_table_indexes,
    migrate_primary_keys, mirror_all_constraints,
)


def run_migrate_sequences(conn, cfg: dict) -> dict:
    res = reconcile_sequences(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    return {"sequences": res}


def run_migrate_functions(conn, cfg: dict) -> dict:
    res = _migrate_functions(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    return {"functions": res}


def run_migrate_triggers(conn, cfg: dict, table_or_all: Optional[str]) -> dict:
    if table_or_all == '__ALL__':
        res = reconcile_all_triggers(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    else:
        res = reconcile_table_triggers(conn, str(table_or_all), src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    return {"triggers": res}


def run_migrate_indexes(conn, cfg: dict, table_or_all: Optional[str]) -> dict:
    src_schema = str(cfg.get('source_schema'))
    dst_schema = str(cfg.get('dest_schema'))
    if table_or_all == '__ALL__':
        res = reconcile_all_indexes(conn, src_schema=src_schema, dst_schema=dst_schema)
    else:
        res = reconcile_table_indexes(conn, str(table_or_all), src_schema=src_schema, dst_schema=dst_schema)
    return {"indexes": res}


def run_migrate_constraints(conn, cfg: dict, only_tables: Optional[list[str]], *, skip_validate_fk: bool, validate_parallel: Optional[int], dsn: str) -> dict:
    pk_res = migrate_primary_keys(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    kwargs = {
        'src_schema': str(cfg['source_schema']),
        'dst_schema': str(cfg['dest_schema']),
        'only_tables': only_tables,
        'validate_fk_tables': only_tables,
        'validate_fks': (not skip_validate_fk),
        'dsn': dsn,
    }
    if validate_parallel is not None:
        kwargs['validate_parallel'] = int(validate_parallel)
    cons_res = mirror_all_constraints(conn, **kwargs)
    return {"primary_keys": pk_res, "constraints": cons_res}
