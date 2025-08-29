#!/usr/bin/env python3
from __future__ import annotations

# Re-export public API to preserve imports like `from dbslice.dbutil import ...`.

from .introspect import (
    table_exists,
    column_exists,
    get_column_char_max_length,
    get_primary_key,
    has_primary_key,
    list_tables_in_schema,
    list_unlogged_tables,
    is_unlogged_table,
    schema_exists,
)

from .ddl import (
    ensure_schemas,
    rename_schema,
    refresh_all_matviews,
    swap_schemas,
    unswap_schemas,
    reset_schema,
    move_to_schema,
    analyze_table,
    set_logged,
    drop_table_if_exists,
    drop_tables_if_exists,
    list_relations_like,
    add_primary_key,
    create_schema_only_table,
    full_copy_table,
)

from .indexes import (
    recreate_regular_indexes,
    reconcile_table_indexes,
    reconcile_all_indexes,
)

from .constraints import (
    preflight_check,
    mirror_all_constraints,
    migrate_primary_keys,
)

from .sequences import (
    create_missing_sequence,
    reconcile_sequences,
)

from .functions import (
    migrate_functions,
)

from .triggers import (
    reconcile_table_triggers,
    reconcile_all_triggers,
)

from .neuter import (
    neuter_data,
)

__all__ = [
    # introspect
    'table_exists', 'column_exists', 'get_column_char_max_length', 'get_primary_key', 'has_primary_key',
    'list_tables_in_schema', 'list_unlogged_tables', 'is_unlogged_table', 'schema_exists',
    # ddl
    'ensure_schemas', 'rename_schema', 'refresh_all_matviews', 'swap_schemas', 'unswap_schemas', 'reset_schema',
    'move_to_schema', 'analyze_table', 'set_logged', 'drop_table_if_exists', 'drop_tables_if_exists',
    'list_relations_like', 'add_primary_key', 'create_schema_only_table', 'full_copy_table',
    # indexes
    'recreate_regular_indexes', 'reconcile_table_indexes', 'reconcile_all_indexes',
    # constraints
    'preflight_check', 'mirror_all_constraints', 'migrate_primary_keys',
    # sequences
    'create_missing_sequence', 'reconcile_sequences',
    # functions
    'migrate_functions',
    # triggers
    'reconcile_table_triggers', 'reconcile_all_triggers',
    # neuter
    'neuter_data',
]
