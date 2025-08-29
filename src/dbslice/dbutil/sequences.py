#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Tuple
import re

from .introspect import table_exists, column_exists


def _list_sequences(cur, schema: str) -> List[str]:
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S' AND n.nspname = %s
        ORDER BY c.relname
        """,
        (schema,),
    )
    return [str(r[0]) for r in (cur.fetchall() or [])]


def _get_sequence_props(cur, schema: str, name: str) -> Dict[str, object] | None:
    cur.execute(
        """
        SELECT increment_by, min_value, max_value, start_value, cache_size, cycle
        FROM pg_sequences
        WHERE schemaname = %s AND sequencename = %s
        """,
        (schema, name),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "increment_by": int(row[0]),
        "min_value": int(row[1]),
        "max_value": int(row[2]),
        "start_value": int(row[3]),
        "cache_size": int(row[4]),
        "cycle": bool(row[5]),
    }


def _ensure_sequence_exists_like(conn, *, src_schema: str, seq_name: str, dst_schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'S' AND n.nspname = %s AND c.relname = %s
            """,
            (dst_schema, seq_name),
        )
        exists = cur.fetchone() is not None
    if exists:
        return False
    with conn.cursor() as cur:
        cur.execute(f'CREATE SEQUENCE IF NOT EXISTS "{dst_schema}"."{seq_name}"')
    conn.commit()
    return True


def create_missing_sequence(
    conn,
    *,
    target_schema: str,
    sequence_name: str,
    next_value: int | None = None,
    owned_by: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SEQUENCE IF NOT EXISTS "{target_schema}"."{sequence_name}"')
    conn.commit()
    if next_value is not None:
        with conn.cursor() as cur:
            cur.execute('SELECT setval(%s, %s, false)', (f'{target_schema}.{sequence_name}', int(next_value)))
        conn.commit()
    if owned_by:
        try:
            sch, tbl, col = owned_by.split('.')
        except ValueError:
            sch = tbl = col = None
        if sch and tbl and col and table_exists(conn, sch, tbl) and column_exists(conn, sch, tbl, col):
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER SEQUENCE "{target_schema}"."{sequence_name}" OWNED BY "{sch}"."{tbl}"."{col}"'
                )
            conn.commit()


def _fetch_serial_defaults(cur, schema: str) -> Dict[Tuple[str, str], Tuple[str | None, str]]:
    cur.execute(
        """
        SELECT table_name, column_name, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND column_default LIKE %s
        """,
        (schema, "nextval(%"),
    )
    out: Dict[Tuple[str, str], Tuple[str | None, str]] = {}
    for tbl, col, default_expr in (cur.fetchall() or []):
        if not default_expr:
            continue
        m = re.search(r"nextval\('\"?([\w]+)\"?\.?\"?([\w]+)?\"?'::regclass\)", str(default_expr))
        if not m:
            continue
        if m.group(2):
            out[(str(tbl), str(col))] = (str(m.group(1)), str(m.group(2)))
        else:
            out[(str(tbl), str(col))] = (None, str(m.group(1)))
    return out


def _column_default(conn, schema: str, table: str, column: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (schema, table, column),
        )
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else None


def _set_default_to_sequence(conn, schema: str, table: str, column: str, dst_schema: str, seq_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"ALTER TABLE \"{schema}\".\"{table}\" ALTER COLUMN \"{column}\" SET DEFAULT nextval('{dst_schema}.{seq_name}'::regclass)"
        )
    conn.commit()


def _set_sequence_value(conn, schema: str, name: str, target_max: int) -> None:
    with conn.cursor() as cur:
        if int(target_max) < 1:
            cur.execute('SELECT setval(%s, %s, false)', (f'{schema}.{name}', 1))
        else:
            cur.execute('SELECT setval(%s, %s, true)', (f'{schema}.{name}', int(target_max)))
    conn.commit()


def reconcile_sequences(
    conn,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
    drop_extraneous: bool = True,
) -> Dict[str, int]:
    created = 0
    aligned_owned = 0
    aligned_next = 0
    dropped = 0

    def _fetch_seq_core(cur, schema: str, name: str) -> Tuple[int | None, int | None, bool | None]:
        cur.execute(f'SELECT last_value, is_called FROM "{schema}"."{name}"')
        row = cur.fetchone()
        if not row:
            return None, None, None
        last_value, is_called = int(row[0]), bool(row[1])
        cur.execute(
            """
            SELECT increment_by
            FROM pg_sequences
            WHERE schemaname = %s AND sequencename = %s
            """,
            (schema, name),
        )
        inc_row = cur.fetchone()
        increment_by = int(inc_row[0]) if inc_row else 1
        next_value = last_value if (is_called is False) else last_value + increment_by
        return next_value, increment_by, is_called

    def _fetch_seq_owned_by(cur, schema: str, name: str) -> str | None:
        cur.execute(
            """
            SELECT tns.nspname AS table_schema, t.relname AS table_name, a.attname AS column_name
            FROM pg_class seq
            JOIN pg_namespace seq_ns ON seq_ns.oid = seq.relnamespace
            LEFT JOIN pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
            LEFT JOIN pg_class t ON t.oid = d.refobjid
            LEFT JOIN pg_namespace tns ON tns.oid = t.relnamespace
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
            WHERE seq.relkind = 'S' AND seq_ns.nspname = %s AND seq.relname = %s
            """,
            (schema, name),
        )
        row = cur.fetchone()
        if row and row[0] and row[1] and row[2]:
            return f"{row[0]}.{row[1]}.{row[2]}"
        return None

    with conn.cursor() as cur:
        src_list = _list_sequences(cur, src_schema)
        dst_list = _list_sequences(cur, dst_schema)

    if drop_extraneous:
        for name in sorted(set(dst_list) - set(src_list)):
            with conn.cursor() as cur:
                cur.execute(f'DROP SEQUENCE IF EXISTS "{dst_schema}"."{name}" CASCADE')
            conn.commit()
            dropped += 1

    for name in sorted(set(src_list) - set(dst_list)):
        with conn.cursor() as cur:
            src_next, _, _ = _fetch_seq_core(cur, src_schema, name)
            owned_src = _fetch_seq_owned_by(cur, src_schema, name)
        owned_dst = None
        if owned_src:
            try:
                _, tbl, col = owned_src.split('.')
                owned_dst = f"{dst_schema}.{tbl}.{col}"
            except ValueError:
                owned_dst = None
        create_missing_sequence(conn, target_schema=dst_schema, sequence_name=name, next_value=src_next, owned_by=owned_dst)
        created += 1
        if src_next is not None:
            aligned_next += 1
        if owned_dst:
            aligned_owned += 1

    for name in sorted(set(src_list) & set(dst_list)):
        with conn.cursor() as cur:
            src_next, _, _ = _fetch_seq_core(cur, src_schema, name)
            dst_next, _, _ = _fetch_seq_core(cur, dst_schema, name)
            owned_src = _fetch_seq_owned_by(cur, src_schema, name)
            owned_dst = _fetch_seq_owned_by(cur, dst_schema, name)

        if src_next is not None and dst_next != src_next:
            with conn.cursor() as cur:
                cur.execute('SELECT setval(%s, %s, false)', (f'{dst_schema}.{name}', int(src_next)))
            conn.commit()
            aligned_next += 1

        desired_owned = None
        if owned_src:
            try:
                _, tbl, col = owned_src.split('.')
                desired_owned = f"{dst_schema}.{tbl}.{col}"
            except ValueError:
                desired_owned = None
        if desired_owned != owned_dst:
            with conn.cursor() as cur:
                if desired_owned:
                    sch, tbl, col = desired_owned.split('.')
                    if table_exists(conn, sch, tbl) and column_exists(conn, sch, tbl, col):
                        cur.execute(f'ALTER SEQUENCE "{dst_schema}"."{name}" OWNED BY "{sch}"."{tbl}"."{col}"')
                else:
                    cur.execute(f'ALTER SEQUENCE "{dst_schema}"."{name}" OWNED BY NONE')
            conn.commit()
            aligned_owned += 1

    return {"created": created, "aligned_owned_by": aligned_owned, "aligned_next": aligned_next, "dropped": dropped}

