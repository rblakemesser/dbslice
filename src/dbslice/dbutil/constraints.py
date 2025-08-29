#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple
import re
import psycopg
import asyncio

from .introspect import list_tables_in_schema, table_exists


def _fetch_fk_child_parent_pairs(cur, schema: str) -> List[Tuple[str, str]]:
    cur.execute(
        """
        SELECT child.relname AS child_table, parent.relname AS parent_table
        FROM pg_constraint con
        JOIN pg_class child ON child.oid = con.conrelid
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        JOIN pg_class parent ON parent.oid = con.confrelid
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        WHERE con.contype = 'f' AND child_ns.nspname = %s AND parent_ns.nspname = %s
        ORDER BY child.relname
        """,
        (schema, schema),
    )
    rows = cur.fetchall() or []
    return [(str(r[0]), str(r[1])) for r in rows]


def preflight_check(conn, *, src_schema: str, dst_schema: str) -> Dict[str, object]:
    from .introspect import list_unlogged_tables, get_primary_key, has_primary_key
    unlogged = list_unlogged_tables(conn, dst_schema)
    src_tables = set(list_tables_in_schema(conn, src_schema))
    dst_tables = set(list_tables_in_schema(conn, dst_schema))
    pk_missing: List[str] = []
    for tbl in sorted(src_tables & dst_tables):
        if has_primary_key(conn, dst_schema, tbl):
            continue
        if get_primary_key(conn, src_schema, tbl):
            pk_missing.append(tbl)

    with conn.cursor() as cur:
        fk_pairs = _fetch_fk_child_parent_pairs(cur, src_schema)
    fk_unlogged: List[Tuple[str, str]] = []
    for child, parent in fk_pairs:
        if child in dst_tables and parent in dst_tables:
            if child in unlogged or parent in unlogged:
                fk_unlogged.append((child, parent))

    ok = not unlogged and not pk_missing and not fk_unlogged
    return {
        'ok': ok,
        'unlogged_tables': unlogged,
        'pk_missing': pk_missing,
        'fk_prereq': {
            'unlogged_pairs': fk_unlogged,
        },
    }


def _fetch_constraints_map(cur, schema: str, table: str) -> Dict[str, Dict[str, str]]:
    cur.execute(
        """
        SELECT con.conname, con.contype, pg_get_constraintdef(con.oid, true) AS defn
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        ORDER BY con.conname
        """,
        (schema, table),
    )
    rows = cur.fetchall() or []
    out: Dict[str, Dict[str, str]] = {"u": {}, "c": {}, "x": {}, "f": {}}
    for name, typ, defn in rows:
        if typ in out:
            out[typ][str(name)] = str(defn)
    return out


def _ensure_constraint(conn, dst_schema: str, table: str, name: str, defn: str) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(f'SET LOCAL search_path = "{dst_schema}"')
            cur.execute(f'ALTER TABLE "{dst_schema}"."{table}" ADD CONSTRAINT "{name}" {defn}')
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        is_unique = 'UNIQUE' in defn.upper()
        is_deferrable = 'DEFERRABLE' in defn.upper()
        if is_unique and is_deferrable:
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SET LOCAL search_path = "{dst_schema}"')
                    cur.execute(f'DROP INDEX IF EXISTS "{dst_schema}"."{name}"')
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SET LOCAL search_path = "{dst_schema}"')
                    cur.execute(f'ALTER TABLE "{dst_schema}"."{table}" ADD CONSTRAINT "{name}" {defn}')
                conn.commit()
                return True
            except Exception:
                conn.rollback()
        if is_unique and not is_deferrable:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1 FROM pg_class i
                        JOIN pg_namespace n ON n.oid = i.relnamespace
                        WHERE i.relkind='i' AND n.nspname=%s AND i.relname=%s
                        LIMIT 1
                        """,
                        (dst_schema, name),
                    )
                    if cur.fetchone():
                        cur.execute(f'SET LOCAL search_path = "{dst_schema}"')
                        cur.execute(f'ALTER TABLE "{dst_schema}"."{table}" ADD CONSTRAINT "{name}" UNIQUE USING INDEX "{name}"')
                        conn.commit()
                        return True
            except Exception:
                conn.rollback()
        return False


def migrate_primary_keys(
    conn,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
) -> Dict[str, int]:
    from .introspect import get_primary_key, has_primary_key
    from .ddl import add_primary_key
    added = 0
    for tbl in list_tables_in_schema(conn, src_schema):
        if not table_exists(conn, dst_schema, tbl):
            continue
        if has_primary_key(conn, dst_schema, tbl):
            continue
        src_pk = get_primary_key(conn, src_schema, tbl)
        if not src_pk:
            continue
        cname, cols = src_pk
        add_primary_key(conn, dst_schema, tbl, cols, cname)
        conn.commit()
        added += 1
    return {"added": added}


def mirror_all_constraints(
    conn,
    *,
    src_schema: str = 'public',
    dst_schema: str = 'stage',
    only_tables: Sequence[str] | None = None,
    validate_fk_tables: Sequence[str] | None = None,
    validate_fks: bool = True,
    validate_parallel: int = 16,
    validate_across_tables_only: bool = True,
    dsn: str | None = None,
) -> Dict[str, int]:
    def _canon(defn: str, *, is_fk: bool = False) -> str:
        d = re.sub(r"\s+", " ", (defn or '').strip())
        if is_fk:
            d = re.sub(r"\s+NOT\s+VALID\b", "", d, flags=re.IGNORECASE)
        return d

    created = 0
    replaced = 0
    dropped = 0
    validated_fk = 0

    _ = migrate_primary_keys(conn, src_schema=src_schema, dst_schema=dst_schema)

    only_set = set([t for t in (only_tables or [])]) if only_tables is not None else None
    for tbl in list_tables_in_schema(conn, src_schema):
        if only_set is not None and tbl not in only_set:
            continue
        if not table_exists(conn, dst_schema, tbl):
            continue
        with conn.cursor() as cur:
            src_map = _fetch_constraints_map(cur, src_schema, tbl)
            dst_map = _fetch_constraints_map(cur, dst_schema, tbl)

        for typ in ("u", "c", "x", "f"):
            src_defs = src_map.get(typ, {})
            dst_defs = dst_map.get(typ, {})

            def _qualify_fk(defn: str) -> str:
                return re.sub(
                    r"(REFERENCES\s+)(?:\"?[A-Za-z_][\w$]*\"?\.)?\"?([A-Za-z_][\w$]*)\"?",
                    f'\\1"{dst_schema}"."\\2"',
                    defn,
                    count=1,
                    flags=re.IGNORECASE,
                )

            src_norm = {}
            for name, defn in src_defs.items():
                d = re.sub(rf'\b{re.escape(src_schema)}\.', f'{dst_schema}.', defn)
                if typ == 'f':
                    d = _qualify_fk(d)
                src_norm[name] = _canon(d, is_fk=(typ == 'f'))
            dst_norm = {name: _canon(defn, is_fk=(typ == 'f')) for name, defn in dst_defs.items()}

            def _fk_ref_table(defn: str) -> str | None:
                m = re.search(r"REFERENCES\s+(?:\"?[A-Za-z_][\w$]*\"?\.)?\"?([A-Za-z_][\w$]*)\"?", defn, flags=re.IGNORECASE)
                return str(m.group(1)) if m else None

            for name in set(src_norm.keys()) & set(dst_norm.keys()):
                if src_norm[name] != dst_norm[name]:
                    with conn.cursor() as cur:
                        cur.execute(f'ALTER TABLE "{dst_schema}"."{tbl}" DROP CONSTRAINT IF EXISTS "{name}"')
                    conn.commit()
                    d = re.sub(rf'\b{re.escape(src_schema)}\.', f'{dst_schema}.', src_defs[name])
                    if typ == 'f':
                        d = _qualify_fk(d)
                    if typ == 'f' and 'NOT VALID' not in d.upper():
                        d = d + ' NOT VALID'
                    ok = _ensure_constraint(conn, dst_schema, tbl, name, d)
                    if not ok:
                        raise RuntimeError(f'Failed to replace constraint {dst_schema}.{tbl}.{name}')
                    replaced += 1

            for name in dst_norm.keys() - src_norm.keys():
                with conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE "{dst_schema}"."{tbl}" DROP CONSTRAINT IF EXISTS "{name}"')
                conn.commit()
                dropped += 1

            for name in src_norm.keys() - dst_norm.keys():
                d = re.sub(rf'\b{re.escape(src_schema)}\.', f'{dst_schema}.', src_defs[name])
                if typ == 'f' and 'NOT VALID' not in d.upper():
                    d = d + ' NOT VALID'
                if typ == 'f':
                    d = _qualify_fk(d)
                ok = _ensure_constraint(conn, dst_schema, tbl, name, d)
                if not ok:
                    raise RuntimeError(f'Failed to add constraint {dst_schema}.{tbl}.{name}')
                created += 1

        if validate_fks:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT con.conname
                    FROM pg_constraint con
                    JOIN pg_class c ON c.oid = con.conrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s AND con.contype='f' AND NOT con.convalidated
                    ORDER BY con.conname
                    """,
                    (dst_schema, tbl),
                )
                fks = [(tbl, r[0]) for r in (cur.fetchall() or [])]
            if validate_fk_tables is not None:
                allowed = set(validate_fk_tables)
                fks = [(t, n) for (t, n) in fks if t in allowed]

            if validate_parallel and validate_parallel > 1 and fks:
                dsn_local = dsn
                if not dsn_local:
                    try:
                        dsn_local = str(conn.info.dsn)
                    except Exception:
                        dsn_local = None
                if dsn_local:
                    async def _validate_one(aconn, table: str, conname: str) -> None:
                        async with aconn.cursor() as cur2:
                            await cur2.execute(
                                f'ALTER TABLE "{dst_schema}"."{table}" VALIDATE CONSTRAINT "{conname}"'
                            )
                        await aconn.commit()

                    async def _runner() -> int:
                        sem = asyncio.Semaphore(int(validate_parallel))
                        errors: list[Exception] = []

                        if validate_across_tables_only:
                            from collections import OrderedDict
                            groups: "OrderedDict[str, list[str]]" = OrderedDict()
                            for t, n in fks:
                                groups.setdefault(t, []).append(n)

                            async def _bounded_table(table: str, connames: list[str]) -> None:
                                async with sem:
                                    try:
                                        async with await psycopg.AsyncConnection.connect(dsn_local, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                                            for conname in connames:
                                                await _validate_one(aconn, table, conname)
                                    except Exception as e:
                                        errors.append(e)

                            tasks = [asyncio.create_task(_bounded_table(t, names)) for t, names in groups.items()]
                            await asyncio.gather(*tasks)
                            if errors:
                                raise errors[0]
                            return len(fks)
                        else:
                            async def _bounded(table: str, conname: str) -> None:
                                async with sem:
                                    try:
                                        async with await psycopg.AsyncConnection.connect(dsn_local, connect_timeout=5) as aconn:  # type: ignore[attr-defined]
                                            await _validate_one(aconn, table, conname)
                                    except Exception as e:
                                        errors.append(e)

                            tasks = [asyncio.create_task(_bounded(t, n)) for (t, n) in fks]
                            await asyncio.gather(*tasks)
                            if errors:
                                raise errors[0]
                            return len(fks)

                    try:
                        validated_fk += asyncio.run(_runner())
                    except Exception as e:
                        raise RuntimeError(f'FK validation failed (async): {e}')
                else:
                    for t, name in fks:
                        with conn.cursor() as cur:
                            cur.execute(f'ALTER TABLE "{dst_schema}"."{t}" VALIDATE CONSTRAINT "{name}"')
                        conn.commit()
                        validated_fk += 1
            else:
                for t, name in fks:
                    with conn.cursor() as cur:
                        cur.execute(f'ALTER TABLE "{dst_schema}"."{t}" VALIDATE CONSTRAINT "{name}"')
                    conn.commit()
                    validated_fk += 1

    return {
        "created": created,
        "replaced": replaced,
        "dropped": dropped,
        "validated_fk": validated_fk,
    }

