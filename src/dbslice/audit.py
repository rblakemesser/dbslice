#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List, Tuple
import re


def _fetch_columns(cur, schema: str, table: str) -> Dict[str, dict]:
    cur.execute(
        """
        SELECT column_name,
               data_type,
               udt_name,
               is_nullable,
               column_default,
               character_maximum_length,
               numeric_precision,
               numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    rows = cur.fetchall() or []
    out: Dict[str, dict] = {}
    for (
        name,
        data_type,
        udt_name,
        is_nullable,
        column_default,
        charlen,
        numprec,
        numscale,
    ) in rows:
        out[name] = {
            "data_type": data_type,
            "udt": udt_name,
            "nullable": is_nullable,
            "default": column_default,
            "charlen": charlen,
            "numprec": numprec,
            "numscale": numscale,
        }
    return out


def _fetch_pk(cur, schema: str, table: str) -> List[str]:
    cur.execute(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in (cur.fetchall() or [])]


def _fetch_constraints(cur, schema: str, table: str) -> Dict[str, Dict[str, str]]:
    cur.execute(
        """
        SELECT con.conname,
               con.contype,
               pg_get_constraintdef(con.oid, true) AS defn
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        ORDER BY con.conname
        """,
        (schema, table),
    )
    rows = cur.fetchall() or []
    out: Dict[str, Dict[str, str]] = {"p": {}, "u": {}, "c": {}, "f": {}, "x": {}}
    for name, typ, defn in rows:
        out.get(typ, {}).update({name: defn})
    return out


def _fetch_indexes(cur, schema: str, table: str) -> Dict[str, str]:
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname
        """,
        (schema, table),
    )
    return {name: defn for name, defn in (cur.fetchall() or [])}


def _fetch_triggers(cur, schema: str, table: str) -> Dict[str, dict]:
    cur.execute(
        """
        SELECT t.tgname,
               pg_get_triggerdef(t.oid, true) AS tgdef,
               p.proname AS func_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc p ON p.oid = t.tgfoid
        WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
        ORDER BY t.tgname
        """,
        (schema, table),
    )
    return {name: {"def": tgdef, "func": func} for name, tgdef, func in (cur.fetchall() or [])}


def _extract_seq_from_default(default_expr: str | None) -> Tuple[str | None, str] | None:
    if not default_expr:
        return None
    m = re.search(r"nextval\('\"?([\w]+)\"?\.?\"?([\w]+)?\"?'::regclass\)", default_expr)
    if not m:
        return None
    if m.group(2):
        return (m.group(1), m.group(2))  # (schema, seq)
    else:
        return (None, m.group(1))  # (None, seq)


def _fetch_sequence_owned_by(cur, schema: str, table: str) -> List[dict]:
    cur.execute(
        """
        SELECT seq_ns.nspname AS seq_schema, seq.relname AS seq_name,
               a.attname AS column_name
        FROM pg_class seq
        JOIN pg_namespace seq_ns ON seq_ns.oid = seq.relnamespace
        JOIN pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
        JOIN pg_class t ON t.oid = d.refobjid
        JOIN pg_namespace tns ON tns.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        WHERE seq.relkind = 'S' AND tns.nspname = %s AND t.relname = %s
        ORDER BY seq_ns.nspname, seq.relname, a.attname
        """,
        (schema, table),
    )
    return [
        {"seq_schema": r[0], "seq_name": r[1], "column": r[2]}
        for r in (cur.fetchall() or [])
    ]


def _norm_schema_refs(text: str) -> str:
    return re.sub(r'"?(public|stage)"?\.', 'SCHEMA.', text)


def audit_table(conn, table: str, *, src_schema: str = 'public', dst_schema: str = 'stage') -> Dict[str, object]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
            ),
            EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
            )
            """,
            (src_schema, table, dst_schema, table),
        )
        src_exists, dst_exists = cur.fetchone()

        gaps: Dict[str, object] = {
            "table": table,
            "schemas": {"src": src_schema, "dst": dst_schema},
        }

        if not (src_exists and dst_exists):
            gaps["exists_diff"] = {"src_missing": not src_exists, "dst_missing": not dst_exists}
            return gaps

        # Columns diff
        src_cols = _fetch_columns(cur, src_schema, table)
        dst_cols = _fetch_columns(cur, dst_schema, table)
        only_src_cols = sorted(set(src_cols.keys()) - set(dst_cols.keys()))
        only_dst_cols = sorted(set(dst_cols.keys()) - set(src_cols.keys()))
        mismatched_cols = {}

        def _norm_default_for_compare(val: str | None) -> str | None:
            if not isinstance(val, str):
                return val
            out = re.sub(r"nextval\('\s*\"?(?:public|stage)\"?\.", "nextval('", val, flags=re.IGNORECASE)
            out = re.sub(r"\s+", " ", out.strip())
            return out

        for c in set(src_cols.keys()) & set(dst_cols.keys()):
            ap = src_cols[c]
            as_ = dst_cols[c]
            for fld in ("data_type", "udt", "nullable", "default", "charlen", "numprec", "numscale"):
                a_val = ap.get(fld)
                b_val = as_.get(fld)
                a_cmp = _norm_default_for_compare(a_val) if fld == "default" else a_val
                b_cmp = _norm_default_for_compare(b_val) if fld == "default" else b_val
                if (a_cmp or None) != (b_cmp or None):
                    mismatched_cols.setdefault(c, {})[fld] = {"src": a_val, "dst": b_val}
        if only_src_cols or only_dst_cols or mismatched_cols:
            gaps["columns"] = {"only_src": only_src_cols, "only_dst": only_dst_cols, "mismatched": mismatched_cols}

        # Primary key
        src_pk = _fetch_pk(cur, src_schema, table)
        dst_pk = _fetch_pk(cur, dst_schema, table)
        if src_pk != dst_pk:
            gaps["pk"] = {"src": src_pk, "dst": dst_pk}

        # Constraints
        src_cons = _fetch_constraints(cur, src_schema, table)
        dst_cons = _fetch_constraints(cur, dst_schema, table)

        def _diff_map(a: Dict[str, str], b: Dict[str, str], *, norm_schema: bool = True, normalizer=None):
            only_a = sorted(set(a.keys()) - set(b.keys()))
            only_b = sorted(set(b.keys()) - set(a.keys()))
            mismatched = {}
            for k in set(a.keys()) & set(b.keys()):
                va = _norm_schema_refs(a[k]) if norm_schema else a[k]
                vb = _norm_schema_refs(b[k]) if norm_schema else b[k]
                va_c = re.sub(r"\s+", " ", va.strip())
                vb_c = re.sub(r"\s+", " ", vb.strip())
                if normalizer is not None:
                    va_c = normalizer(va_c)
                    vb_c = normalizer(vb_c)
                if va_c != vb_c:
                    mismatched[k] = {"src": a[k], "dst": b[k]}
            return {"only_src": only_a, "only_dst": only_b, "mismatched": mismatched}

        def _normalize_fk(defn: str) -> str:
            # Ignore NOT VALID token differences
            out = re.sub(r"\s+NOT\s+VALID\b", "", defn or "", flags=re.IGNORECASE)
            # Normalize schema-qualified REFERENCES to be schema-agnostic while preserving table name
            # Handles variants like: REFERENCES public.table, REFERENCES "public"."table", REFERENCES SCHEMA.table, or no schema
            out = re.sub(
                r"(REFERENCES\s+)(?:SCHEMA\.|\"?[A-Za-z_][\w$]*\"?\.)?(\"?[A-Za-z_][\w$]*\"?)",
                r"\1\2",
                out,
                flags=re.IGNORECASE,
            )
            return out

        cons_diff = {
            "unique": _diff_map(src_cons.get("u", {}), dst_cons.get("u", {})),
            "check": _diff_map(src_cons.get("c", {}), dst_cons.get("c", {})),
            "exclusion": _diff_map(src_cons.get("x", {}), dst_cons.get("x", {})),
            "foreign": _diff_map(src_cons.get("f", {}), dst_cons.get("f", {}), normalizer=_normalize_fk),
        }
        cons_diff = {k: v for k, v in cons_diff.items() if v["only_src"] or v["only_dst"] or v["mismatched"]}
        if cons_diff:
            gaps["constraints"] = cons_diff

        # Indexes (exclude PK index)
        src_idx = {k: v for k, v in _fetch_indexes(cur, src_schema, table).items() if not k.endswith("_pkey")}
        dst_idx = {k: v for k, v in _fetch_indexes(cur, dst_schema, table).items() if not k.endswith("_pkey")}

        def _norm_idx(d: Dict[str, str]) -> Dict[str, str]:
            out = {}
            for k, v in d.items():
                v2 = _norm_schema_refs(v)
                v2 = re.sub(rf" ON SCHEMA\.\"?{table}\"? ", " ON SCHEMA.TABLE ", v2)
                out[k] = v2
            return out

        idx_diff = _diff_map(_norm_idx(src_idx), _norm_idx(dst_idx), norm_schema=False)
        if idx_diff["only_src"] or idx_diff["only_dst"] or idx_diff["mismatched"]:
            gaps["indexes"] = idx_diff

        # Triggers
        src_tr = _fetch_triggers(cur, src_schema, table)
        dst_tr = _fetch_triggers(cur, dst_schema, table)

        def _normalize_trigger_def(defn: str, table_name: str) -> str:
            out = _norm_schema_refs(defn or "")
            pattern = rf"\bON\s+(?:SCHEMA\.)?\"?{re.escape(table_name)}\"?\b"
            out = re.sub(pattern, "ON SCHEMA.TABLE", out, flags=re.IGNORECASE)
            out = re.sub(r"EXECUTE\s+FUNCTION\s+SCHEMA\.", "EXECUTE FUNCTION ", out, flags=re.IGNORECASE)
            out = re.sub(r"\s+", " ", out.strip())
            return out

        def _normalize_tr_map(d: Dict[str, dict]) -> Dict[str, dict]:
            return {k: {"def": _normalize_trigger_def(v.get("def", ""), table), "func": v.get("func")} for k, v in d.items()}

        src_tr_n = _normalize_tr_map(src_tr)
        dst_tr_n = _normalize_tr_map(dst_tr)
        only_src_tr = sorted(set(src_tr_n.keys()) - set(dst_tr_n.keys()))
        only_dst_tr = sorted(set(dst_tr_n.keys()) - set(src_tr_n.keys()))
        mismatched_tr = {}
        for k in set(src_tr_n.keys()) & set(dst_tr_n.keys()):
            if src_tr_n[k]["def"] != dst_tr_n[k]["def"] or src_tr_n[k]["func"] != dst_tr_n[k]["func"]:
                mismatched_tr[k] = {"src": src_tr.get(k), "dst": dst_tr.get(k)}
        if only_src_tr or only_dst_tr or mismatched_tr:
            gaps["triggers"] = {"only_src": only_src_tr, "only_dst": only_dst_tr, "mismatched": mismatched_tr}

        # Sequence defaults on columns
        src_seq_defaults = {col: _extract_seq_from_default(meta.get("default")) for col, meta in src_cols.items()}
        dst_seq_defaults = {col: _extract_seq_from_default(meta.get("default")) for col, meta in dst_cols.items()}
        seq_def_missing_in_dst = {col: v for col, v in src_seq_defaults.items() if v and not dst_seq_defaults.get(col)}
        seq_def_missing_in_src = {col: v for col, v in dst_seq_defaults.items() if v and not src_seq_defaults.get(col)}
        if seq_def_missing_in_dst or seq_def_missing_in_src:
            gaps["seq_defaults"] = {
                "missing_in_dst": seq_def_missing_in_dst,
                "missing_in_src": seq_def_missing_in_src,
            }

        # OWNED BY differences (schema-agnostic)
        src_owned = _fetch_sequence_owned_by(cur, src_schema, table)
        dst_owned = _fetch_sequence_owned_by(cur, dst_schema, table)

        def _owned_key_norm(r):
            return (r["seq_name"], r["column"])  # ignore schema

        src_norm = { _owned_key_norm(r): r for r in src_owned }
        dst_norm = { _owned_key_norm(r): r for r in dst_owned }
        only_src_norm = sorted(list(set(src_norm.keys()) - set(dst_norm.keys())))
        only_dst_norm = sorted(list(set(dst_norm.keys()) - set(src_norm.keys())))
        if only_src_norm or only_dst_norm:
            gaps["seq_owned_by"] = {
                "only_src": [src_norm[k] for k in only_src_norm],
                "only_dst": [dst_norm[k] for k in only_dst_norm],
            }

        return gaps


def audit_all_tables(conn, *, src_schema: str, dst_schema: str) -> Dict[str, object]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (src_schema,),
        )
        tables = [r[0] for r in (cur.fetchall() or [])]
    out: Dict[str, object] = {}
    for t in tables:
        rpt = audit_table(conn, t, src_schema=src_schema, dst_schema=dst_schema)
        # Include only if there are differences (heuristic: has exists_diff or columns)
        if any(k for k in rpt.keys() if k not in ("table", "schemas")):
            out[t] = rpt
    return out


def audit_sequences(conn, *, src_schema: str = 'public', dst_schema: str = 'stage') -> Dict[str, object]:
    """Audit sequences across two schemas (gaps-only), modeled after prodcopy."""
    def list_sequences(cur, schema: str) -> List[str]:
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
        return [r[0] for r in (cur.fetchall() or [])]

    def fetch_seq_core(cur, schema: str, name: str) -> Tuple[int | None, int | None, bool | None]:
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

    def fetch_seq_owned_by(cur, schema: str, name: str) -> str | None:
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

    out: Dict[str, object] = {"schemas": {"src": src_schema, "dst": dst_schema}}
    with conn.cursor() as cur:
        src_list = list_sequences(cur, src_schema)
        dst_list = list_sequences(cur, dst_schema)
        only_src_names = sorted(list(set(src_list) - set(dst_list)))
        only_dst_names = sorted(list(set(dst_list) - set(src_list)))
        if only_src_names:
            only_src: List[dict] = []
            for name in only_src_names:
                next_val, _, _ = fetch_seq_core(cur, src_schema, name)
                owned = fetch_seq_owned_by(cur, src_schema, name)
                only_src.append({"name": name, "next_value": next_val, "owned_by": owned})
            out["only_src"] = only_src
        if only_dst_names:
            only_dst: List[dict] = []
            for name in only_dst_names:
                next_val, _, _ = fetch_seq_core(cur, dst_schema, name)
                owned = fetch_seq_owned_by(cur, dst_schema, name)
                only_dst.append({"name": name, "next_value": next_val, "owned_by": owned})
            out["only_dst"] = only_dst

        common = sorted(list(set(src_list) & set(dst_list)))
        next_mismatch: Dict[str, dict] = {}
        owned_by_diff: Dict[str, dict] = {}
        for name in common:
            src_next, _, _ = fetch_seq_core(cur, src_schema, name)
            dst_next, _, _ = fetch_seq_core(cur, dst_schema, name)
            if src_next != dst_next:
                next_mismatch[name] = {"src": src_next, "dst": dst_next}
            src_owned = fetch_seq_owned_by(cur, src_schema, name)
            dst_owned = fetch_seq_owned_by(cur, dst_schema, name)
            if src_owned != dst_owned:
                if src_owned and dst_owned:
                    try:
                        _, st_tbl, st_col = src_owned.split('.')
                        _, dt_tbl, dt_col = dst_owned.split('.')
                        if st_tbl == dt_tbl and st_col == dt_col:
                            pass
                        else:
                            owned_by_diff[name] = {"src": src_owned, "dst": dst_owned}
                    except ValueError:
                        owned_by_diff[name] = {"src": src_owned, "dst": dst_owned}
                else:
                    owned_by_diff[name] = {"src": src_owned, "dst": dst_owned}

        if next_mismatch:
            out["next_value_mismatched"] = next_mismatch
        if owned_by_diff:
            out["owned_by_diff"] = owned_by_diff

    return out
