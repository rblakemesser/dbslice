#!/usr/bin/env python3
from __future__ import annotations

from typing import Dict, List
import re

import psycopg


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

