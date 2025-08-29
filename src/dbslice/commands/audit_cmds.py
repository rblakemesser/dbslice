#!/usr/bin/env python3
from __future__ import annotations

"""Audit commands for tables and sequences.

run_audit_tables:
- If target == '__ALL__', returns gaps-only report across all tables; otherwise returns table report.
- Normalizes perfect match to {result: 'perfect match'} without extra noise.

run_audit_sequences:
- Returns gaps-only sequence audit; normalizes perfect match similarly.
"""

from ..audit import audit_all_tables, audit_table, audit_sequences as _audit_sequences


def run_audit_tables(conn, cfg: dict, target: str | None):
    if target == '__ALL__':
        res = audit_all_tables(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
        return res if res else {"result": "perfect match"}
    else:
        rpt = audit_table(conn, str(target), src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
        if all(k in ("table", "schemas") for k in rpt.keys()):
            rpt = {"result": "perfect match", **rpt}
        return rpt


def run_audit_sequences(conn, cfg: dict):
    res = _audit_sequences(conn, src_schema=str(cfg['source_schema']), dst_schema=str(cfg['dest_schema']))
    if set(res.keys()) == {"schemas"}:
        res = {"result": "perfect match", **res}
    return res
