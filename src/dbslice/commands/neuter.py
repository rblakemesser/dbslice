#!/usr/bin/env python3
from __future__ import annotations

"""Neuter (redaction) command helper.

Applies config-driven neuter operations to the destination schema.
Idempotent by design: prefix strategies avoid double-prefixing; replace is stable.

Inputs
- conn: psycopg connection
- cfg: normalized config dict with 'neuter' section

Outputs
- Dict with {changed: bool, result: 'neuter_applied'|'neuter_skipped'}

Failure policy
- Validate required inputs; raise on unsupported strategies.
"""

from ..dbutil import neuter_data


def run_neuter(conn, cfg: dict) -> dict:
    changed = neuter_data(conn, cfg)
    return {"changed": bool(changed), "result": "neuter_applied" if changed else "neuter_skipped"}
