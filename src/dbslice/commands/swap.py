#!/usr/bin/env python3
from __future__ import annotations

"""Swap/Unswap schema commands.

Swap: public → old, dest → public
Unswap: public → dest, old → public

Inputs
- conn: psycopg connection
- dest_schema: destination schema name to swap into public (string)
- do_swap: True for swap, False for unswap

Outputs
- Dict with result and message, suitable for YAML emission by CLI.

Failure policy
- Raise on preconditions failure (missing/extra schemas). No suppression.
"""

from ..dbutil import swap_schemas, unswap_schemas


def run_swap(conn, dest_schema: str, *, do_swap: bool) -> dict:
    if do_swap:
        swap_schemas(conn, dest_schema=dest_schema, old_schema='old')
        return {"result": "swapped", "message": f"public->old, {dest_schema}->public"}
    else:
        unswap_schemas(conn, dest_schema=dest_schema, old_schema='old')
        return {"result": "unswapped", "message": f"public->{dest_schema}, old->public"}
