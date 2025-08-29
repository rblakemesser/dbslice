#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any


_PATCHED = False


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        # Fail loudly: let exceptions propagate for invalid paths/permissions
        raise


def setup_logging() -> logging.Logger:
    """Configure file logging for the 'dbslice' logger.

    Environment variables:
    - DBSLICE_LOG_DIR (default: ./logs)
    - DBSLICE_LOG_LEVEL (default: INFO)
    - DBSLICE_LOG_MAX_BYTES (default: 10485760 i.e., 10MB)
    - DBSLICE_LOG_BACKUPS (default: 5)
    """
    log_dir = os.environ.get("DBSLICE_LOG_DIR", os.path.join(os.getcwd(), "logs"))
    _ensure_dir(log_dir)
    log_file = os.path.join(log_dir, "dbslice.log")

    # Default to DEBUG so every query is captured without extra env setup
    level_name = os.environ.get("DBSLICE_LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.INFO)
    max_bytes = int(os.environ.get("DBSLICE_LOG_MAX_BYTES", 10 * 1024 * 1024))
    backups = int(os.environ.get("DBSLICE_LOG_BACKUPS", 5))

    # Root stays WARNING; app logger handles our messages
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger("dbslice")
    logger.setLevel(level)
    logger.propagate = False  # don't bubble to root (avoid stdout/stderr noise)

    # Avoid duplicate handlers if called twice
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backups)
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(process)d] %(name)s %(module)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        handler.setLevel(level)
        logger.addHandler(handler)
    logger.debug("Logging initialized: %s level=%s", log_file, level_name)

    # Also route psycopg's own logger through our handler for connection-level events
    psy_logger = logging.getLogger("psycopg")
    psy_logger.setLevel(level)
    psy_logger.propagate = False
    # Also ensure nested psycopg loggers don't propagate
    psy_pq_logger = logging.getLogger("psycopg.pq")
    psy_pq_logger.setLevel(level)
    psy_pq_logger.propagate = False
    for h in logger.handlers:
        if h not in psy_logger.handlers:
            psy_logger.addHandler(h)
    return logger


def _format_params(params: Any, limit: int = 400) -> str:
    try:
        s = repr(params)
    except Exception:
        return "<unreprable params>"
    if len(s) > limit:
        return s[:limit] + "... (truncated)"
    return s


def install_psycopg_query_logging(logger: logging.Logger) -> None:
    """Monkey-patch psycopg Cursor/AsyncCursor execute/executemany to log queries.

    Logs at DEBUG level to avoid over-noise at INFO; operators can raise level if needed.
    """
    global _PATCHED
    if _PATCHED:
        return

    try:
        import psycopg
    except Exception:  # pragma: no cover
        raise

    def _wrap_exec(func_name: str, cls):
        orig = getattr(cls, func_name)

        async_marker = (cls.__name__.lower().startswith("async"))

        if async_marker:
            async def wrapper(self, query, params=None, *args, **kwargs):  # type: ignore[no-untyped-def]
                try:
                    logger.debug("SQL: %s | params: %s", str(query), _format_params(params))
                except Exception:
                    pass
                return await orig(self, query, params, *args, **kwargs)
        else:
            def wrapper(self, query, params=None, *args, **kwargs):  # type: ignore[no-untyped-def]
                try:
                    logger.debug("SQL: %s | params: %s", str(query), _format_params(params))
                except Exception:
                    pass
                return orig(self, query, params, *args, **kwargs)

        setattr(cls, func_name, wrapper)

    # Patch sync and async cursors
    _wrap_exec("execute", psycopg.Cursor)
    _wrap_exec("executemany", psycopg.Cursor)
    try:
        from psycopg import AsyncCursor  # type: ignore[attr-defined]
    except Exception:
        AsyncCursor = None  # type: ignore[assignment]
    if AsyncCursor is not None:
        _wrap_exec("execute", AsyncCursor)
        _wrap_exec("executemany", AsyncCursor)

    _PATCHED = True
