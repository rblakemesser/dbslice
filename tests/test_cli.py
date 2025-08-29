import os
import sys
from typing import List

import importlib
import yaml


def write_overlay_config(base_path: str, overlay: dict, tmp_path) -> str:
    """Load base YAML, deep-merge overlay, write to a temp file, return its path."""
    def _merge(a, b):
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                _merge(a[k], v)
            else:
                a[k] = v
        return a
    base = {}
    with open(base_path, 'r') as f:
        base = yaml.safe_load(f) or {}
    merged = _merge(base, overlay)
    out_path = tmp_path / 'overlay.yml'
    out_path.write_text(yaml.safe_dump(merged, sort_keys=False))
    return str(out_path)


def run_cli(args: List[str], env: dict | None = None) -> tuple[int, str, str]:
    """Import and run dbslice.cli.main() with custom argv/env, capture output.

    Returns (rc, stdout, stderr).
    """
    from io import StringIO
    import contextlib

    # Ensure fresh import state
    if 'dbslice.cli' in sys.modules:
        del sys.modules['dbslice.cli']
    if 'dbslice' in sys.modules:
        del sys.modules['dbslice']

    # Patch environment
    old_environ = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(old_environ)
        if env:
            os.environ.update(env)

        # Patch argv
        old_argv = sys.argv[:]
        sys.argv = ['dbslice'] + list(args)

        captured_out = StringIO()
        captured_err = StringIO()
        with contextlib.redirect_stdout(captured_out), contextlib.redirect_stderr(captured_err):
            mod = importlib.import_module('dbslice.cli')
            rc = mod.main()
        return rc, captured_out.getvalue(), captured_err.getvalue()
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
        sys.argv = old_argv


def test_check_connection_missing_url(tmp_path):
    # Provide an empty .env so loader doesn't override env
    empty_env_path = tmp_path / "empty.env"
    empty_env_path.write_text("\n")
    rc, out, err = run_cli(["--env", str(empty_env_path), "--check-connection"], env={})
    assert rc == 2
    assert "DATABASE_URL is not set" in err


def test_check_connection_success(tmp_path):
    # Point to docker-compose postgres service 'db'
    url = "postgresql://postgres:postgres@db:5432/postgres"
    empty_env_path = tmp_path / "empty.env"
    empty_env_path.write_text("\n")
    rc, out, err = run_cli(["--env", str(empty_env_path), "--check-connection"], env={"DATABASE_URL": url})
    assert rc == 0
    env_out = yaml.safe_load(out)
    assert env_out['run']['result'] == 'ok'
