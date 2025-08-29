import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _fn_exists(conn, schema: str, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
            """,
            (schema, name),
        )
        return cur.fetchone() is not None


def test_migrate_functions_creates_in_dest(tmp_path):
    url = "postgresql://postgres:postgres@db:5432/postgres"
    # Create a simple function in source schema (minicom)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION minicom.fn_add1(a integer)
                RETURNS integer LANGUAGE sql AS $$ SELECT a + 1 $$;
                """
            )
        conn.commit()

    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_fn',
        'tmp_schema': 'tmp_fn',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate-functions",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        assert _fn_exists(conn, 'stage_fn', 'fn_add1')

