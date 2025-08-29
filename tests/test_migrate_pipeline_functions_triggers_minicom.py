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


def _trigger_names(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.tgname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
            ORDER BY t.tgname
            """,
            (schema, table),
        )
        return {r[0] for r in (cur.fetchall() or [])}


def test_migrate_pipeline_includes_functions_and_triggers(tmp_path):
    url = "postgresql://postgres:postgres@db:5432/postgres"
    # Create source function and a simple trigger on coupon in minicom
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION minicom.fn_add1(a integer)
                RETURNS integer LANGUAGE sql AS $$ SELECT a + 1 $$;
                """
            )
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION minicom.trg_noop()
                RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
                """
            )
            # Ensure the source table exists
            cur.execute("SELECT to_regclass('minicom.coupon')")
            assert cur.fetchone()[0] is not None
            cur.execute('DROP TRIGGER IF EXISTS test_noop_trigger ON minicom.coupon')
            cur.execute(
                'CREATE TRIGGER test_noop_trigger BEFORE UPDATE ON minicom.coupon '
                'FOR EACH ROW EXECUTE FUNCTION minicom.trg_noop()'
            )
        conn.commit()

    # Use migrate with overlay dest schema; precopy will create stage_mig_ft2.coupon from minicom
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_mig_ft2',
        'tmp_schema': 'tmp_mig_ft2',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        # Function present in dest schema
        assert _fn_exists(conn, 'stage_mig_ft2', 'fn_add1')
        # Trigger exists on dest coupon
        names = _trigger_names(conn, 'stage_mig_ft2', 'coupon')
        assert 'test_noop_trigger' in names

