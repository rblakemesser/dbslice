import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


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


def test_migrate_triggers_for_table(tmp_path):
    url = "postgresql://postgres:postgres@db:5432/postgres"
    # Create a simple trigger function and trigger in source (minicom)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION minicom.trg_noop()
                RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
                """
            )
            # Ensure source table exists
            cur.execute("SELECT to_regclass('minicom.coupon')")
            assert cur.fetchone()[0] is not None
            # Drop if exists then create trigger on source table
            cur.execute('DROP TRIGGER IF EXISTS test_noop_trigger ON minicom.coupon')
            cur.execute(
                'CREATE TRIGGER test_noop_trigger BEFORE UPDATE ON minicom.coupon '
                'FOR EACH ROW EXECUTE FUNCTION minicom.trg_noop()'
            )
        conn.commit()

    # Prepare dest schema and table
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_trig CASCADE')
            cur.execute('CREATE SCHEMA stage_trig')
            cur.execute('CREATE TABLE stage_trig.coupon (LIKE minicom.coupon INCLUDING DEFAULTS)')
        conn.commit()

    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_trig',
        'tmp_schema': 'tmp_trig',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)

    # Ensure function exists in dest first
    rc1, out1, err1 = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate-functions",
    ], env={"DATABASE_URL": url})
    assert rc1 == 0, err1

    # Migrate triggers for coupon only
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--migrate-triggers", 'coupon',
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        names = _trigger_names(conn, 'stage_trig', 'coupon')
        assert 'test_noop_trigger' in names

