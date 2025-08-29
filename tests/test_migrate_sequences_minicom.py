import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _seq_next_value(conn, schema: str, name: str) -> int:
    # Compute next value without advancing using last_value/is_called and increment_by
    with conn.cursor() as cur:
        cur.execute(f'SELECT last_value, is_called FROM "{schema}"."{name}"')
        last_value, is_called = cur.fetchone()
        cur.execute(
            """
            SELECT increment_by FROM pg_sequences WHERE schemaname = %s AND sequencename = %s
            """,
            (schema, name),
        )
        inc = cur.fetchone()[0]
    return int(last_value) if not is_called else int(last_value) + int(inc)


def test_migrate_sequences_runs_in_migrate_pipeline(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    # Use base config (precopy: full_copy coupon, schema_only shipment)
    cfg_path = write_overlay_config(str(base), {}, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        # After migrate, sequence next value in dest should match source's next value
        src_next = _seq_next_value(conn, 'minicom', 'coupon_id_seq')
        dst_next = _seq_next_value(conn, 'stage', 'coupon_id_seq')
        assert dst_next == src_next


def test_migrate_sequences_standalone_creates_and_sets_defaults(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_seq_only',
        'tmp_schema': 'tmp_seq_only',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_seq_only CASCADE')
            cur.execute('CREATE SCHEMA stage_seq_only')
            # Create a table with a SERIAL default copied from source
            cur.execute('CREATE TABLE stage_seq_only.store (LIKE minicom.store INCLUDING DEFAULTS)')
        conn.commit()

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty2.env').write_text('\n') or (tmp_path / 'empty2.env')),
        "--config", str(cfg_path),
        "--migrate-sequences",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        # Next value should match source sequence's next value
        src_next = _seq_next_value(conn, 'minicom', 'store_id_seq')
        dst_next = _seq_next_value(conn, 'stage_seq_only', 'store_id_seq')
        assert dst_next == src_next
