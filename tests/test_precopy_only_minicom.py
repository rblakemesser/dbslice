import psycopg
from pathlib import Path

from test_cli import run_cli


def test_precopy_only_minicom(tmp_path):
    cfg = Path('tests/fixtures/minicom/minicom.yml').resolve()
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Ensure dest schema is clean
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage CASCADE')
            cur.execute('CREATE SCHEMA stage')
        conn.commit()

    # Run precopy-only to populate schema_only and full_copy tables
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg),
        "--precopy-only",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            # shipment should exist in dest schema with 0 rows (schema_only)
            cur.execute("SELECT to_regclass('stage.shipment') IS NOT NULL")
            assert cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM stage.shipment")
            assert cur.fetchone()[0] == 0

            # coupon should be fully copied
            cur.execute("SELECT count(*) FROM minicom.coupon")
            src_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM stage.coupon")
            dst_count = cur.fetchone()[0]
            assert dst_count == src_count

