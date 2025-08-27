import psycopg
from pathlib import Path

from test_cli import run_cli


def test_precopy_creates_dest_tables_and_copies_full(tmp_path):
    cfg = Path('tests/fixtures/minicom/minicom.yml').resolve()
    # Run migrate (precopy only for now)
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg),
        "--migrate",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
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

