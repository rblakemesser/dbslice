import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _index_names(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            ORDER BY indexname
            """,
            (schema, table),
        )
        return {r[0] for r in (cur.fetchall() or [])}


def _index_def(conn, schema: str, name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = %s AND indexname = %s
            """,
            (schema, name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def test_migrate_indexes_all_tables(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_idx',
        'tmp_schema': 'tmp_idx',
        'shards_schema': 'shards_idx',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Create empty dest tables without indexes
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_idx CASCADE')
            cur.execute('CREATE SCHEMA stage_idx')
            for t in ('store', 'customer', 'order'):
                cur.execute(f'CREATE TABLE stage_idx."{t}" (LIKE minicom."{t}" INCLUDING DEFAULTS)')
        conn.commit()

    # Run migrate-indexes for all tables
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate-indexes",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        # Missing indexes should be created in dest
        assert 'store_settings_gin' in _index_names(conn, 'stage_idx', 'store')
        assert 'customer_email_lower_uq' in _index_names(conn, 'stage_idx', 'customer')
        assert 'order_placed_non_test_idx' in _index_names(conn, 'stage_idx', 'order')

        # Create an extraneous index then ensure it is dropped by reconcile
        with conn.cursor() as cur:
            cur.execute('CREATE INDEX extraneous_idx ON stage_idx.store(id)')
        conn.commit()

    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--migrate-indexes",
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        assert 'extraneous_idx' not in _index_names(conn, 'stage_idx', 'store')

        # Make a mismatched same-name index and ensure it gets recreated
        with conn.cursor() as cur:
            cur.execute('DROP INDEX IF EXISTS "stage_idx"."customer_email_lower_uq"')
            cur.execute('CREATE UNIQUE INDEX "customer_email_lower_uq" ON stage_idx.customer (email)')
        conn.commit()

    rc3, out3, err3 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--migrate-indexes", 'customer',
    ], env={"DATABASE_URL": url})
    assert rc3 == 0, err3

    with psycopg.connect(url) as conn:
        idef = _index_def(conn, 'stage_idx', 'customer_email_lower_uq')
        assert idef is not None and 'lower' in idef.lower()

