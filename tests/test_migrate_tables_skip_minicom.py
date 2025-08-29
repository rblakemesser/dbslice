import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_migrate_tables_skips_when_present(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_skip',
        'tmp_schema': 'tmp_skip',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1]}},
        ],
        'families': [
            {'name': 'product', 'root': {'table': 'product', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'}},
        ],
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"
    # First run creates stage_skip.product
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", cfg,
        "--migrate-tables", 'product',
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), max(id) FROM stage_skip.product")
            before_count, max_id = cur.fetchone()
            # Insert a synthetic row with id beyond current max and unique SKU suffix
            new_id = int(max_id or 0) + 1000
            cur.execute(
                """
                INSERT INTO stage_skip.product (id, store_id, sku, price_cents, deleted_at)
                SELECT %s, store_id, sku || '-' || %s::text, price_cents, deleted_at
                FROM stage_skip.product LIMIT 1
                """,
                (new_id, new_id),
            )
            conn.commit()

    # Second run should skip because table exists
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", cfg,
        "--migrate-tables", 'product',
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM stage_skip.product")
            after = cur.fetchone()[0]
            # Because we skipped, the extra row remains (no rebuild occurred)
            assert after == before_count + 1
