import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_listing_union_by_store_or_product(tmp_path):
    # Create a minimal listing table in minicom for this test
    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS minicom.listing CASCADE")
            cur.execute(
                """
                CREATE TABLE minicom.listing (
                  id SERIAL PRIMARY KEY,
                  store_id INTEGER NULL,
                  product_id INTEGER NULL
                )
                """
            )
            # Seed: one row with store_id=1, one with product_id=1, one with store_id=2/product_id=3, one orphan
            cur.execute("INSERT INTO minicom.listing (store_id, product_id) VALUES (1, NULL), (NULL, 1), (2, 3), (NULL, NULL)")

    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'roots': [{'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1]}}],
        'families': [{
            'name': 'product',
            'root': {'table': 'product', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'},
            'deps': [{
                'table': 'listing', 'distinct': True,
                'sources': [
                    {'selection': 'stores', 'join': 'd.store_id = p.id'},
                    {'parent_table': 'product', 'join': 'd.product_id = p.id'}
                ]
            }]
        }]
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", cfg,
        "--migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT store_id, product_id FROM stage.listing ORDER BY id")
            rows = cur.fetchall()
            # Expect only rows that match store_id in [1] OR product_id among selected products (store_id=1 => products ids 1,2)
            # Our seed rows: (1,NULL) -> included; (NULL,1) -> included; (2,3) -> excluded; (NULL,NULL) -> excluded
            assert rows == [(1, None), (None, 1)]
