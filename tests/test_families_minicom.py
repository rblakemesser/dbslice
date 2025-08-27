import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_family_store_and_products_subset(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_subset',
        'tmp_schema': 'tmp_subset',
        'roots': [
            {
                'name': 'store_sel',
                'table': 'store',
                'id_col': 'id',
                'selector': {
                    'mode': 'sql',
                    'sql': "select id from minicom.store where name = 'Alpha Store'",
                },
            }
        ],
        'families': [
            {
                'name': 'store',
                'root': {'table': 'store', 'id_col': 'id', 'selection': 'store_sel', 'join': 'd.id = p.id'},
                'deps': [
                    {'table': 'product', 'parent_table': 'store', 'join': 'd.store_id = p.id'},
                ],
            }
        ],
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", cfg,
        "--migrate",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            # Only Alpha Store (id=1) should be present
            cur.execute("SELECT id, name FROM stage_subset.store ORDER BY id")
            rows = cur.fetchall()
            assert rows == [(1, 'Alpha Store')]

            # Products only from store_id=1
            cur.execute("SELECT DISTINCT store_id FROM stage_subset.product ORDER BY store_id")
            store_ids = [r[0] for r in cur.fetchall()]
            assert store_ids == [1]
