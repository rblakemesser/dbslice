import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_sharded_store_family_copies_both_stores(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1, 2]}, 'shard': {'count': 2, 'strategy': 'round_robin'}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}, 'deps': [
                {'table': 'product', 'parent_table': 'store', 'join': 'd.store_id = p.id'}
            ]}
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
            cur.execute("SELECT id FROM stage.store ORDER BY id")
            ids = [r[0] for r in cur.fetchall()]
            assert ids == [1, 2]
            cur.execute("SELECT DISTINCT store_id FROM stage.product ORDER BY store_id")
            pids = [r[0] for r in cur.fetchall()]
            assert pids == [1, 2]
