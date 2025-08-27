import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_pk_mod_sharded_order_items(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1, 2]}},
        ],
        'families': [
            {'name': 'order', 'root': {'table': 'order', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'}, 'deps': [
                {'table': 'order_item', 'parent_table': 'order', 'join': 'd.order_id = p.id', 'shard_by': 'pk_mod', 'shard_key': 'id', 'shard_count': 2}
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
            cur.execute("SELECT count(*) FROM minicom.order_item oi JOIN minicom.\"order\" o ON o.id = oi.order_id WHERE o.store_id IN (1,2)")
            src = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM stage.order_item")
            dst = cur.fetchone()[0]
            assert dst == src
