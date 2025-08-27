import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_migrate_tables_runs_selected_families_only(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    # Define two families; we'll run only 'product'
    overlay = {
        'dest_schema': 'stage_mt',
        'tmp_schema': 'tmp_mt',
        'shards_schema': 'shards_mt',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1]}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}},
            {'name': 'product', 'root': {'table': 'product', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'}},
        ],
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", cfg,
        "--migrate-tables", 'product',
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            # Product should exist, store should not
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='stage_mt' AND table_name='product')")
            assert cur.fetchone()[0]
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='stage_mt' AND table_name='store')")
            assert not cur.fetchone()[0]


def test_migrate_tables_all_when_no_args(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_mt_all',
        'tmp_schema': 'tmp_mt_all',
        'shards_schema': 'shards_mt_all',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1]}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}},
            {'name': 'product', 'root': {'table': 'product', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'}},
        ],
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty2.env').write_text('\n') or (tmp_path / 'empty2.env')),
        "--config", cfg,
        "--migrate-tables",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='stage_mt_all' AND table_name='product')")
            assert cur.fetchone()[0]
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='stage_mt_all' AND table_name='store')")
            assert cur.fetchone()[0]
