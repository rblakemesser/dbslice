import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _schema_table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def _relations_like(conn, schema: str, pattern: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname LIKE %s
            ORDER BY c.relname
            """,
            (schema, pattern),
        )
        return [r[0] for r in (cur.fetchall() or [])]


def test_reset_sharded_family_drops_tables_and_shards(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_reset_sh',
        'tmp_schema': 'tmp_reset_sh',
        'shards_schema': 'shards_reset_sh',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1, 2]}, 'shard': {'count': 2, 'strategy': 'round_robin'}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}, 'deps': [
                {'table': 'product', 'parent_table': 'store', 'join': 'd.store_id = p.id'}
            ]}
        ],
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)

    # Run migrate to create objects
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        # Verify created
        assert _schema_table_exists(conn, 'stage_reset_sh', 'store')
        assert _schema_table_exists(conn, 'stage_reset_sh', 'product')
        # Engine cleans shard artifacts post-finalize; ensure none remain
        assert _relations_like(conn, 'shards_reset_sh', 'store_sh%') == []
        assert _relations_like(conn, 'shards_reset_sh', 'product_sh%') == []

    # Reset store family
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--reset", 'store',
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        # Verify dropped
        assert not _schema_table_exists(conn, 'stage_reset_sh', 'store')
        assert not _schema_table_exists(conn, 'stage_reset_sh', 'product')
        assert _relations_like(conn, 'shards_reset_sh', 'store_sh%') == []
        assert _relations_like(conn, 'shards_reset_sh', 'product_sh%') == []


def test_reset_pkmod_family_drops_pmsh_shards(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_reset_pm',
        'tmp_schema': 'tmp_reset_pm',
        'shards_schema': 'shards_reset_pm',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1, 2]}}
        ],
        'families': [
            {'name': 'order', 'root': {'table': 'order', 'id_col': 'id', 'selection': 'stores', 'join': 'd.store_id = p.id'}, 'deps': [
                {'table': 'order_item', 'parent_table': 'order', 'join': 'd.order_id = p.id', 'shard_by': 'pk_mod', 'shard_key': 'id', 'shard_count': 2}
            ]}
        ],
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env2').write_text('\n') or (tmp_path / 'empty.env2')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err

    url = "postgresql://postgres:postgres@db:5432/postgres"
    with psycopg.connect(url) as conn:
        assert _schema_table_exists(conn, 'stage_reset_pm', 'order')
        assert _schema_table_exists(conn, 'stage_reset_pm', 'order_item')
        # Engine cleans pk_mod shard artifacts after finalize
        assert _relations_like(conn, 'shards_reset_pm', 'order_item_pmsh%') == []

    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env2'),
        "--config", str(cfg_path),
        "--reset", 'order',
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        assert not _schema_table_exists(conn, 'stage_reset_pm', 'order')
        assert not _schema_table_exists(conn, 'stage_reset_pm', 'order_item')
        assert _relations_like(conn, 'shards_reset_pm', 'order_item_pmsh%') == []
