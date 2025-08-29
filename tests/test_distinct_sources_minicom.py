import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def test_dep_sources_distinct_dedups_across_sources(tmp_path):
    url = "postgresql://postgres:postgres@db:5432/postgres"
    # Prepare minimal tables to reproduce duplicates from two parent sources
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO minicom, public")
            # Drop if exist (idempotent)
            cur.execute("DROP TABLE IF EXISTS minicom.parent_a CASCADE")
            cur.execute("DROP TABLE IF EXISTS minicom.parent_b CASCADE")
            cur.execute("DROP TABLE IF EXISTS minicom.batch CASCADE")
            # Create tables
            cur.execute(
                """
                CREATE TABLE minicom.batch (
                  id SERIAL PRIMARY KEY
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE minicom.parent_a (
                  id SERIAL PRIMARY KEY,
                  store_id INTEGER NOT NULL,
                  batch_id INTEGER NOT NULL REFERENCES minicom.batch(id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE minicom.parent_b (
                  id SERIAL PRIMARY KEY,
                  store_id INTEGER NOT NULL,
                  batch_id INTEGER NOT NULL REFERENCES minicom.batch(id)
                )
                """
            )
            # Seed one batch and multiple parent rows pointing to the same batch (store 1)
            cur.execute("INSERT INTO minicom.batch DEFAULT VALUES RETURNING id")
            batch_id = cur.fetchone()[0]
            # Three rows in each parent referencing store 1 and same batch
            cur.execute(
                "INSERT INTO minicom.parent_a (store_id, batch_id) VALUES (1, %s), (1, %s), (1, %s)",
                (batch_id, batch_id, batch_id),
            )
            cur.execute(
                "INSERT INTO minicom.parent_b (store_id, batch_id) VALUES (1, %s), (1, %s), (1, %s)",
                (batch_id, batch_id, batch_id),
            )

    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'dest_schema': 'stage_distinct_src',
        'tmp_schema': 'tmp_distinct_src',
        'table_groups': [{
            'name': 'dup_case',
            'root': {
                'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id',
                'selector': {'mode': 'list', 'ids': [1]},
            },
            'deps': [
                { 'table': 'parent_a', 'parent_table': 'store', 'join': 'd.store_id = p.id' },
                { 'table': 'parent_b', 'parent_table': 'store', 'join': 'd.store_id = p.id' },
                { 'table': 'batch', 'distinct': True, 'sources': [
                    { 'parent_table': 'parent_a', 'parent_schema': 'stage_distinct_src', 'join': 'd.id = p.batch_id' },
                    { 'parent_table': 'parent_b', 'parent_schema': 'stage_distinct_src', 'join': 'd.id = p.batch_id' },
                ]},
            ],
        }],
    }
    cfg = write_overlay_config(str(base), overlay, tmp_path)

    # Ensure a clean destination schema for deterministic results
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_distinct_src CASCADE')
            cur.execute('CREATE SCHEMA stage_distinct_src')

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", cfg,
        "--migrate-tables", "dup_case",
    ], env={"DATABASE_URL": url})
    print('CLI OUT:\n', out)
    print('CLI ERR:\n', err)
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            # Ensure only one batch row copied and no duplicates remain
            cur.execute("SELECT count(*), count(DISTINCT id) FROM stage_distinct_src.batch")
            total, distinct = cur.fetchone()
            assert total == 1 and distinct == 1
            # Primary key should be present on dest table
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_constraint
                WHERE conrelid = 'stage_distinct_src.batch'::regclass
                  AND contype = 'p'
                """
            )
            assert cur.fetchone()[0] == 1
