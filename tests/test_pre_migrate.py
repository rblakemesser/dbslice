import psycopg
from pathlib import Path
import yaml

from test_cli import run_cli, write_overlay_config


def _count_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return int(cur.fetchone()[0])


def test_pre_migrate_truncate_and_sql(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    schema = 'stage_premig'
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': schema,
        'tmp_schema': 'tmp_premig',
        'pre_migrate': {
            'truncate': [
                'customer',            # resolves against dest_schema
                f'{schema}.foo',       # fully-qualified
            ],
            'sql': [
                f'CREATE TABLE IF NOT EXISTS "{schema}".pre_marker (id int primary key)',
                f'INSERT INTO "{schema}".pre_marker (id) VALUES (1) ON CONFLICT DO NOTHING',
            ],
        },
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Prepare dest schema and tables with data
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{schema}"')
            # customer table copied from fixture schema
            cur.execute(f'CREATE TABLE "{schema}".customer (LIKE minicom.customer INCLUDING DEFAULTS)')
            cur.execute(f'INSERT INTO "{schema}".customer SELECT * FROM minicom.customer')
            # simple auxiliary table
            cur.execute(f'CREATE TABLE "{schema}".foo (id int primary key, name text)')
            cur.execute(f"INSERT INTO \"{schema}\".foo VALUES (1, 'a'), (2, 'b')")
        conn.commit()

    # Run pre-migrate
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--pre-migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err
    res = yaml.safe_load(out)
    assert res['run']['result'] == 'pre_migrate_done'
    assert f'{schema}.customer' in res['run']['truncated']
    assert f'{schema}.foo' in res['run']['truncated']

    with psycopg.connect(url) as conn:
        assert _count_rows(conn, schema, 'customer') == 0
        assert _count_rows(conn, schema, 'foo') == 0
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}".pre_marker')
            cnt = int(cur.fetchone()[0])
        assert cnt == 1

    # Run again; ensure idempotent (still truncated, SQL insert is no-op)
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--pre-migrate",
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2
    res2 = yaml.safe_load(out2)
    assert res2['run']['result'] == 'pre_migrate_done'
    with psycopg.connect(url) as conn:
        assert _count_rows(conn, schema, 'customer') == 0
        assert _count_rows(conn, schema, 'foo') == 0
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}".pre_marker')
            cnt2 = int(cur.fetchone()[0])
        assert cnt2 == 1
