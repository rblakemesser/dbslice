import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _fetch_vals(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, email FROM "{schema}"."{table}" ORDER BY id')
        return cur.fetchall()


def test_neuter_parallel_sharded_prefix(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    schema = 'stage_nparallel'
    table = 'big_emails'
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': schema,
        'tmp_schema': 'tmp_nparallel',
        'neuter': {
            'enabled': True,
            'parallel': 4,
            'targets': {
                table: [
                    {
                        'column': 'email',
                        'strategy': 'prefix',
                        'value': 'x-',
                        'shard': {'column': 'id', 'modulo': 4},
                    }
                ]
            }
        },
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Prepare dest schema and a large table
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id int primary key, email text not null)')
            args = ",".join([f"({i}, 'u{i}@ex.com')" for i in range(1, 201)])
            cur.execute(f'INSERT INTO "{schema}"."{table}" VALUES ' + args)
        conn.commit()

    # Run neuter-only with parallel sharding
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--neuter-only",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        rows = _fetch_vals(conn, schema, table)
        assert len(rows) == 200
        assert all(e.startswith('x-') for _, e in rows)

    # Run again; ensure idempotent (no double prefix)
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--neuter-only",
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        rows2 = _fetch_vals(conn, schema, table)
        assert rows == rows2

