import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _fetch_customers(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f'SELECT email, password FROM "{schema}".customer ORDER BY id')
        return cur.fetchall()


def test_neuter_only_with_config(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_neuter',
        'tmp_schema': 'tmp_neuter',
        'neuter': {
            'enabled': True,
            'targets': {
                'customer': [
                    {'column': 'password', 'strategy': 'replace', 'value': 'HASHED'},
                    {'column': 'email', 'strategy': 'prefix', 'value': 'x-'}
                ]
            }
        },
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Create dest table from source
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_neuter CASCADE')
            cur.execute('CREATE SCHEMA stage_neuter')
            cur.execute('CREATE TABLE stage_neuter.customer (LIKE minicom.customer INCLUDING DEFAULTS)')
            cur.execute('INSERT INTO stage_neuter.customer SELECT * FROM minicom.customer')
        conn.commit()

    # Run neuter only
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--neuter-only",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        rows = _fetch_customers(conn, 'stage_neuter')
        assert rows[0][0].startswith('x-') and rows[1][0].startswith('x-')
        assert rows[0][1] == 'HASHED' and rows[1][1] == 'HASHED'

    # Run again; ensure not double-prefixed
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--neuter-only",
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        rows2 = _fetch_customers(conn, 'stage_neuter')
        assert rows == rows2


def test_migrate_performs_neuter_when_enabled(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_neutermig',
        'tmp_schema': 'tmp_neutermig',
        'roots': [
            {'name': 'customers', 'table': 'customer', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1,2]}},
        ],
        'families': [
            {'name': 'customer', 'root': {'table': 'customer', 'id_col': 'id', 'selection': 'customers', 'join': 'd.id = p.id'}},
        ],
        'neuter': {
            'enabled': True,
            'targets': {
                'customer': [
                    {'column': 'password', 'strategy': 'replace', 'value': 'HASHED2'},
                    {'column': 'email', 'strategy': 'prefix', 'value': 'x-'}
                ]
            }
        },
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty2.env').write_text('\n') or (tmp_path / 'empty2.env')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        rows = _fetch_customers(conn, 'stage_neutermig')
        assert all(e.startswith('x-') for e, _ in rows)
        assert all(p == 'HASHED2' for _, p in rows)
