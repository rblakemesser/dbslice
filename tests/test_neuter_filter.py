import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _fetch_emails(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(f'SELECT email FROM "{schema}"."{table}" ORDER BY 1')
        return [r[0] for r in (cur.fetchall() or [])]


def test_neuter_only_filters_to_single_table(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    schema = 'stage_neuterf'
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': schema,
        'tmp_schema': 'tmp_neuterf',
        'neuter': {
            'enabled': True,
            'targets': {
                'customer': [
                    {'column': 'email', 'strategy': 'prefix', 'value': 'x-'}
                ],
                'other': [
                    {'column': 'email', 'strategy': 'prefix', 'value': 'y-'}
                ],
            }
        },
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Prepare schema: copy customer from source; create 'other'
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}".customer (LIKE minicom.customer INCLUDING DEFAULTS)')
            cur.execute(f'INSERT INTO "{schema}".customer SELECT * FROM minicom.customer')
            cur.execute(f'CREATE TABLE "{schema}".other (id serial primary key, email text)')
            cur.execute(f"INSERT INTO \"{schema}\".other (email) VALUES ('a@x.com'), ('b@y.com')")
        conn.commit()

    # Run neuter-only filtered to 'customer'
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--neuter-only", 'customer',
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        emails_customer = _fetch_emails(conn, schema, 'customer')
        emails_other = _fetch_emails(conn, schema, 'other')
        assert all(e.startswith('x-') for e in emails_customer if e)
        assert all(not e.startswith('y-') and not e.startswith('x-') for e in emails_other if e)

