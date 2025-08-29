import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _schema_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name=%s)", (schema,))
        return bool(cur.fetchone()[0])


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s AND table_type='BASE TABLE'
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def test_swap_and_unswap_roundtrip(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    # Build into an isolated dest schema to test swap without interfering with others
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_swap',
        'tmp_schema': 'tmp_swap',
        'shards_schema': 'shards_swap',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1]}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}},
        ],
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Build the dest schema
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        assert _schema_exists(conn, 'stage_swap')
        assert _table_exists(conn, 'stage_swap', 'store')
        # Ensure old does not exist yet
        assert not _schema_exists(conn, 'old')

    # Swap: public->old, stage_swap->public
    rc2, out2, err2 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--swap",
    ], env={"DATABASE_URL": url})
    assert rc2 == 0, err2

    with psycopg.connect(url) as conn:
        # stage_swap should be gone; store should appear under public
        assert not _schema_exists(conn, 'stage_swap')
        assert _schema_exists(conn, 'old')
        assert _table_exists(conn, 'public', 'store')

    # Unswap: public->stage_swap, old->public (restore)
    rc3, out3, err3 = run_cli([
        "--env", str(tmp_path / 'empty.env'),
        "--config", str(cfg_path),
        "--unswap",
    ], env={"DATABASE_URL": url})
    assert rc3 == 0, err3

    with psycopg.connect(url) as conn:
        # stage_swap restored; old removed; public no longer has store from stage_swap
        assert _schema_exists(conn, 'stage_swap')
        assert not _schema_exists(conn, 'old')
        # After unswap, store returns to stage_swap
        assert _table_exists(conn, 'stage_swap', 'store')
