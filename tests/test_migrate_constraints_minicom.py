import psycopg
from pathlib import Path

from test_cli import run_cli, write_overlay_config


def _constraint_names(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT conname FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = %s AND t.relname = %s
            ORDER BY conname
            """,
            (schema, table),
        )
        return {r[0] for r in (cur.fetchall() or [])}


def test_migrate_constraints_all(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_cons',
        'tmp_schema': 'tmp_cons',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_cons CASCADE')
            cur.execute('CREATE SCHEMA stage_cons')
            for t in ('store','customer','order','order_item','product'):
                cur.execute(f'CREATE TABLE stage_cons."{t}" (LIKE minicom."{t}" INCLUDING DEFAULTS)')
        conn.commit()

    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate-constraints",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        cons_oi = _constraint_names(conn, 'stage_cons', 'order_item')
        assert 'order_item_order_fk' in cons_oi
        assert 'order_item_product_fk' in cons_oi
        cons_order = _constraint_names(conn, 'stage_cons', 'order')
        assert 'order_store_fk' in cons_order
        cons_prod = _constraint_names(conn, 'stage_cons', 'product')
        assert any('check' in name or 'price' in name for name in cons_prod)


def test_migrate_runs_constraints_post_families(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_cons_mig',
        'tmp_schema': 'tmp_cons_mig',
        'roots': [
            {'name': 'stores', 'table': 'store', 'id_col': 'id', 'selector': {'mode': 'list', 'ids': [1,2]}},
        ],
        'families': [
            {'name': 'store', 'root': {'table': 'store', 'id_col': 'id', 'selection': 'stores', 'join': 'd.id = p.id'}, 'deps': [
                {'table': 'product', 'parent_table': 'store', 'join': 'd.store_id = p.id'}
            ]}
        ],
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
        # Product check constraints should exist in dest after migrate
        cons_prod = _constraint_names(conn, 'stage_cons_mig', 'product')
        assert any('check' in name or 'price' in name for name in cons_prod)


def _pg_get_constraint_def(conn, schema: str, table: str, name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_get_constraintdef(con.oid, true)
            FROM pg_constraint con
            JOIN pg_class t ON t.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = %s AND t.relname = %s AND con.conname = %s
            """,
            (schema, table, name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def test_migrate_constraints_strict_replace_drop_validate(tmp_path):
    base = Path('tests/fixtures/minicom/minicom.yml').resolve()
    overlay = {
        'source_schema': 'minicom',
        'dest_schema': 'stage_cons_strict',
        'tmp_schema': 'tmp_cons_strict',
    }
    cfg_path = write_overlay_config(str(base), overlay, tmp_path)
    url = "postgresql://postgres:postgres@db:5432/postgres"

    # Prepare dest tables and inject extraneous + mismatched constraints
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS stage_cons_strict CASCADE')
            cur.execute('CREATE SCHEMA stage_cons_strict')
            for t in ('store','product','order','order_item'):
                cur.execute(f'CREATE TABLE stage_cons_strict."{t}" (LIKE minicom."{t}" INCLUDING DEFAULTS)')
            # Extraneous check on product
            cur.execute('ALTER TABLE stage_cons_strict."product" ADD CONSTRAINT spurious_check CHECK (1=1)')
            # Ensure parent tables have PKs so our synthetic mismatched FK and mirror adds can succeed
            cur.execute('ALTER TABLE stage_cons_strict."order" ADD CONSTRAINT order_pkey PRIMARY KEY (id)')
            cur.execute('ALTER TABLE stage_cons_strict."store" ADD CONSTRAINT store_pkey PRIMARY KEY (id)')
        conn.commit()

        # Determine a known FK name on minicom.order_item to create a mismatched same-named FK in dest
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class t ON t.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = 'minicom' AND t.relname = 'order_item' AND con.contype = 'f'
                ORDER BY con.conname
                """
            )
            fk_row = cur.fetchone()
            assert fk_row, 'Expected at least one FK on minicom.order_item'
            fk_name = fk_row[0]
        # Create a mismatched FK with the same name in dest (e.g., add DEFERRABLE)
        with conn.cursor() as cur:
            cur.execute(
                f'ALTER TABLE stage_cons_strict."order_item" ADD CONSTRAINT "{fk_name}" FOREIGN KEY (order_id) REFERENCES stage_cons_strict."order"(id) DEFERRABLE INITIALLY IMMEDIATE'
            )
        conn.commit()

    # Run migrate-constraints
    rc, out, err = run_cli([
        "--env", str((tmp_path / 'empty.env').write_text('\n') or (tmp_path / 'empty.env')),
        "--config", str(cfg_path),
        "--migrate-constraints",
    ], env={"DATABASE_URL": url})
    assert rc == 0, err

    with psycopg.connect(url) as conn:
        # Extraneous constraint should be dropped
        assert 'spurious_check' not in _constraint_names(conn, 'stage_cons_strict', 'product')

        # Mismatched FK should be replaced to match source (canonical form)
        pub_def = _pg_get_constraint_def(conn, 'minicom', 'order_item', fk_name)
        dst_def = _pg_get_constraint_def(conn, 'stage_cons_strict', 'order_item', fk_name)
        assert pub_def and dst_def
        # Canonicalize: ignore NOT VALID and schema qualifier differences
        import re
        def _canon(s: str) -> str:
            s1 = re.sub(r"\s+", " ", s.strip())
            s1 = re.sub(r"\s+NOT\s+VALID\b", "", s1, flags=re.IGNORECASE)
            s1 = s1.replace('REFERENCES "minicom".', 'REFERENCES ').replace('REFERENCES minicom.', 'REFERENCES ')
            s1 = s1.replace('REFERENCES "stage_cons_strict".', 'REFERENCES ').replace('REFERENCES stage_cons_strict.', 'REFERENCES ')
            return s1
        assert _canon(pub_def) == _canon(dst_def)

        # All FKs in dest should be validated
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_constraint con
                JOIN pg_class t ON t.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE con.contype='f' AND n.nspname = 'stage_cons_strict' AND con.convalidated = false
                """
            )
            not_valid = cur.fetchone()[0]
        assert not_valid == 0
