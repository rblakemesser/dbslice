import yaml
from pathlib import Path

from test_cli import run_cli


def test_audit_tables_all_reports_missing_stage(tmp_path):
    # Use compose DB and minicom config; stage schema should not exist
    base_cfg = Path('tests/fixtures/minicom/minicom.yml').read_text()
    # Use an isolated dest schema for this test to avoid interference
    tmp_cfg_path = tmp_path / "minicom_audit.yml"
    tmp_cfg_path.write_text(base_cfg.replace('dest_schema: stage', 'dest_schema: stage_audit'))
    empty_env_path = tmp_path / "empty.env"
    empty_env_path.write_text("\n")
    rc, out, err = run_cli([
        "--env", str(empty_env_path),
        "--config", str(tmp_cfg_path),
        "--audit-tables",
    ], env={"DATABASE_URL": "postgresql://postgres:postgres@db:5432/postgres"})
    assert rc == 0, err
    data = yaml.safe_load(out)
    # Expect several tables reported (at least store/product/order, etc.)
    assert 'store' in data
    assert data['store']['exists_diff']['dst_missing'] is True
    # Sanity: all reported tables should claim dst missing
    for tbl, rpt in data.items():
        assert rpt['exists_diff']['dst_missing'] is True
