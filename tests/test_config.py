"""Config loading tests."""

from saas_pipeline.config import KNOWN_TENANTS, load_config, resolve_tenants


def test_load_base_and_dev_paths():
    cfg = load_config(env="dev", tenant="all")
    assert cfg.paths.bronze == "data/bronze"
    assert cfg.business.cs_to_st_factor == 20
    assert cfg.execution.tenant == "all"


def test_env_override_changes_paths():
    cfg = load_config(env="qa", tenant="all")
    assert cfg.paths.bronze == "data/qa/bronze"
    assert cfg.quality.fail_on_critical is True


def test_tenant_override_and_resolve():
    cfg = load_config(env="dev", tenant="sv")
    assert cfg.execution.tenant == "sv"
    assert resolve_tenants(cfg) == ["sv"]
    cfg_all = load_config(env="dev", tenant="all")
    assert resolve_tenants(cfg_all) == list(KNOWN_TENANTS)


def test_cli_overrides_dates():
    cfg = load_config(
        env="dev",
        tenant="hn",
        overrides={"execution": {"start_date": "2025-03-01", "end_date": "2025-03-31"}},
    )
    assert cfg.execution.start_date == "2025-03-01"
    assert cfg.execution.end_date == "2025-03-31"
    assert cfg.execution.tenant == "hn"


def test_batch2_env_and_raw_path_override():
    cfg = load_config(env="batch2", tenant="sv")
    assert "batch2" in str(cfg.paths.raw_deliveries)
    assert cfg.paths.bronze == "data/bronze_batch2"
    cfg2 = load_config(
        env="dev",
        tenant="sv",
        overrides={"paths": {"raw_deliveries": "raw/custom.csv"}},
    )
    assert cfg2.paths.raw_deliveries == "raw/custom.csv"
