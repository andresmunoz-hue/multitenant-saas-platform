"""Hierarchical configuration loader (base <- env <- tenant <- CLI)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf

from saas_pipeline.domain.constants import KNOWN_TENANTS


def repo_root() -> Path:
    """Resolve repository root regardless of module depth."""
    here = Path(__file__).resolve()
    for candidate in [here, *here.parents]:
        has_config = (candidate / "config" / "base.yaml").exists()
        has_project = (candidate / "pyproject.toml").exists()
        if has_config and has_project:
            return candidate
    # Fallback: src/saas_pipeline/infrastructure/config -> 4 levels up to repo
    return here.parents[4]


def config_dir() -> Path:
    return repo_root() / "config"


def load_config(
    env: str = "dev",
    tenant: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> DictConfig:
    """Load and merge YAML configs.

    Precedence: ``base.yaml`` < ``env/<env>.yaml`` < ``tenants/<tenant>.yaml`` < overrides.
    When tenant is ``\"all\"`` or None, the tenant-specific file is skipped.
    """
    base_path = config_dir() / "base.yaml"
    env_path = config_dir() / "env" / f"{env}.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"Missing env config: {env_path}")

    cfg = OmegaConf.merge(
        OmegaConf.load(base_path),
        OmegaConf.load(env_path),
    )

    tenant_norm = (tenant or OmegaConf.select(cfg, "execution.tenant") or "all").lower()
    if tenant_norm != "all":
        tenant_path = config_dir() / "tenants" / f"{tenant_norm}.yaml"
        if not tenant_path.exists():
            raise FileNotFoundError(f"Missing tenant config: {tenant_path}")
        cfg = OmegaConf.merge(cfg, OmegaConf.load(tenant_path))

    cfg = OmegaConf.merge(cfg, {"execution": {"tenant": tenant_norm}})

    if overrides:
        cfg = OmegaConf.merge(cfg, OmegaConf.create(overrides))

    OmegaConf.resolve(cfg)
    return cfg  # type: ignore[return-value]


def resolve_tenants(cfg: DictConfig) -> list[str]:
    """Resolve ``execution.tenant`` to a concrete list (``all`` expands ``KNOWN_TENANTS``)."""
    tenant = str(cfg.execution.tenant).lower()
    if tenant == "all":
        return list(KNOWN_TENANTS)
    if tenant not in KNOWN_TENANTS:
        raise ValueError(
            f"Unknown tenant '{tenant}'. Expected one of {KNOWN_TENANTS} or 'all'."
        )
    return [tenant]


def table_path(base: str, tenant: str, table: str) -> str:
    return str(Path(base) / tenant / table)


def quarantine_path(quarantine_root: str, layer: str, tenant: str, table: str) -> str:
    return str(Path(quarantine_root) / f"{layer}_quarantine" / tenant / table)
