"""Compatibility shim — prefer infrastructure.config.loader."""

from saas_pipeline.domain.constants import KNOWN_TENANTS
from saas_pipeline.infrastructure.config.loader import (
    config_dir,
    load_config,
    quarantine_path,
    repo_root,
    resolve_tenants,
    table_path,
)

__all__ = [
    "KNOWN_TENANTS",
    "repo_root",
    "config_dir",
    "load_config",
    "resolve_tenants",
    "table_path",
    "quarantine_path",
]
