"""Path and batch helpers shared across infrastructure adapters."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

from omegaconf import DictConfig

from saas_pipeline.infrastructure.config.loader import quarantine_path, repo_root, table_path


def new_batch_id() -> str:
    """UTC timestamp + short uuid suffix identifying one pipeline batch."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]


def new_run_id() -> str:
    """Opaque id grouping quality_logs rows for one orchestration run."""
    return "run_" + uuid.uuid4().hex


def abs_path(path_str: str) -> str:
    """Resolve relative paths against the repository root."""
    path = Path(path_str)
    if path.is_absolute():
        return str(path)
    return str(repo_root() / path)


def yyyymmdd(d: date) -> str:
    """Format a date as partition key ``yyyyMMdd``."""
    return d.strftime("%Y%m%d")


def parse_iso_date(value: str) -> date:
    """Parse ``YYYY-MM-DD`` into a ``date``."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range_yyyymmdd(start: str, end: str) -> list[str]:
    """Inclusive list of ``yyyyMMdd`` keys between ISO start/end dates."""
    start_d = parse_iso_date(start)
    end_d = parse_iso_date(end)
    if end_d < start_d:
        raise ValueError(f"end_date {end} is before start_date {start}")
    out: list[str] = []
    cur = start_d
    while cur <= end_d:
        out.append(yyyymmdd(cur))
        cur += timedelta(days=1)
    return out


def bronze_table(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Bronze deliveries Delta for ``tenant``."""
    return abs_path(table_path(str(cfg.paths.bronze), tenant, "deliveries"))


def silver_fact(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Silver ``fact_deliveries`` for ``tenant``."""
    return abs_path(table_path(str(cfg.paths.silver), tenant, "fact_deliveries"))


def silver_dim(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Silver ``dim_materials`` for ``tenant``."""
    return abs_path(table_path(str(cfg.paths.silver), tenant, "dim_materials"))


def gold_metrics(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Gold daily metrics table for ``tenant``."""
    return abs_path(table_path(str(cfg.paths.gold), tenant, "daily_metrics_by_delivery_type"))


def bronze_quarantine(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Bronze quarantine deliveries for ``tenant``."""
    return abs_path(quarantine_path(str(cfg.paths.quarantine_root), "bronze", tenant, "deliveries"))


def silver_quarantine(cfg: DictConfig, tenant: str) -> str:
    """Absolute path to Silver quarantine facts for ``tenant``."""
    return abs_path(
        quarantine_path(str(cfg.paths.quarantine_root), "silver", tenant, "fact_deliveries")
    )


def quality_logs_path(cfg: DictConfig) -> str:
    """Absolute path to shared quality_logs Delta table."""
    return abs_path(str(cfg.paths.quality_logs))
