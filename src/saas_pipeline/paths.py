"""Compatibility shim — prefer infrastructure.config.paths."""

from saas_pipeline.infrastructure.config.paths import (
    abs_path,
    bronze_quarantine,
    bronze_table,
    date_range_yyyymmdd,
    gold_metrics,
    new_batch_id,
    new_run_id,
    parse_iso_date,
    quality_logs_path,
    silver_dim,
    silver_fact,
    silver_quarantine,
    yyyymmdd,
)

__all__ = [
    "new_batch_id",
    "new_run_id",
    "abs_path",
    "yyyymmdd",
    "parse_iso_date",
    "date_range_yyyymmdd",
    "bronze_table",
    "silver_fact",
    "silver_dim",
    "gold_metrics",
    "bronze_quarantine",
    "silver_quarantine",
    "quality_logs_path",
]
