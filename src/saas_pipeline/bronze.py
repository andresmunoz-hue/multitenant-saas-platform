"""Compatibility shim — prefer application.use_cases.ingest_bronze."""

from saas_pipeline.application.use_cases.ingest_bronze import ingest_bronze as run_bronze

__all__ = ["run_bronze"]
