"""Compatibility shim — prefer infrastructure.spark.session."""

from saas_pipeline.infrastructure.spark.session import build_spark

__all__ = ["build_spark"]
