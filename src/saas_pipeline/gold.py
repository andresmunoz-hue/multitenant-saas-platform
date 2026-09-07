"""Compatibility shim — prefer application.use_cases.build_gold."""

from saas_pipeline.application.use_cases.build_gold import build_gold as run_gold

__all__ = ["run_gold"]
