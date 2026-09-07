"""Compatibility shim — prefer application.use_cases.build_silver + domain.rules."""

from saas_pipeline.application.use_cases.build_silver import build_silver as run_silver
from saas_pipeline.domain.rules.delivery_types import is_valid_delivery_type
from saas_pipeline.domain.rules.units import cs_to_st
from saas_pipeline.infrastructure.transforms.silver_transforms import (
    apply_anomaly_rules,
    enrich_with_scd,
    normalize_units,
)

__all__ = [
    "run_silver",
    "cs_to_st",
    "is_valid_delivery_type",
    "normalize_units",
    "apply_anomaly_rules",
    "enrich_with_scd",
]
