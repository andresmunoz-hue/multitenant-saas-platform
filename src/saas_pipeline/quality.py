"""Compatibility shim — prefer application.use_cases.run_quality_checks."""

from saas_pipeline.application.use_cases.run_quality_checks import run_quality_checks as run_quality
from saas_pipeline.domain.exceptions import CriticalQualityError
from saas_pipeline.infrastructure.transforms.quality_checks import (
    check_delivery_type_domain,
    check_fk_material_resolved,
    check_qty_st_positive,
    check_unit_normalized,
)

__all__ = [
    "run_quality",
    "CriticalQualityError",
    "check_qty_st_positive",
    "check_delivery_type_domain",
    "check_fk_material_resolved",
    "check_unit_normalized",
]
