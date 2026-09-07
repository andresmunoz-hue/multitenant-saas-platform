"""Delivery type domain rules (pure)."""

from saas_pipeline.domain.constants import DEFAULT_VALID_DELIVERY_TYPES


def is_valid_delivery_type(
    tipo: str, valid_types: list[str] | tuple[str, ...] | None = None
) -> bool:
    valid = valid_types or DEFAULT_VALID_DELIVERY_TYPES
    return tipo in valid
