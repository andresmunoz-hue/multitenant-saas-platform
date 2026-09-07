"""Unit conversion rules (pure)."""

from saas_pipeline.domain.constants import DEFAULT_CS_TO_ST_FACTOR


def cs_to_st(
    cantidad: float, unidad: str, factor: int = DEFAULT_CS_TO_ST_FACTOR
) -> float:
    if str(unidad).upper() == "CS":
        return float(cantidad) * factor
    return float(cantidad)
