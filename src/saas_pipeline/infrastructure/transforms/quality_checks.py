"""Quality check Spark adapters."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from saas_pipeline.domain.constants import DEFAULT_VALID_DELIVERY_TYPES

QUALITY_SCHEMA = T.StructType(
    [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_batch_id", T.StringType(), False),
        T.StructField("tenant_id", T.StringType(), False),
        T.StructField("layer", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("check_severity", T.StringType(), False),
        T.StructField("records_checked", T.LongType(), False),
        T.StructField("records_failed", T.LongType(), False),
        T.StructField("check_passed", T.BooleanType(), False),
        T.StructField("executed_at", T.TimestampType(), False),
    ]
)


def quality_result(
    run_id: str,
    batch_id: str,
    tenant: str,
    check_name: str,
    severity: str,
    checked: int,
    failed: int,
) -> dict:
    """Build one quality_logs row; ``check_passed`` is True when ``failed == 0``."""
    return {
        "_run_id": run_id,
        "_batch_id": batch_id,
        "tenant_id": tenant,
        "layer": "silver",
        "table_name": "fact_deliveries",
        "check_name": check_name,
        "check_severity": severity,
        "records_checked": int(checked),
        "records_failed": int(failed),
        "check_passed": failed == 0,
        "executed_at": datetime.utcnow().isoformat(),
    }


def check_qty_st_positive(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    """Critical: ``cantidad_normalizada_st`` must be present and > 0."""
    checked = df.count()
    failed = df.filter(
        F.col("cantidad_normalizada_st").isNull() | (F.col("cantidad_normalizada_st") <= 0)
    ).count()
    return quality_result(
        run_id, batch_id, tenant, "qty_st_positive", "critical", checked, failed
    )


def check_delivery_type_domain(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    """Warning: ``tipo_entrega`` must be in the default valid domain."""
    valid = list(DEFAULT_VALID_DELIVERY_TYPES)
    checked = df.count()
    failed = df.filter(~F.col("tipo_entrega").isin(valid)).count()
    return quality_result(
        run_id, batch_id, tenant, "delivery_type_domain", "warning", checked, failed
    )


def check_fk_material_resolved(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    """Critical: SCD enrichment left ``material_descripcion`` / ``material_categoria``."""
    checked = df.count()
    failed = df.filter(
        F.col("material_descripcion").isNull() | F.col("material_categoria").isNull()
    ).count()
    return quality_result(
        run_id, batch_id, tenant, "fk_material_resolved", "critical", checked, failed
    )


def check_unit_normalized(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    """Info: rows with unidad CS must satisfy ST = original × 20."""
    checked = df.count()
    failed = df.filter(
        (F.upper(F.col("unidad_original")) == "CS")
        & (F.col("cantidad_normalizada_st") != F.col("cantidad_original") * F.lit(20))
    ).count()
    return quality_result(
        run_id, batch_id, tenant, "unit_normalized_st", "info", checked, failed
    )
