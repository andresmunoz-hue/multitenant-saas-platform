"""Data quality checks persisted to shared quality_logs Delta table."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from saas_pipeline.paths import quality_logs_path, silver_fact

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


class CriticalQualityError(RuntimeError):
    """Raised when a critical check fails and fail_on_critical is enabled."""


def _result(
    run_id: str,
    batch_id: str,
    tenant: str,
    check_name: str,
    severity: str,
    checked: int,
    failed: int,
) -> dict:
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
    checked = df.count()
    failed = df.filter(
        F.col("cantidad_normalizada_st").isNull() | (F.col("cantidad_normalizada_st") <= 0)
    ).count()
    return _result(run_id, batch_id, tenant, "qty_st_positive", "critical", checked, failed)


def check_delivery_type_domain(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    valid = ["ZPRE", "ZVE1", "Z04", "Z05"]
    checked = df.count()
    failed = df.filter(~F.col("tipo_entrega").isin(valid)).count()
    return _result(run_id, batch_id, tenant, "delivery_type_domain", "warning", checked, failed)


def check_fk_material_resolved(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    checked = df.count()
    failed = df.filter(
        F.col("material_descripcion").isNull() | F.col("material_categoria").isNull()
    ).count()
    return _result(run_id, batch_id, tenant, "fk_material_resolved", "critical", checked, failed)


def check_unit_normalized(df: DataFrame, run_id: str, batch_id: str, tenant: str) -> dict:
    """Info check: CS rows must have cantidad_normalizada_st == cantidad_original * 20."""
    checked = df.count()
    failed = df.filter(
        (F.upper(F.col("unidad_original")) == "CS")
        & (F.col("cantidad_normalizada_st") != F.col("cantidad_original") * F.lit(20))
    ).count()
    return _result(run_id, batch_id, tenant, "unit_normalized_st", "info", checked, failed)


def persist_quality_logs(spark: SparkSession, cfg: DictConfig, rows: list[dict]) -> None:
    """Persist via JSON files to avoid Windows Python-worker crashes on createDataFrame."""
    path = quality_logs_path(cfg)
    with tempfile.TemporaryDirectory() as tmp:
        json_path = Path(tmp) / "quality.json"
        with json_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        df = (
            spark.read.json(str(json_path))
            .withColumn("executed_at", F.to_timestamp("executed_at"))
            .withColumn("records_checked", F.col("records_checked").cast("long"))
            .withColumn("records_failed", F.col("records_failed").cast("long"))
            .withColumn("check_passed", F.col("check_passed").cast("boolean"))
            .select([f.name for f in QUALITY_SCHEMA.fields])
        )
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path)
        )


def run_quality(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    run_id: str,
    batch_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    start = (start_date or str(cfg.execution.start_date)).replace("-", "")
    end = (end_date or str(cfg.execution.end_date)).replace("-", "")

    fact = (
        spark.read.format("delta")
        .load(silver_fact(cfg, tenant))
        .filter(
            (F.col("_tenant_id") == tenant)
            & (F.col("fecha_proceso") >= start)
            & (F.col("fecha_proceso") <= end)
        )
    )

    results = [
        check_fk_material_resolved(fact, run_id, batch_id, tenant),
        check_qty_st_positive(fact, run_id, batch_id, tenant),
        check_delivery_type_domain(fact, run_id, batch_id, tenant),
        check_unit_normalized(fact, run_id, batch_id, tenant),
    ]
    persist_quality_logs(spark, cfg, results)

    fail_on_critical = bool(cfg.quality.fail_on_critical)
    critical_failures = [
        r for r in results if r["check_severity"] == "critical" and not r["check_passed"]
    ]
    if fail_on_critical and critical_failures:
        names = ", ".join(r["check_name"] for r in critical_failures)
        raise CriticalQualityError(
            f"Critical quality checks failed for tenant={tenant}: {names}"
        )
    return results
