"""Use case: run Silver quality checks and persist quality_logs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.domain.exceptions import CriticalQualityError
from saas_pipeline.infrastructure.config.paths import quality_logs_path, silver_fact
from saas_pipeline.infrastructure.transforms.quality_checks import (
    QUALITY_SCHEMA,
    check_delivery_type_domain,
    check_fk_material_resolved,
    check_qty_st_positive,
    check_unit_normalized,
)


def run_quality_checks(
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

    path = quality_logs_path(cfg)
    with tempfile.TemporaryDirectory() as tmp:
        json_path = Path(tmp) / "quality.json"
        with json_path.open("w", encoding="utf-8") as handle:
            for row in results:
                handle.write(json.dumps(row) + "\n")
        df = (
            spark.read.json(str(json_path))
            .withColumn("executed_at", F.to_timestamp("executed_at"))
            .withColumn("records_checked", F.col("records_checked").cast("long"))
            .withColumn("records_failed", F.col("records_failed").cast("long"))
            .withColumn("check_passed", F.col("check_passed").cast("boolean"))
            .select([f.name for f in QUALITY_SCHEMA.fields])
        )
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

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
