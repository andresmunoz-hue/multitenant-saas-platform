"""Use case: build Gold daily metrics."""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.infrastructure.config.paths import gold_metrics, silver_fact


def build_gold(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    """Aggregate Silver facts into Gold ``daily_metrics_by_delivery_type`` (replaceWhere)."""
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

    metrics = fact.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega").agg(
        F.sum("cantidad_normalizada_st").alias("total_units"),
        F.sum(F.col("cantidad_normalizada_st") * F.col("precio_transaccion")).alias(
            "total_revenue"
        ),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports"),
    )

    target = gold_metrics(cfg, tenant)
    dates = [r.fecha_proceso for r in metrics.select("fecha_proceso").distinct().collect()]
    if not dates:
        (
            metrics.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("fecha_proceso")
            .save(target)
        )
        return {"metric_rows": 0}

    replace = "fecha_proceso IN (" + ",".join(repr(d) for d in dates) + ")"
    (
        metrics.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace)
        .partitionBy("fecha_proceso")
        .save(target)
    )
    return {"metric_rows": metrics.count()}
