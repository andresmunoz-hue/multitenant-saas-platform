"""Use case: ingest raw deliveries into Bronze Delta."""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.infrastructure.config.paths import (
    abs_path,
    bronze_quarantine,
    bronze_table,
    date_range_yyyymmdd,
)
from saas_pipeline.infrastructure.spark.delta_io import delta_exists
from saas_pipeline.infrastructure.transforms.bronze_transforms import (
    DELIVERIES_SCHEMA,
    is_valid_fecha,
    prepare_bronze,
)


def ingest_bronze(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    """Ingest RAW deliveries into Bronze Delta for one tenant."""
    start = start_date or str(cfg.execution.start_date)
    end = end_date or str(cfg.execution.end_date)
    partitions = date_range_yyyymmdd(start, end)

    source = abs_path(str(cfg.paths.raw_deliveries))
    raw = prepare_bronze(
        spark.read.option("header", True)
        .schema(DELIVERIES_SCHEMA)
        .csv(source)
        .withColumn("_source_file", F.lit(source)),
        batch_id,
    )
    tenant_df = raw.filter(F.col("_tenant_id") == tenant)

    valid = tenant_df.filter(is_valid_fecha())
    invalid = tenant_df.filter(~is_valid_fecha()).withColumn(
        "_quarantine_reason", F.lit("invalid_or_null_fecha_proceso")
    )
    in_range = valid.filter(F.col("fecha_proceso").isin(partitions))

    target = bronze_table(cfg, tenant)
    replace_list = ",".join(repr(p) for p in partitions)
    replace_where = f"_tenant_id = '{tenant}' AND fecha_proceso IN ({replace_list})"
    if in_range.take(1):
        (
            in_range.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
            .partitionBy("fecha_proceso", "_tenant_id")
            .save(target)
        )
    elif not delta_exists(spark, target):
        (
            spark.createDataFrame([], in_range.schema)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("fecha_proceso", "_tenant_id")
            .save(target)
        )

    q_path = bronze_quarantine(cfg, tenant)
    q_count = invalid.count()
    if q_count:
        (
            invalid.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(q_path)
        )

    return {
        "rows_written": in_range.count(),
        "rows_quarantined_invalid_fecha": q_count,
        "partitions": len(partitions),
    }
