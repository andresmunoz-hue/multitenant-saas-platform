"""Bronze layer: raw CSV ingestion to partitioned Delta."""

from __future__ import annotations

from datetime import datetime

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from saas_pipeline.paths import abs_path, bronze_quarantine, bronze_table, date_range_yyyymmdd

DELIVERIES_SCHEMA = T.StructType(
    [
        T.StructField("pais", T.StringType(), True),
        T.StructField("fecha_proceso", T.StringType(), True),
        T.StructField("transporte", T.LongType(), True),
        T.StructField("ruta", T.LongType(), True),
        T.StructField("tipo_entrega", T.StringType(), True),
        T.StructField("material", T.StringType(), True),
        T.StructField("precio", T.DecimalType(28, 18), True),
        T.StructField("cantidad", T.DecimalType(28, 18), True),
        T.StructField("unidad", T.StringType(), True),
    ]
)


def _is_valid_fecha(col_name: str = "fecha_proceso"):
    return (
        F.col(col_name).isNotNull()
        & (F.length(F.trim(F.col(col_name).cast("string"))) == 8)
        & F.col(col_name).cast("string").rlike(r"^\d{8}$")
        & F.to_date(F.col(col_name).cast("string"), "yyyyMMdd").isNotNull()
    )


def read_raw_deliveries(spark: SparkSession, cfg: DictConfig) -> DataFrame:
    source = abs_path(str(cfg.paths.raw_deliveries))
    return (
        spark.read.option("header", True)
        .schema(DELIVERIES_SCHEMA)
        .csv(source)
        .withColumn("_source_file", F.lit(source))
    )


def prepare_bronze(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_tenant_id", F.lower(F.trim(F.col("pais"))))
        .withColumn("_ingestion_timestamp", F.lit(datetime.utcnow()).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("fecha_proceso", F.trim(F.col("fecha_proceso").cast("string")))
    )


def run_bronze(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    """Ingest RAW deliveries into Bronze Delta for one tenant.

    Rows with invalid/null fecha_proceso cannot be partitioned: they go to bronze quarantine.
    Idempotency: overwrite partitions for the requested date range via replaceWhere.
    """
    start = start_date or str(cfg.execution.start_date)
    end = end_date or str(cfg.execution.end_date)
    partitions = date_range_yyyymmdd(start, end)

    raw = prepare_bronze(read_raw_deliveries(spark, cfg), batch_id)
    tenant_df = raw.filter(F.col("_tenant_id") == tenant)

    valid = tenant_df.filter(_is_valid_fecha())
    invalid = tenant_df.filter(~_is_valid_fecha()).withColumn(
        "_quarantine_reason", F.lit("invalid_or_null_fecha_proceso")
    )

    # Restrict to requested partition range
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
    elif not _delta_exists(spark, target):
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


def _delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False
