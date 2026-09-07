"""Refactored junior delivery aggregation using Spark-native transforms."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROUTINE_TYPES = ("ZPRE", "ZVE1")
CS_TO_ST_FACTOR = 20


def build_spark(app_name: str = "mentoring-good-code") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def process(
    spark: SparkSession,
    file_path: str,
    tenant_id: str,
    output_path: str,
    valid_types: tuple[str, ...] = ROUTINE_TYPES,
    cs_to_st_factor: int = CS_TO_ST_FACTOR,
) -> DataFrame:
    """Filter routine deliveries for a tenant and write normalized metrics.

    Notes:
    - Uses Spark IO/transforms (no pandas row loops).
    - Tenant path is parameterized (multi-tenant ready).
    - Output path is injected (no hardcoded /tmp).
    """
    tenant = tenant_id.strip().lower()
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
        .withColumn("_tenant_id", F.lower(F.col("pais")))
        .filter(F.col("_tenant_id") == tenant)
        .filter(F.col("tipo_entrega").isin(list(valid_types)))
        .filter(F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
        .filter(F.col("precio").isNotNull())
    )

    result = (
        df.withColumn(
            "cantidad_st",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * F.lit(cs_to_st_factor),
            ).otherwise(F.col("cantidad")),
        )
        .withColumn("total", F.col("cantidad_st") * F.col("precio"))
        .select(
            F.col("_tenant_id").alias("tenant_id"),
            F.col("fecha_proceso").alias("fecha"),
            "material",
            "cantidad_st",
            "total",
        )
    )

    (
        result.write.mode("overwrite")
        .partitionBy("tenant_id")
        .parquet(output_path)
    )
    return result


if __name__ == "__main__":
    session = build_spark()
    try:
        process(
            session,
            file_path="raw/global_mobility_data_entrega_productos.csv",
            tenant_id="gt",
            output_path="data/mentoring_output",
        )
    finally:
        session.stop()
