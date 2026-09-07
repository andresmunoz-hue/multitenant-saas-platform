"""Bronze Spark transforms and schemas."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


def is_valid_fecha(col_name: str = "fecha_proceso"):
    """Spark predicate: non-null ``yyyyMMdd`` string that parses to a real date."""
    return (
        F.col(col_name).isNotNull()
        & (F.length(F.trim(F.col(col_name).cast("string"))) == 8)
        & F.col(col_name).cast("string").rlike(r"^\d{8}$")
        & F.to_date(F.col(col_name).cast("string"), "yyyyMMdd").isNotNull()
    )


def prepare_bronze(df: DataFrame, batch_id: str) -> DataFrame:
    """Attach tenant id (from ``pais``), batch id, and ingestion timestamp."""
    return (
        df.withColumn("_tenant_id", F.lower(F.trim(F.col("pais"))))
        .withColumn("_ingestion_timestamp", F.lit(datetime.utcnow()).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("fecha_proceso", F.trim(F.col("fecha_proceso").cast("string")))
    )
