"""Silver Spark transforms implementing domain rules on DataFrames."""

from __future__ import annotations

from datetime import datetime

from omegaconf import DictConfig
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

MATERIALS_SCHEMA = T.StructType(
    [
        T.StructField("material", T.StringType(), True),
        T.StructField("descripcion", T.StringType(), True),
        T.StructField("categoria", T.StringType(), True),
        T.StructField("precio_base", T.DecimalType(28, 18), True),
        T.StructField("valid_from", T.StringType(), True),
        T.StructField("valid_to", T.StringType(), True),
        T.StructField("is_current", T.BooleanType(), True),
    ]
)


def normalize_units(df: DataFrame, cs_to_st_factor: int) -> DataFrame:
    """Add ``cantidad_normalizada_st`` (CS × factor, otherwise raw ``cantidad``)."""
    return df.withColumn(
        "cantidad_normalizada_st",
        F.when(
            F.upper(F.col("unidad")) == "CS",
            F.col("cantidad") * F.lit(cs_to_st_factor),
        ).otherwise(F.col("cantidad")),
    )


def classify_delivery_flags(df: DataFrame, routine: list[str], bonus: list[str]) -> DataFrame:
    """Flag routine vs bonus delivery types as boolean columns."""
    return df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(routine)).withColumn(
        "is_bonus_delivery", F.col("tipo_entrega").isin(bonus)
    )


def apply_anomaly_rules(
    bronze_df: DataFrame,
    dim_df: DataFrame,
    valid_types: list[str],
) -> tuple[DataFrame, DataFrame, int]:
    """Apply section 5.6 rules.

    Priority: fecha (already handled in bronze) → cantidad/precio → material → tipo.
    Returns: (clean_df, quarantine_df, discarded_count)
    """
    df = bronze_df.dropDuplicates(
        [
            "pais",
            "fecha_proceso",
            "transporte",
            "ruta",
            "tipo_entrega",
            "material",
            "precio",
            "cantidad",
            "unidad",
        ]
    )

    catalog_keys = dim_df.select("material").distinct()
    df = df.join(catalog_keys.withColumn("_in_catalog", F.lit(True)), on="material", how="left")

    qty_bad = F.col("cantidad").isNull() | (F.col("cantidad") <= 0)
    price_bad = F.col("precio").isNull()
    material_bad = F.col("_in_catalog").isNull()
    type_bad = ~F.col("tipo_entrega").isin(valid_types)

    quarantine = (
        df.filter(qty_bad | price_bad | material_bad)
        .withColumn(
            "_quarantine_reason",
            F.when(qty_bad, F.lit("invalid_cantidad"))
            .when(price_bad, F.lit("null_precio"))
            .otherwise(F.lit("material_not_in_catalog")),
        )
        .drop("_in_catalog")
    )

    discarded = df.filter(~qty_bad & ~price_bad & ~material_bad & type_bad)
    discarded_count = discarded.count()

    clean = df.filter(~qty_bad & ~price_bad & ~material_bad & ~type_bad).drop("_in_catalog")
    return clean, quarantine, discarded_count


def enrich_with_scd(fact_df: DataFrame, dim_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Temporal join: fecha_proceso BETWEEN valid_from AND valid_to (inclusive)."""
    facts = fact_df.withColumn("fecha_proceso_date", F.to_date("fecha_proceso", "yyyyMMdd"))
    dim = dim_df.select(
        F.col("material").alias("dim_material"),
        F.col("descripcion").alias("material_descripcion"),
        F.col("categoria").alias("material_categoria"),
        F.col("precio_base"),
        F.col("valid_from"),
        F.col("valid_to"),
    )

    joined = facts.join(
        dim,
        (facts.material == dim.dim_material)
        & (facts.fecha_proceso_date >= dim.valid_from)
        & (facts.fecha_proceso_date <= dim.valid_to),
        how="left",
    )

    matched = joined.filter(F.col("dim_material").isNotNull()).drop(
        "dim_material", "valid_from", "valid_to"
    )
    unmatched = (
        joined.filter(F.col("dim_material").isNull())
        .drop(
            "dim_material",
            "material_descripcion",
            "material_categoria",
            "precio_base",
            "valid_from",
            "valid_to",
        )
        .withColumn("_quarantine_reason", F.lit("no_scd_version_for_fecha"))
    )
    return matched, unmatched


def build_fact_payload(
    clean: DataFrame,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
) -> DataFrame:
    """Project enriched deliveries into the Silver ``fact_deliveries`` column set."""
    factor = int(cfg.business.cs_to_st_factor)
    routine = list(cfg.business.routine_types)
    bonus = list(cfg.business.bonus_types)

    df = normalize_units(clean, factor)
    df = classify_delivery_flags(df, routine, bonus)
    return (
        df.withColumn("_tenant_id", F.lit(tenant))
        .withColumn("precio_transaccion", F.col("precio"))
        .withColumn("cantidad_original", F.col("cantidad"))
        .withColumn("unidad_original", F.col("unidad"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_updated_at", F.lit(datetime.utcnow()).cast("timestamp"))
        .select(
            "_tenant_id",
            "fecha_proceso",
            "transporte",
            "ruta",
            "material",
            "tipo_entrega",
            "precio_transaccion",
            "cantidad_original",
            "unidad_original",
            "cantidad_normalizada_st",
            "is_routine_delivery",
            "is_bonus_delivery",
            "material_descripcion",
            "material_categoria",
            "precio_base",
            "_batch_id",
            "_updated_at",
            "fecha_proceso_date",
        )
    )
