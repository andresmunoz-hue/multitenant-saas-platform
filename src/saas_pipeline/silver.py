"""Silver layer: SCD2 materials, anomaly handling, fact_deliveries MERGE + temporal join."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from saas_pipeline.paths import (
    abs_path,
    bronze_table,
    silver_dim,
    silver_fact,
    silver_quarantine,
)

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


def read_raw_materials(spark: SparkSession, cfg: DictConfig) -> DataFrame:
    source = abs_path(str(cfg.paths.raw_materials))
    return spark.read.option("header", True).schema(MATERIALS_SCHEMA).csv(source)


def upsert_dim_materials(
    spark: SparkSession, cfg: DictConfig, tenant: str, batch_id: str
) -> DataFrame:
    """Load catalog as SCD Type 2 and MERGE into silver dim_materials."""
    src = (
        read_raw_materials(spark, cfg)
        .withColumn("valid_from", F.to_date("valid_from", "yyyy-MM-dd"))
        .withColumn("valid_to", F.to_date("valid_to", "yyyy-MM-dd"))
        .withColumn("_tenant_id", F.lit(tenant))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_updated_at", F.lit(datetime.utcnow()).cast("timestamp"))
        .dropDuplicates(["material", "valid_from"])
    )

    target_path = silver_dim(cfg, tenant)
    if not _delta_exists(spark, target_path):
        (
            src.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )
        return spark.read.format("delta").load(target_path)

    delta = DeltaTable.forPath(spark, target_path)
    (
        delta.alias("t")
        .merge(
            src.alias("s"),
            "t.material = s.material AND t.valid_from = s.valid_from "
            "AND t._tenant_id = s._tenant_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.read.format("delta").load(target_path)


def normalize_units(df: DataFrame, cs_to_st_factor: int) -> DataFrame:
    return df.withColumn(
        "cantidad_normalizada_st",
        F.when(
            F.upper(F.col("unidad")) == "CS",
            F.col("cantidad") * F.lit(cs_to_st_factor),
        ).otherwise(F.col("cantidad")),
    )


def classify_delivery_flags(df: DataFrame, routine: list[str], bonus: list[str]) -> DataFrame:
    return (
        df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(routine))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(bonus))
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

    qty_bad = (
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
    )
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

    clean = (
        df.filter(~qty_bad & ~price_bad & ~material_bad & ~type_bad)
        .drop("_in_catalog")
    )
    return clean, quarantine, discarded_count


def enrich_with_scd(
    fact_df: DataFrame, dim_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
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


def merge_fact_deliveries(
    spark: SparkSession, cfg: DictConfig, tenant: str, facts: DataFrame
) -> None:
    target_path = silver_fact(cfg, tenant)
    payload = facts.drop("fecha_proceso_date")

    if not _delta_exists(spark, target_path):
        (
            payload.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("fecha_proceso")
            .save(target_path)
        )
        return

    delta = DeltaTable.forPath(spark, target_path)
    merge_cond = """
        t._tenant_id = s._tenant_id
        AND t.fecha_proceso = s.fecha_proceso
        AND t.transporte = s.transporte
        AND t.ruta = s.ruta
        AND t.material = s.material
        AND t.tipo_entrega = s.tipo_entrega
    """
    (
        delta.alias("t")
        .merge(payload.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarantine(df: DataFrame, path: str, mode: str = "overwrite") -> int:
    count = df.count()
    if count == 0:
        return 0
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    return count


def run_silver(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    start = start_date or str(cfg.execution.start_date)
    end = end_date or str(cfg.execution.end_date)
    start_key = start.replace("-", "")
    end_key = end.replace("-", "")

    dim = upsert_dim_materials(spark, cfg, tenant, batch_id)

    bronze = (
        spark.read.format("delta")
        .load(bronze_table(cfg, tenant))
        .filter(
            (F.col("_tenant_id") == tenant)
            & (F.col("fecha_proceso") >= start_key)
            & (F.col("fecha_proceso") <= end_key)
        )
    )

    valid_types = list(cfg.business.valid_delivery_types)
    clean, quarantine_pre, discarded = apply_anomaly_rules(bronze, dim, valid_types)

    # Enrich after unit/flag prep needs SCD attrs on clean set
    factor = int(cfg.business.cs_to_st_factor)
    routine = list(cfg.business.routine_types)
    bonus = list(cfg.business.bonus_types)
    staged = classify_delivery_flags(normalize_units(clean, factor), routine, bonus)
    enriched, unmatched_scd = enrich_with_scd(staged, dim)

    facts = build_fact_payload(enriched, cfg, tenant, batch_id)
    merge_fact_deliveries(spark, cfg, tenant, facts)

    q_all = quarantine_pre.unionByName(unmatched_scd, allowMissingColumns=True)
    q_count = write_quarantine(q_all, silver_quarantine(cfg, tenant), mode="overwrite")

    return {
        "fact_rows": facts.count(),
        "quarantine_rows": q_count,
        "discarded_invalid_tipo": discarded,
        "dim_rows": dim.count(),
    }


def _delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


# Pure helpers exported for unit tests without Spark where possible
def cs_to_st(cantidad: float, unidad: str, factor: int = 20) -> float:
    if str(unidad).upper() == "CS":
        return float(cantidad) * factor
    return float(cantidad)


def is_valid_delivery_type(tipo: str, valid_types: list[str] | None = None) -> bool:
    valid = valid_types or ["ZPRE", "ZVE1", "Z04", "Z05"]
    return tipo in valid
