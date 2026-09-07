"""Use case: build Silver dim/fact with SCD2 and anomaly handling."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.infrastructure.config.paths import (
    abs_path,
    bronze_table,
    silver_dim,
    silver_fact,
    silver_quarantine,
)
from saas_pipeline.infrastructure.spark.delta_io import delta_exists
from saas_pipeline.infrastructure.transforms.silver_transforms import (
    MATERIALS_SCHEMA,
    apply_anomaly_rules,
    build_fact_payload,
    classify_delivery_flags,
    enrich_with_scd,
    normalize_units,
)


def _upsert_dim_materials(
    spark: SparkSession, cfg: DictConfig, tenant: str, batch_id: str
) -> DataFrame:
    """Load materials catalog into Silver dim (create or MERGE on material+valid_from)."""
    source = abs_path(str(cfg.paths.raw_materials))
    src = (
        spark.read.option("header", True)
        .schema(MATERIALS_SCHEMA)
        .csv(source)
        .withColumn("valid_from", F.to_date("valid_from", "yyyy-MM-dd"))
        .withColumn("valid_to", F.to_date("valid_to", "yyyy-MM-dd"))
        .withColumn("_tenant_id", F.lit(tenant))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_updated_at", F.lit(datetime.utcnow()).cast("timestamp"))
        .dropDuplicates(["material", "valid_from"])
    )

    target_path = silver_dim(cfg, tenant)
    if not delta_exists(spark, target_path):
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


def _merge_fact_deliveries(
    spark: SparkSession, cfg: DictConfig, tenant: str, facts: DataFrame
) -> None:
    """MERGE Silver facts on natural business key (tenant+fecha+transporte+ruta+material+tipo)."""
    target_path = silver_fact(cfg, tenant)
    payload = facts.drop("fecha_proceso_date")

    if not delta_exists(spark, target_path):
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


def _write_quarantine(df: DataFrame, path: str, mode: str = "overwrite") -> int:
    """Persist quarantine Delta table; return row count (0 skips write)."""
    count = df.count()
    if count == 0:
        return 0
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
    return count


def build_silver(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    batch_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    """Build Silver dim + fact for one tenant (anomalies, SCD2 join, MERGE).

    Returns counts for facts written, quarantine, discarded types, and dim rows.
    """
    start = start_date or str(cfg.execution.start_date)
    end = end_date or str(cfg.execution.end_date)
    start_key = start.replace("-", "")
    end_key = end.replace("-", "")

    dim = _upsert_dim_materials(spark, cfg, tenant, batch_id)

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

    factor = int(cfg.business.cs_to_st_factor)
    routine = list(cfg.business.routine_types)
    bonus = list(cfg.business.bonus_types)
    staged = classify_delivery_flags(normalize_units(clean, factor), routine, bonus)
    enriched, unmatched_scd = enrich_with_scd(staged, dim)

    facts = build_fact_payload(enriched, cfg, tenant, batch_id)
    _merge_fact_deliveries(spark, cfg, tenant, facts)

    q_all = quarantine_pre.unionByName(unmatched_scd, allowMissingColumns=True)
    q_count = _write_quarantine(q_all, silver_quarantine(cfg, tenant), mode="overwrite")

    return {
        "fact_rows": facts.count(),
        "quarantine_rows": q_count,
        "discarded_invalid_tipo": discarded,
        "dim_rows": dim.count(),
    }
