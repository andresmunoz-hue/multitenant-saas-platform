"""Use case: orchestrate Medallion pipeline for one or many tenants."""

from __future__ import annotations

import sys

from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline.application.use_cases.build_gold import build_gold
from saas_pipeline.application.use_cases.build_silver import build_silver
from saas_pipeline.application.use_cases.ingest_bronze import ingest_bronze
from saas_pipeline.application.use_cases.run_quality_checks import run_quality_checks
from saas_pipeline.domain.exceptions import CriticalQualityError
from saas_pipeline.infrastructure.config.loader import load_config, resolve_tenants
from saas_pipeline.infrastructure.config.paths import new_batch_id, new_run_id
from saas_pipeline.infrastructure.spark.session import build_spark


def run_for_tenant(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    layer: str,
    run_id: str,
    batch_id: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    print(f"==> tenant={tenant} layer={layer} batch={batch_id}")
    if layer in ("bronze", "all"):
        stats = ingest_bronze(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    bronze: {stats}")
    if layer in ("silver", "all"):
        stats = build_silver(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    silver: {stats}")
        qstats = run_quality_checks(
            spark, cfg, tenant, run_id, batch_id, start_date, end_date
        )
        failed = [r["check_name"] for r in qstats if not r["check_passed"]]
        print(f"    quality: passed={len(qstats) - len(failed)} failed={failed or 'none'}")
    if layer in ("gold", "all"):
        stats = build_gold(spark, cfg, tenant, start_date, end_date)
        print(f"    gold: {stats}")


def run_pipeline(
    env: str = "dev",
    tenant: str = "all",
    layer: str = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
) -> int:
    """Application entry used by CLI and notebooks (Databricks can pass existing spark)."""
    overrides: dict = {"execution": {"tenant": tenant.lower()}}
    if start_date:
        overrides["execution"]["start_date"] = start_date
    if end_date:
        overrides["execution"]["end_date"] = end_date

    cfg = load_config(env=env, tenant=tenant, overrides=overrides)
    tenants = resolve_tenants(cfg)
    run_id = new_run_id()
    batch_id = new_batch_id()

    owns_spark = spark is None
    if spark is None:
        spark = build_spark(str(cfg.spark.app_name), str(cfg.spark.master))

    failures: list[str] = []
    try:
        for tenant_id in tenants:
            try:
                run_for_tenant(
                    spark,
                    cfg,
                    tenant_id,
                    layer,
                    run_id,
                    batch_id,
                    start_date,
                    end_date,
                )
            except CriticalQualityError as exc:
                failures.append(f"{tenant_id}: {exc}")
                print(f"CRITICAL: {exc}", file=sys.stderr)
                if bool(cfg.execution.fail_fast):
                    break
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{tenant_id}: {exc}")
                print(f"ERROR tenant={tenant_id}: {exc}", file=sys.stderr)
                if bool(cfg.execution.fail_fast):
                    break
    finally:
        if owns_spark:
            spark.stop()

    if failures:
        print("Completed with failures:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    print(f"Done. run_id={run_id}")
    return 0
