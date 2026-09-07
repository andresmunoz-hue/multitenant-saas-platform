"""Use case: orchestrate Medallion pipeline for one or many tenants."""

from __future__ import annotations

import sys
from typing import Any

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
) -> dict[str, Any]:
    """Execute selected Medallion layers for one tenant; return a layer stats dict."""
    print(f"==> tenant={tenant} layer={layer} batch={batch_id}")
    summary: dict[str, Any] = {"tenant": tenant, "ok": True}
    if layer in ("bronze", "all"):
        stats = ingest_bronze(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    bronze: {stats}")
        summary["bronze"] = stats
    if layer in ("silver", "all"):
        stats = build_silver(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    silver: {stats}")
        summary["silver"] = stats
        qstats = run_quality_checks(
            spark, cfg, tenant, run_id, batch_id, start_date, end_date
        )
        failed = [r["check_name"] for r in qstats if not r["check_passed"]]
        print(f"    quality: passed={len(qstats) - len(failed)} failed={failed or 'none'}")
        summary["quality"] = {
            "checks": len(qstats),
            "failed": failed,
            "passed": len(qstats) - len(failed),
        }
    if layer in ("gold", "all"):
        stats = build_gold(spark, cfg, tenant, start_date, end_date)
        print(f"    gold: {stats}")
        summary["gold"] = stats
    return summary


def _print_batch_summary(
    run_id: str,
    batch_id: str,
    layer: str,
    tenant_summaries: list[dict[str, Any]],
    failures: list[str],
) -> None:
    """Print aggregated counters across tenants (demo / ops observability)."""
    bronze_written = sum(s.get("bronze", {}).get("rows_written", 0) for s in tenant_summaries)
    bronze_q = sum(
        s.get("bronze", {}).get("rows_quarantined_invalid_fecha", 0) for s in tenant_summaries
    )
    silver_facts = sum(s.get("silver", {}).get("fact_rows", 0) for s in tenant_summaries)
    silver_q = sum(s.get("silver", {}).get("quarantine_rows", 0) for s in tenant_summaries)
    discarded = sum(
        s.get("silver", {}).get("discarded_invalid_tipo", 0) for s in tenant_summaries
    )
    gold_rows = sum(s.get("gold", {}).get("metric_rows", 0) for s in tenant_summaries)
    quality_failed = sum(len(s.get("quality", {}).get("failed", [])) for s in tenant_summaries)

    print("========== BATCH SUMMARY ==========")
    print(f"run_id={run_id}")
    print(f"batch_id={batch_id}")
    print(f"layer={layer}")
    print(f"tenants_ok={len(tenant_summaries)} failures={len(failures)}")
    print(f"bronze_rows_written={bronze_written}")
    print(f"bronze_quarantine_fecha={bronze_q}")
    print(f"silver_fact_rows={silver_facts}")
    print(f"silver_quarantine_rows={silver_q}")
    print(f"silver_discarded_tipo={discarded}")
    print(f"quality_checks_failed={quality_failed}")
    print(f"gold_metric_rows={gold_rows}")
    print("===================================")


def run_pipeline(
    env: str = "dev",
    tenant: str = "all",
    layer: str = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
    raw_deliveries: str | None = None,
    raw_materials: str | None = None,
) -> int:
    """Application entry used by CLI and notebooks (Databricks can pass existing spark).

    Optional ``raw_deliveries`` / ``raw_materials`` override ``cfg.paths`` without a new env.
    Prints a BATCH SUMMARY and returns ``0`` on success, ``1`` if any tenant failed.
    """
    overrides: dict[str, Any] = {"execution": {"tenant": tenant.lower()}}
    if start_date:
        overrides["execution"]["start_date"] = start_date
    if end_date:
        overrides["execution"]["end_date"] = end_date
    path_overrides: dict[str, str] = {}
    if raw_deliveries:
        path_overrides["raw_deliveries"] = raw_deliveries
    if raw_materials:
        path_overrides["raw_materials"] = raw_materials
    if path_overrides:
        overrides["paths"] = path_overrides

    cfg = load_config(env=env, tenant=tenant, overrides=overrides)
    tenants = resolve_tenants(cfg)
    run_id = new_run_id()
    batch_id = new_batch_id()

    print(
        f"Pipeline start env={env} tenants={tenants} "
        f"dates={start_date or cfg.execution.start_date}..{end_date or cfg.execution.end_date} "
        f"raw_deliveries={cfg.paths.raw_deliveries}"
    )

    owns_spark = spark is None
    if spark is None:
        spark = build_spark(str(cfg.spark.app_name), str(cfg.spark.master))

    failures: list[str] = []
    tenant_summaries: list[dict[str, Any]] = []
    try:
        for tenant_id in tenants:
            try:
                summary = run_for_tenant(
                    spark,
                    cfg,
                    tenant_id,
                    layer,
                    run_id,
                    batch_id,
                    start_date,
                    end_date,
                )
                tenant_summaries.append(summary)
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

    _print_batch_summary(run_id, batch_id, layer, tenant_summaries, failures)

    if failures:
        print("Completed with failures:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    print(f"Done. run_id={run_id}")
    return 0
