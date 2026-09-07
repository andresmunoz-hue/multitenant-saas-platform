"""CLI entrypoint for the multi-tenant medallion pipeline."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from saas_pipeline.bronze import run_bronze
from saas_pipeline.config import load_config, resolve_tenants
from saas_pipeline.gold import run_gold
from saas_pipeline.paths import new_batch_id, new_run_id
from saas_pipeline.quality import CriticalQualityError, run_quality
from saas_pipeline.silver import run_silver
from saas_pipeline.spark import build_spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SAAS multi-tenant Bronze/Silver/Gold pipeline",
    )
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "main"])
    parser.add_argument("--tenant", default="all", help="Tenant code or 'all'")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    parser.add_argument(
        "--layer",
        default="all",
        choices=["bronze", "silver", "gold", "all"],
        help="Layer to execute",
    )
    return parser


def run_for_tenant(
    spark,
    cfg,
    tenant: str,
    layer: str,
    run_id: str,
    batch_id: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    print(f"==> tenant={tenant} layer={layer} batch={batch_id}")
    if layer in ("bronze", "all"):
        stats = run_bronze(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    bronze: {stats}")
    if layer in ("silver", "all"):
        stats = run_silver(spark, cfg, tenant, batch_id, start_date, end_date)
        print(f"    silver: {stats}")
        qstats = run_quality(spark, cfg, tenant, run_id, batch_id, start_date, end_date)
        failed = [r["check_name"] for r in qstats if not r["check_passed"]]
        print(f"    quality: passed={len(qstats) - len(failed)} failed={failed or 'none'}")
    if layer in ("gold", "all"):
        stats = run_gold(spark, cfg, tenant, start_date, end_date)
        print(f"    gold: {stats}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    overrides: dict = {"execution": {"tenant": args.tenant.lower()}}
    if args.start_date:
        overrides["execution"]["start_date"] = args.start_date
    if args.end_date:
        overrides["execution"]["end_date"] = args.end_date

    cfg = load_config(env=args.env, tenant=args.tenant, overrides=overrides)
    tenants = resolve_tenants(cfg)
    run_id = new_run_id()
    batch_id = new_batch_id()

    spark = build_spark(str(cfg.spark.app_name), str(cfg.spark.master))
    failures: list[str] = []
    try:
        for tenant in tenants:
            try:
                run_for_tenant(
                    spark,
                    cfg,
                    tenant,
                    args.layer,
                    run_id,
                    batch_id,
                    args.start_date,
                    args.end_date,
                )
            except CriticalQualityError as exc:
                failures.append(f"{tenant}: {exc}")
                print(f"CRITICAL: {exc}", file=sys.stderr)
                if bool(cfg.execution.fail_fast):
                    break
            except Exception as exc:  # noqa: BLE001 - surface tenant errors in multi-run
                failures.append(f"{tenant}: {exc}")
                print(f"ERROR tenant={tenant}: {exc}", file=sys.stderr)
                if bool(cfg.execution.fail_fast):
                    break
    finally:
        spark.stop()

    if failures:
        print("Completed with failures:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    print(f"Done. run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
