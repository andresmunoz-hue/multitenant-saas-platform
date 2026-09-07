#!/usr/bin/env python3
"""Demo / interview Spark SQL queries against local Medallion Delta tables.

Usage (Docker — preferred on Windows):
  docker compose -f docker-compose.queries.yml run --rm queries

Usage (native, if Spark/Delta works):
  python scripts/queries/run_demo_queries.py --tenant sv --data-root data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

QUERIES: list[tuple[str, str]] = [
    (
        "Gold — KPI by delivery type",
        """
        SELECT tipo_entrega,
               ROUND(SUM(total_units), 2) AS units_st,
               ROUND(SUM(total_revenue), 2) AS revenue,
               ROUND(AVG(active_routes), 2) AS avg_routes
        FROM gold
        GROUP BY tipo_entrega
        ORDER BY revenue DESC
        """,
    ),
    (
        "Gold — top days by revenue",
        """
        SELECT fecha_proceso,
               ROUND(SUM(total_revenue), 2) AS revenue,
               ROUND(SUM(total_units), 2) AS units_st
        FROM gold
        GROUP BY fecha_proceso
        ORDER BY revenue DESC
        LIMIT 10
        """,
    ),
    (
        "Silver — daily units / revenue by tipo",
        """
        SELECT fecha_proceso,
               tipo_entrega,
               ROUND(SUM(cantidad_normalizada_st), 2) AS units_st,
               ROUND(SUM(cantidad_normalizada_st * precio_transaccion), 2) AS revenue
        FROM silver_fact
        GROUP BY fecha_proceso, tipo_entrega
        ORDER BY fecha_proceso, tipo_entrega
        LIMIT 30
        """,
    ),
    (
        "Silver — SCD sample (historical material versions)",
        """
        SELECT fecha_proceso, material, material_descripcion, precio_base, tipo_entrega
        FROM silver_fact
        WHERE material IS NOT NULL
        ORDER BY material, fecha_proceso
        LIMIT 20
        """,
    ),
    (
        "Bronze — row counts by fecha",
        """
        SELECT fecha_proceso, COUNT(*) AS rows
        FROM bronze
        GROUP BY fecha_proceso
        ORDER BY fecha_proceso
        LIMIT 20
        """,
    ),
    (
        "Bronze quarantine — reasons",
        """
        SELECT _quarantine_reason, COUNT(*) AS rows
        FROM bronze_q
        GROUP BY _quarantine_reason
        ORDER BY rows DESC
        """,
    ),
    (
        "Silver quarantine — reasons",
        """
        SELECT _quarantine_reason, COUNT(*) AS rows
        FROM silver_q
        GROUP BY _quarantine_reason
        ORDER BY rows DESC
        """,
    ),
    (
        "Quality logs — recent checks",
        """
        SELECT check_name, check_severity, check_passed, records_failed, executed_at
        FROM quality
        ORDER BY executed_at DESC
        LIMIT 20
        """,
    ),
]


def _register(spark: SparkSession, path: Path, view: str) -> bool:
    if not path.exists():
        print(f"  [skip] missing {path} → view `{view}`")
        return False
    spark.read.format("delta").load(str(path)).createOrReplaceTempView(view)
    print(f"  [ok]   {view} ← {path}")
    return True


def run(data_root: Path, tenant: str) -> int:
    from saas_pipeline.spark import build_spark

    spark = build_spark("demo-queries")
    print(f"data_root={data_root} tenant={tenant}")
    registered = 0
    registered += _register(
        spark, data_root / "bronze" / tenant / "deliveries", "bronze"
    )
    registered += _register(
        spark, data_root / "silver" / tenant / "fact_deliveries", "silver_fact"
    )
    registered += _register(
        spark, data_root / "silver" / tenant / "dim_materials", "silver_dim"
    )
    registered += _register(
        spark,
        data_root / "gold" / tenant / "daily_metrics_by_delivery_type",
        "gold",
    )
    registered += _register(
        spark,
        data_root / "bronze_quarantine" / tenant / "deliveries",
        "bronze_q",
    )
    registered += _register(
        spark,
        data_root / "silver_quarantine" / tenant / "fact_deliveries",
        "silver_q",
    )
    registered += _register(spark, data_root / "shared" / "quality_logs", "quality")

    if registered == 0:
        print(
            "No Delta tables found. Run smoke/init first:\n"
            "  docker compose --profile smoke run --rm pipeline-smoke",
            file=sys.stderr,
        )
        spark.stop()
        return 1

    # Only run queries whose primary view exists
    available = {t.name for t in spark.catalog.listTables()}
    exit_code = 0
    for title, sql in QUERIES:
        needed = {
            tok
            for tok in (
                "gold",
                "silver_fact",
                "bronze",
                "bronze_q",
                "silver_q",
                "quality",
            )
            if tok in sql
        }
        if not needed.issubset(available):
            missing = ", ".join(sorted(needed - available))
            print(f"\n==> {title}\n  [skip] missing views: {missing}")
            continue
        print(f"\n==> {title}")
        try:
            spark.sql(sql).show(truncate=False)
        except Exception as exc:  # noqa: BLE001
            exit_code = 1
            print(f"  ERROR: {exc}", file=sys.stderr)

    spark.stop()
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run demo Medallion SQL queries")
    parser.add_argument(
        "--tenant",
        default=None,
        help="Tenant id (default: env QUERY_TENANT or sv)",
    )
    parser.add_argument(
        "--data-root",
        default=None,
        help="Medallion root (default: env QUERY_DATA_ROOT or data)",
    )
    args = parser.parse_args(argv)
    import os

    tenant = (args.tenant or os.environ.get("QUERY_TENANT") or "sv").lower()
    data_root = args.data_root or os.environ.get("QUERY_DATA_ROOT") or "data"
    root = Path(data_root)
    if not root.is_absolute():
        root = Path.cwd() / root
    return run(root.resolve(), tenant)


if __name__ == "__main__":
    raise SystemExit(main())
