"""CLI adapter for the Medallion pipeline."""

from __future__ import annotations

import argparse
from typing import Sequence

from saas_pipeline.application.use_cases.run_pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SAAS multi-tenant Bronze/Silver/Gold pipeline",
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "qa", "main", "batch2"],
        help="Environment config under config/env/ (batch2 = second sample CSV)",
    )
    parser.add_argument("--tenant", default="all", help="Tenant code or 'all'")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    parser.add_argument(
        "--layer",
        default="all",
        choices=["bronze", "silver", "gold", "all"],
        help="Layer to execute",
    )
    parser.add_argument(
        "--raw-deliveries",
        default=None,
        help="Override paths.raw_deliveries (e.g. raw/..._batch2.csv)",
    )
    parser.add_argument(
        "--raw-materials",
        default=None,
        help="Override paths.raw_materials",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_pipeline(
        env=args.env,
        tenant=args.tenant,
        layer=args.layer,
        start_date=args.start_date,
        end_date=args.end_date,
        raw_deliveries=args.raw_deliveries,
        raw_materials=args.raw_materials,
    )


if __name__ == "__main__":
    raise SystemExit(main())
