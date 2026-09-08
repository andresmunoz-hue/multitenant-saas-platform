"""Quality check unit tests (file-backed Spark frames for Windows compatibility).

Asserts critical/warning severity and failure detection for qty, delivery type, and FK.
See ``docs/functions_and_tests.md``.
"""

from pathlib import Path

from pyspark.sql import functions as F

from saas_pipeline.infrastructure.spark.session import build_spark
from saas_pipeline.infrastructure.transforms.quality_checks import (
    check_delivery_type_domain,
    check_fk_material_resolved,
    check_qty_st_positive,
)


def test_quality_checks_detect_failures(tmp_path: Path):
    """One good row + one bad row: qty, domain, and FK checks must fail."""
    csv_path = tmp_path / "fact.csv"
    csv_path.write_text(
        "cantidad_normalizada_st,tipo_entrega,material_descripcion,material_categoria\n"
        "10.0,ZPRE,ok,CAT\n"
        "-1.0,Z99,,\n",
        encoding="utf-8",
    )

    spark = build_spark("tests-quality")
    try:
        df = (
            spark.read.option("header", True)
            .option("nullValue", "")
            .csv(str(csv_path))
            .withColumn(
                "cantidad_normalizada_st",
                F.col("cantidad_normalizada_st").cast("double"),
            )
        )
        qty = check_qty_st_positive(df, "r1", "b1", "sv")
        domain = check_delivery_type_domain(df, "r1", "b1", "sv")
        fk = check_fk_material_resolved(df, "r1", "b1", "sv")

        assert qty["check_passed"] is False
        assert qty["records_failed"] == 1
        assert qty["check_severity"] == "critical"

        assert domain["check_passed"] is False
        assert domain["check_severity"] == "warning"

        assert fk["check_passed"] is False
        assert fk["check_severity"] == "critical"
    finally:
        spark.stop()
