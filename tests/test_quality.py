"""Quality check unit tests (file-backed Spark frames for Windows compatibility)."""

from pathlib import Path

from pyspark.sql import functions as F

from saas_pipeline.quality import (
    check_delivery_type_domain,
    check_fk_material_resolved,
    check_qty_st_positive,
)
from saas_pipeline.spark import build_spark


def test_quality_checks_detect_failures(tmp_path: Path):
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
