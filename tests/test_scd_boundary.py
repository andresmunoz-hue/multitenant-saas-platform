"""SCD2 boundary and anomaly determinism tests.

Locks inclusive ``[valid_from, valid_to]`` join behavior, gap → quarantine,
and deterministic anomaly counts. See ``docs/functions_and_tests.md``.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from saas_pipeline.silver import apply_anomaly_rules, enrich_with_scd
from saas_pipeline.spark import build_spark


@pytest.fixture(scope="module")
def spark():
    """Shared local Spark session for SCD boundary tests."""
    session = build_spark("tests-scd-boundary")
    yield session
    session.stop()


def _dim(spark, rows_csv: str, tmp):
    """Build a typed materials dim DataFrame from inline CSV text."""
    path = tmp / "dim.csv"
    path.write_text(rows_csv, encoding="utf-8")
    return (
        spark.read.option("header", True)
        .csv(str(path))
        .withColumn("precio_base", F.col("precio_base").cast("double"))
        .withColumn("valid_from", F.to_date("valid_from"))
        .withColumn("valid_to", F.to_date("valid_to"))
        .withColumn("is_current", F.col("is_current").cast("boolean"))
    )


def _facts(spark, rows_csv: str, tmp):
    """Build a typed fact DataFrame from inline CSV text."""
    path = tmp / "fact.csv"
    path.write_text(rows_csv, encoding="utf-8")
    return (
        spark.read.option("header", True)
        .csv(str(path))
        .withColumn("cantidad", F.col("cantidad").cast("double"))
    )


def test_scd_inclusive_on_valid_from_and_valid_to(spark, tmp_path_factory):
    """fecha_proceso on both endpoints of [valid_from, valid_to] must match."""
    tmp = tmp_path_factory.mktemp("scd-bounds")
    facts = _facts(
        spark,
        "material,fecha_proceso,cantidad,unidad\n"
        "AA004003,20250331,1.0,ST\n"
        "AA004003,20250401,1.0,ST\n",
        tmp,
    )
    dim = _dim(
        spark,
        "material,descripcion,categoria,precio_base,valid_from,valid_to,is_current\n"
        "AA004003,Cola old,BEBIDAS_GASEOSAS,31.95,2024-01-01,2025-03-31,false\n"
        "AA004003,Cola new,BEBIDAS_GASEOSAS,33.80,2025-04-01,9999-12-31,true\n",
        tmp,
    )
    matched, unmatched = enrich_with_scd(facts, dim)
    assert unmatched.count() == 0
    rows = {r.fecha_proceso: r.material_descripcion for r in matched.collect()}
    assert rows["20250331"] == "Cola old"
    assert rows["20250401"] == "Cola new"


def test_scd_day_after_valid_to_is_unmatched_when_gap(spark, tmp_path_factory):
    """If versions leave a gap, fecha outside both windows goes to quarantine."""
    tmp = tmp_path_factory.mktemp("scd-gap")
    facts = _facts(
        spark,
        "material,fecha_proceso,cantidad,unidad\n"
        "AA004003,20250415,1.0,ST\n",
        tmp,
    )
    dim = _dim(
        spark,
        "material,descripcion,categoria,precio_base,valid_from,valid_to,is_current\n"
        "AA004003,Cola old,BEBIDAS_GASEOSAS,31.95,2024-01-01,2025-03-31,false\n"
        "AA004003,Cola new,BEBIDAS_GASEOSAS,33.80,2025-05-01,9999-12-31,true\n",
        tmp,
    )
    matched, unmatched = enrich_with_scd(facts, dim)
    assert matched.count() == 0
    assert unmatched.count() == 1
    assert unmatched.collect()[0]._quarantine_reason == "no_scd_version_for_fecha"


def test_anomaly_rules_are_deterministic(spark, tmp_path_factory):
    """Same input twice → same clean / quarantine / discard counts (idempotent transform)."""
    tmp = tmp_path_factory.mktemp("anom-idem")
    bronze_csv = tmp / "bronze.csv"
    bronze_csv.write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n"
        "SV,20250301,1,1,ZPRE,AA004003,10.0,2.0,CS\n"
        "SV,20250301,1,1,ZPRE,AA004003,10.0,2.0,CS\n"
        "SV,20250301,2,2,ZPRE,XX999999,10.0,2.0,CS\n"
        "SV,20250301,3,3,COBR,AA004003,10.0,2.0,CS\n"
        "SV,20250301,4,4,ZVE1,AA004003,10.0,0.0,ST\n",
        encoding="utf-8",
    )
    dim_csv = tmp / "dim.csv"
    dim_csv.write_text("material\nAA004003\n", encoding="utf-8")

    bronze = (
        spark.read.option("header", True)
        .csv(str(bronze_csv))
        .withColumn("transporte", F.col("transporte").cast("long"))
        .withColumn("ruta", F.col("ruta").cast("long"))
        .withColumn("precio", F.col("precio").cast("double"))
        .withColumn("cantidad", F.col("cantidad").cast("double"))
    )
    dim = spark.read.option("header", True).csv(str(dim_csv))
    types = ["ZPRE", "ZVE1", "Z04", "Z05"]

    a = apply_anomaly_rules(bronze, dim, types)
    b = apply_anomaly_rules(bronze, dim, types)
    assert a[0].count() == b[0].count() == 1
    assert a[1].count() == b[1].count() == 2
    assert a[2] == b[2] == 1
