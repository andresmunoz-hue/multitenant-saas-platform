"""Unit tests for silver transform helpers and SCD temporal join."""


import pytest
from pyspark.sql import functions as F

from saas_pipeline.silver import (
    apply_anomaly_rules,
    cs_to_st,
    enrich_with_scd,
    is_valid_delivery_type,
    normalize_units,
)
from saas_pipeline.spark import build_spark


@pytest.fixture(scope="module")
def spark():
    session = build_spark("tests-silver")
    yield session
    session.stop()


def test_cs_to_st_conversion():
    assert cs_to_st(2, "CS", 20) == 40
    assert cs_to_st(5, "ST", 20) == 5
    assert cs_to_st(1.5, "cs", 20) == 30.0


def test_normalize_units_dataframe(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("units")
    csv_path = tmp / "units.csv"
    csv_path.write_text("cantidad,unidad\n2.0,CS\n3.0,ST\n", encoding="utf-8")
    df = (
        spark.read.option("header", True)
        .csv(str(csv_path))
        .withColumn("cantidad", F.col("cantidad").cast("double"))
    )
    out = normalize_units(df, 20).collect()
    by_unit = {r.unidad: r.cantidad_normalizada_st for r in out}
    assert float(by_unit["CS"]) == 40.0
    assert float(by_unit["ST"]) == 3.0


def test_filter_invalid_delivery_types():
    assert is_valid_delivery_type("ZPRE")
    assert is_valid_delivery_type("Z04")
    assert not is_valid_delivery_type("COBR")
    assert not is_valid_delivery_type("Z99")


def test_anomaly_quarantine_and_discard(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("anom")
    bronze_csv = tmp / "bronze.csv"
    bronze_csv.write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n"
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
    clean, quarantine, discarded = apply_anomaly_rules(
        bronze, dim, ["ZPRE", "ZVE1", "Z04", "Z05"]
    )
    assert clean.count() == 1
    assert quarantine.count() == 2
    assert discarded == 1


def test_temporal_scd_join_uses_historical_version(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("scd")
    fact_csv = tmp / "fact.csv"
    fact_csv.write_text(
        "material,fecha_proceso,cantidad,unidad\n"
        "AA004003,20250215,1.0,ST\n"
        "AA004003,20250501,1.0,ST\n",
        encoding="utf-8",
    )
    dim_csv = tmp / "dim.csv"
    dim_csv.write_text(
        "material,descripcion,categoria,precio_base,valid_from,valid_to,is_current\n"
        "AA004003,Cola old,BEBIDAS_GASEOSAS,31.95,2024-01-01,2025-03-31,false\n"
        "AA004003,Cola new,BEBIDAS_GASEOSAS,33.80,2025-04-01,9999-12-31,true\n",
        encoding="utf-8",
    )

    facts = (
        spark.read.option("header", True)
        .csv(str(fact_csv))
        .withColumn("cantidad", F.col("cantidad").cast("double"))
    )
    dim = (
        spark.read.option("header", True)
        .csv(str(dim_csv))
        .withColumn("precio_base", F.col("precio_base").cast("double"))
        .withColumn("valid_from", F.to_date("valid_from"))
        .withColumn("valid_to", F.to_date("valid_to"))
        .withColumn("is_current", F.col("is_current").cast("boolean"))
    )
    matched, unmatched = enrich_with_scd(facts, dim)
    assert unmatched.count() == 0
    rows = {
        r.fecha_proceso: (r.material_descripcion, float(r.precio_base))
        for r in matched.collect()
    }
    assert rows["20250215"][0] == "Cola old"
    assert rows["20250215"][1] == pytest.approx(31.95)
    assert rows["20250501"][0] == "Cola new"
    assert rows["20250501"][1] == pytest.approx(33.80)
