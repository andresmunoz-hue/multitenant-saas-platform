"""Shared Delta helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession


def delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False
