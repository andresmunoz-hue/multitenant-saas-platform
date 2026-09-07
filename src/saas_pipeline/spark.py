"""Local SparkSession factory with Delta Lake support."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from saas_pipeline.config import repo_root


def _ensure_hadoop_home() -> None:
    """On Windows, Spark needs winutils via HADOOP_HOME."""
    if os.name != "nt":
        return
    if os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"):
        return
    local = repo_root() / ".hadoop"
    if (local / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(local)
        os.environ["PATH"] = str(local / "bin") + os.pathsep + os.environ.get("PATH", "")


def _ensure_pyspark_python() -> None:
    python = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python)


def _delta_jars() -> list[str]:
    """Optional offline Delta jars (set DELTA_JARS=/path/a.jar,/path/b.jar)."""
    raw = os.environ.get("DELTA_JARS", "").strip()
    if not raw:
        default_dir = Path(os.environ.get("DELTA_JARS_DIR", "/opt/delta-jars"))
        if default_dir.is_dir():
            return sorted(str(p) for p in default_dir.glob("*.jar"))
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def build_spark(app_name: str = "saas-data-platform", master: str = "local[*]") -> SparkSession:
    """Build a Databricks-compatible local Spark session with Delta extensions."""
    _ensure_hadoop_home()
    _ensure_pyspark_python()

    if os.name == "nt" and master == "local[*]":
        master = "local[1]"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
    )

    jars = _delta_jars()
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
        spark = builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark
