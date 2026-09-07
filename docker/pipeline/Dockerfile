FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv curl \
    && rm -rf /var/lib/apt/lists/*

# Pre-download Delta jars (offline Spark/Delta — Databricks-compatible runtime path)
RUN mkdir -p /opt/delta-jars \
    && curl -fsSL -o /opt/delta-jars/delta-spark_2.12-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL -o /opt/delta-jars/delta-storage-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    && curl -fsSL -o /opt/delta-jars/antlr4-runtime-4.9.3.jar \
      https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

ENV DELTA_JARS_DIR=/opt/delta-jars
ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app
COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY config ./config
COPY raw ./raw
COPY tests ./tests
COPY mentoring ./mentoring

RUN pip3 install --no-cache-dir -U pip setuptools wheel \
    && pip3 install --no-cache-dir -r requirements.txt \
    && pip3 install --no-cache-dir .

# Default: process all tenants for the sample window
CMD ["python3", "-m", "saas_pipeline.cli", \
     "--env", "dev", "--tenant", "all", \
     "--start-date", "2025-01-01", "--end-date", "2025-06-30", \
     "--layer", "all"]
