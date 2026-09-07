# SAAS Data Platform — Multi-tenant Medallion Pipeline

Pipeline Bronze → Silver → Gold para entregas de producto, multi-tenant (país = tenant), ejecutable en **Spark local** y compatible con Databricks Runtime 15.x (PySpark 3.5 + Delta 3.x).

## Estructura

```text
config/                 # base + env + tenants (OmegaConf)
raw/                    # CSV de entrada (versionados)
src/saas_pipeline/      # bronze, silver, gold, quality, cli
tests/                  # pytest
mentoring/              # code review exercise
docs/                   # arquitectura, plan, observations, infra
data/                   # Delta local (generado, no versionar)
```

## Requisitos

- Python **3.11+** (probado con 3.12)
- Java 17 (Spark)
- Windows/macOS/Linux
- En **Windows**: ejecutar `powershell -File scripts/setup_winutils.ps1` (genera `.hadoop/bin/winutils.exe` no-op). El factory de Spark usa ese path como `HADOOP_HOME`.

Versiones pinneadas en `requirements.txt` / `pyproject.toml`:

- `pyspark==3.5.5`
- `delta-spark==3.2.1`
- `omegaconf==2.3.0`
## Setup

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
# source .venv/bin/activate

pip install -r requirements.txt
pip install -e .
```

Con Make (si está disponible):

```bash
make install
make test
make lint
```

## Ejecutar el pipeline

```bash
# Un tenant
python -m saas_pipeline.cli --env dev --tenant sv --start-date 2025-01-01 --end-date 2025-06-30 --layer all

# Todos los tenants
python -m saas_pipeline.cli --env dev --tenant all --start-date 2025-01-01 --end-date 2025-06-30 --layer all
```

### Windows + Docker (recomendado para e2e Delta)

En Windows nativo, Hadoop/Delta puede fallar por `winutils`/`hadoop.dll`. Usa el contenedor Linux:

```bash
docker build -t saas-pipeline:local .
docker run --rm -v "%cd%/data:/app/data" saas-pipeline:local
# o rango corto:
docker run --rm -v "%cd%/data:/app/data" saas-pipeline:local \
  python3 -m saas_pipeline.cli --env dev --tenant sv --start-date 2025-03-01 --end-date 2025-03-15 --layer all
```

Capas: `--layer bronze|silver|gold|all`.

Salidas locales:

- `data/bronze/<tenant>/deliveries/`
- `data/silver/<tenant>/fact_deliveries/`
- `data/silver/<tenant>/dim_materials/`
- `data/gold/<tenant>/daily_metrics_by_delivery_type/`
- `data/shared/quality_logs/`
- `data/*_quarantine/...`

## Tests y linter

```bash
ruff check src tests mentoring
pytest -q
```

## Onboarding de un tenant nuevo

Ver `docs/onboarding-tenant.md`. Resumen: agregar `config/tenants/<code>.yaml`, registrar el código en `KNOWN_TENANTS`, asegurar que el CSV traiga `pais=<CODE>`, correr el CLI.

## Qué dejé fuera y por qué

- **Credenciales / Databricks cloud / ADLS / Unity Catalog reales:** la prueba pide paths locales; IaC queda como snippet en `docs/infra.md`.
- **Auto Loader / streaming:** arquitectura lo marca como “provisto”; el MVP usa batch CSV para cerrar idempotencia y SCD2.
- **Segunda tabla Gold / dashboard / pre-commit / Terraform apply:** bonus; priorizamos MVP evaluable (pipeline 40% + DQ + CI + mentoría).
- **Copia del catálogo por tenant:** decisión pragmática documentada en `docs/observations.md`.

## Docs clave

- `docs/modelo_inicial.md` — modelo de datos
- `docs/plan_implementacion.md` — plan de ejecución
- `docs/observations.md` — disensos / ambigüedades / horizonte 2-3
- `docs/infra.md` — Terraform ilustrativo
- `mentoring/code_review.md` — revisión del junior

## Sustentación (checklist demo)

1. `pytest -q`
2. Pipeline `--tenant sv --layer all`
3. Mostrar partición Delta bronze + fact + gold + quality_logs
4. Explicar join temporal SCD2 (no `is_current`)
