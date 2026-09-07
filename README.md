# SAAS Data Platform — Multi-tenant Medallion Pipeline

Pipeline Bronze → Silver → Gold para entregas de producto, multi-tenant (país = tenant), ejecutable en **Spark local** y compatible con Databricks Runtime 15.x (PySpark 3.5 + Delta 3.x).

## Estructura

```text
config/                      # YAML base / env / tenants
raw/                         # CSV de entrada
src/saas_pipeline/
  domain/                    # reglas puras de negocio
  application/use_cases/     # orquestación Bronze/Silver/Gold/Quality
  infrastructure/            # Spark, Delta, config, transforms
  interfaces/                # CLI
docs/                        # modelo, plan, architecture, observations
tests/
mentoring/
data/                        # Delta local (generado)
```

Detalle de capas: `docs/architecture.md`.

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

## Dashboard + Docker stack

Ver `docs/stack.md`.

```bash
docker compose build
docker compose --profile init run --rm pipeline-init
docker compose up -d dashboard
# http://localhost:8501
```

Local Streamlit (sin Docker), con Gold ya materializado:

```bash
pip install -r dashboard/requirements.txt
set DASHBOARD_DATA_ROOT=data
streamlit run dashboard/app.py
```

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
