# Stack local (Docker Compose)

## Local vs Databricks

| Componente | ¿En Compose? | Nota |
|---|---|---|
| Pipeline PySpark + Delta | Sí (`pipeline` / `pipeline-init`) | Mismo código que en Databricks Runtime |
| Dashboard Streamlit (Gold) | Sí (`dashboard`) | Lee `./data/gold` |
| Databricks Community Edition | **No** | SaaS en la nube; no es un contenedor local |
| Unity Catalog / ADLS Gen2 | No | Se simula con rutas bajo `./data` |

Para Community: subir notebook y usar `run_pipeline(..., spark=spark)` apuntando a DBFS. En local validamos con el contenedor Spark.

## Servicios

| Servicio | Perfil | Descripción |
|---|---|---|
| `dashboard` | (default) | UI en http://localhost:8501 |
| `pipeline` | `batch` | Ejecución on-demand del CLI |
| `pipeline-init` | `init` | Backfill one-shot de todos los tenants → `./data` |
| `pipeline-smoke` | `smoke` | Smoke MVP: tenant `sv`, 2025-03-01..07 |

Queries de demo (archivo **separado**):

```bash
# Requiere data ya materializada (smoke o init)
docker compose -f docker-compose.queries.yml run --rm queries
# otro tenant:
docker compose -f docker-compose.queries.yml run --rm -e QUERY_TENANT=hn queries
```

SQL de referencia: `scripts/queries/demo_queries.sql` · runner: `scripts/queries/run_demo_queries.py`.

## Uso

```bash
docker compose build

# Smoke rápido (1 tenant)
docker compose --profile smoke run --rm pipeline-smoke

# Materializar capas (todos los tenants)
docker compose --profile init run --rm pipeline-init

# UI (Gold / Quality / Quarantine)
docker compose up -d dashboard
```

Smoke con re-run idempotente:

```bash
bash scripts/smoke_e2e.sh
# Windows: pwsh scripts/smoke_e2e.ps1
```

Reproceso puntual:

```bash
docker compose --profile batch run --rm pipeline \
  python3 -m saas_pipeline.cli --env dev --tenant sv \
  --start-date 2025-03-01 --end-date 2025-03-15 --layer all
```

Segundo CSV de muestra (`batch2`, julio 2025, Delta bajo `data/*_batch2`):

```bash
docker compose --profile batch run --rm pipeline \
  python3 -m saas_pipeline.cli --env batch2 --tenant all \
  --start-date 2025-07-01 --end-date 2025-07-31 --layer all
```

## Imágenes

| Imagen | Dockerfile |
|---|---|
| Pipeline | `docker/pipeline/Dockerfile` (también `Dockerfile` en la raíz) |
| Dashboard | `dashboard/Dockerfile` |

Volumen compartido: host `./data` ↔ pipeline `/app/data` y dashboard `/data`.
