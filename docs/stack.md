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

## Uso

```bash
docker compose build

# Materializar capas
docker compose --profile init run --rm pipeline-init

# UI
docker compose up -d dashboard
```

Reproceso puntual:

```bash
docker compose --profile batch run --rm pipeline \
  python3 -m saas_pipeline.cli --env dev --tenant sv \
  --start-date 2025-03-01 --end-date 2025-03-15 --layer all
```

## Imágenes

| Imagen | Dockerfile |
|---|---|
| Pipeline | `docker/pipeline/Dockerfile` (también `Dockerfile` en la raíz) |
| Dashboard | `dashboard/Dockerfile` |

Volumen compartido: host `./data` ↔ pipeline `/app/data` y dashboard `/data`.
