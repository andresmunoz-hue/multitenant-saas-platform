# Estado de entrega — MVP

Pipeline Medallion multi-tenant (**Bronze → Silver → Gold**) con Clean Architecture, DQ, CI (ruff + pytest), mentoría, dashboard Streamlit y stack Docker local.

## Incluido

| Área | Entrega |
|---|---|
| Config | OmegaConf `base` / `env` / `tenants` |
| Bronze | CSV → Delta, overwrite por partición, cuarentena de fechas |
| Silver | SCD2 + join temporal, CS→ST, anomalías, MERGE `fact_deliveries` |
| Quality | ≥3 checks → `quality_logs`; `fail_on_critical` |
| Gold | `daily_metrics_by_delivery_type` |
| CI | GitHub Actions: **ruff** + **pytest** (sin smoke cloud) |
| Mentoría | `mentoring/bad_code.py` → review → `good_code.py` |
| Dashboard | Streamlit sobre Gold (`dashboard/`) |
| Runtime local | `docker-compose.yml` (pipeline Spark/Delta + UI) |

## Fuera de alcance (a propósito)

- Credenciales Databricks / ADLS / Unity Catalog reales
- Auto Loader / streaming
- Terraform aplicado contra cuenta cloud (solo snippet en `infra.md`)
- Databricks Community **dentro** de Compose (es SaaS; el código es compatible)

## Cómo validar

```bash
# Calidad de código
ruff check src tests mentoring
pytest -q

# Stack local
docker compose build
docker compose --profile init run --rm pipeline-init
docker compose up -d dashboard   # http://localhost:8501
```

Detalle operativo: [stack.md](stack.md) · modelo: [modelo_inicial.md](modelo_inicial.md).
