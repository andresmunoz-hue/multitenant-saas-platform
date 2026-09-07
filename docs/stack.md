# Local Docker stack

## What runs locally vs Databricks

| Piece | In this Compose? | Notes |
|---|---|---|
| PySpark + Delta Medallion pipeline | Yes (`pipeline` / `pipeline-init`) | Same code you’d run on Databricks Runtime |
| Streamlit Gold dashboard | Yes (`dashboard`) | Reads `./data/gold` via Delta |
| Databricks Community Edition | **No** | Cloud SaaS; not a local container |
| Unity Catalog / ADLS Gen2 | No | Simulated with local paths under `./data` |

Community Edition stays in the cloud: upload notebooks / point paths to DBFS. Locally we validate with Spark containers.

## Services

- **dashboard** — UI on http://localhost:8501
- **pipeline** (profile `batch`) — on-demand pipeline run
- **pipeline-init** (profile `init`) — one-shot full tenant backfill into `./data`

## Quick start

```bash
# 1) Build images
docker compose build

# 2) Materialize Bronze/Silver/Gold (all tenants)
docker compose --profile init run --rm pipeline-init

# 3) Start dashboard
docker compose up -d dashboard
```

Open http://localhost:8501

Re-run pipeline later:

```bash
docker compose --profile batch run --rm pipeline \
  python3 -m saas_pipeline.cli --env dev --tenant sv \
  --start-date 2025-03-01 --end-date 2025-03-15 --layer all
```

## Dockerfiles

| Image | Dockerfile |
|---|---|
| Pipeline (Spark/Delta) | `docker/pipeline/Dockerfile` |
| Dashboard (Streamlit) | `dashboard/Dockerfile` |
| Legacy root image | `Dockerfile` (same pipeline recipe) |

Shared volume: host `./data` ↔ pipeline `/app/data` and dashboard `/data`.
