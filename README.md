# SAAS Data Platform

Pipeline **multi-tenant** Bronze → Silver → Gold para entregas de producto.  
PySpark 3.5 + Delta 3.x (compatible con Databricks Runtime). Ejecutable en local / Docker.

**Licencia:** propietaria — *All Rights Reserved* ([`LICENSE`](LICENSE)).  
Código de portafolio / evaluación técnica: se puede **ver**, no reutilizar ni redistribuir sin autorización escrita.

## Arranque rápido

```bash
# 1) Tests (sin cloud)
python -m venv .venv && .venv\Scripts\activate   # o: source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
ruff check src tests mentoring
pytest -q

# 2) Pipeline + dashboard (Docker)
docker compose build
docker compose --profile init run --rm pipeline-init
docker compose up -d dashboard
# → http://localhost:8501
```

Sin Docker (pipeline nativo; en Windows puede fallar Delta por Hadoop — preferir Docker):

```bash
python -m saas_pipeline.cli --env dev --tenant sv \
  --start-date 2025-01-01 --end-date 2025-06-30 --layer all
```

## Qué hace el flujo

1. **Bronze** — ingesta CSV a Delta; partición `fecha_proceso` + tenant; fechas inválidas a cuarentena  
2. **Silver** — limpia anomalías, CS→ST (×20), SCD2 de materiales con **join temporal**, MERGE de hechos  
3. **Quality** — checks sobre Silver → `data/shared/quality_logs`  
4. **Gold** — métricas diarias por `tipo_entrega` (units, revenue, rutas, transportes)  
5. **Dashboard** — Streamlit lee Gold (y quality logs si existen)

Tenants de muestra: `sv`, `hn`, `ec`, `gt`, `jm`, `pe` (`pais` del CSV → `_tenant_id` minúscula).

## Estructura del repo

```text
config/           YAML jerárquico (base / env / tenants)
raw/              CSV de entrada (versionados)
src/saas_pipeline/
  domain/             reglas de negocio puras
  application/        use cases (B/S/G/quality/orquestación)
  infrastructure/     Spark, Delta, config, transforms
  interfaces/         CLI
dashboard/        Streamlit + Dockerfile
docker/pipeline/  imagen Spark/Delta
docs/             índice en docs/README.md
tests/            pytest
mentoring/        ejercicio de code review
data/             salida Delta (no versionar)
```

## Documentación

Ver **[docs/README.md](docs/README.md)** (arquitectura, modelo, stack Docker, onboarding, observaciones, infra).

## CI

GitHub Actions en `dev` / `main`: solo **ruff** + **pytest**.  
No corre el pipeline e2e en Actions (sin credenciales / entorno lakehouse compartido).

## Alcance consciente

| Incluido | No incluido |
|---|---|
| Medallion local + Docker | ADLS / Unity Catalog reales |
| DQ + quality_logs | Auto Loader / streaming |
| Dashboard Streamlit | Databricks Community embebido en Compose |
| Snippet Terraform | `terraform apply` contra cuenta real |
| Mentoría / CI lint+test | Smoke Spark en GitHub Actions |

## Demo (sustentación)

1. `pytest -q`  
2. `docker compose --profile init run --rm pipeline-init` (o CLI `--tenant sv`)  
3. Mostrar `data/bronze|silver|gold` + dashboard  
4. Explicar join temporal SCD2 (**no** solo `is_current`)

## Licencia

© 2026 Andrés Muñoz. **Todos los derechos reservados.**  
Ver [`LICENSE`](LICENSE): permiso limitado solo para **visualización** (evaluación / portafolio).  
Queda prohibido el uso, copia, modificación, redistribución o explotación comercial sin autorización escrita.
