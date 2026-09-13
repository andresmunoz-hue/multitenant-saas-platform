# Clean Architecture

Adaptación a un lakehouse Spark/Delta (sin over-abstracting el DataFrame).

```text
interfaces/cli.py
        │
application/use_cases/
  ingest_bronze · build_silver · run_quality_checks · build_gold · run_pipeline
        │
domain/
  constants · rules (cs_to_st, delivery types) · exceptions
        │
infrastructure/
  config (OmegaConf, paths) · spark · transforms · Delta I/O
```

## Dependencias

| Capa | Puede depender de | No debe depender de |
|---|---|---|
| `domain` | stdlib | Spark, OmegaConf, IO |
| `application` | domain + infrastructure (adapters) | CLI |
| `infrastructure` | domain, Spark/Delta/YAML | interfaces |
| `interfaces` | application | detalles de Delta |

Entrypoint CLI: `python -m saas_pipeline` → `interfaces/cli.py` (vía `__main__.py`).

**Por qué / ventajas en este MVP:** ver [observations_adicionales.md §5](observations_adicionales.md).

## Databricks / notebook

```python
from saas_pipeline.application.use_cases.run_pipeline import run_pipeline

run_pipeline(env="dev", tenant="sv", layer="all", spark=spark)
```

Se reutiliza el `spark` de la sesión del cluster (no se crea sesión local).
