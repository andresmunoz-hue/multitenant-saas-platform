# Clean Architecture

El pipeline sigue Clean Architecture adaptada a un lakehouse Spark/Delta.

```text
interfaces/          CLI (y futuros notebooks Databricks)
      │
application/         use cases: ingest_bronze, build_silver, run_quality, build_gold, run_pipeline
      │
domain/              reglas puras (cs_to_st, delivery types) + excepciones + constantes
      │
infrastructure/      OmegaConf, paths, Spark session, transforms DataFrame, Delta I/O
```

## Reglas de dependencia

- `domain` no importa Spark ni OmegaConf.
- `application` orquesta y puede usar infrastructure (adaptadores).
- `interfaces` solo habla con application.
- Los módulos raíz (`bronze.py`, `silver.py`, …) son **shims** de compatibilidad para imports/tests existentes.

## Databricks

En Community/workspace, un notebook puede hacer:

```python
from saas_pipeline.application.use_cases.run_pipeline import run_pipeline
run_pipeline(env="dev", tenant="sv", layer="all", spark=spark)
```

pasando el `spark` de la sesión (sin crear uno local).
