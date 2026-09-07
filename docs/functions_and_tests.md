# Funciones y tests

Referencia del MVP: qué hace cada pieza pública y qué valida la suite `pytest`.

## Cómo correr tests

```bash
pytest -q
# o un módulo:
pytest -q tests/test_scd_boundary.py
```

Smoke e2e (Docker, no forma parte de CI): `scripts/smoke_e2e.sh` / `.ps1`.

---

## Domain (reglas puras)

| Símbolo | Módulo | Rol |
|---|---|---|
| `KNOWN_TENANTS`, `DEFAULT_*` | `domain/constants.py` | Tenants y dominios de negocio por defecto |
| `cs_to_st(cantidad, unidad, factor=20)` | `domain/rules/units.py` | Convierte CS→ST (× factor); ST se deja igual |
| `is_valid_delivery_type(tipo, valid_types?)` | `domain/rules/delivery_types.py` | Dominio ZPRE/ZVE1/Z04/Z05 (o lista inyectada) |
| `CriticalQualityError` | `domain/exceptions.py` | Fallo crítico de DQ cuando `fail_on_critical` |

## Application (use cases)

| Función | Entrada clave | Salida |
|---|---|---|
| `ingest_bronze(...)` | Spark, cfg, tenant, batch, fechas | `dict`: `rows_written`, `rows_quarantined_invalid_fecha`, `partitions` |
| `build_silver(...)` | idem | `dict`: `fact_rows`, `quarantine_rows`, `discarded_invalid_tipo`, `dim_rows` |
| `run_quality_checks(...)` | idem + run_id | `list[dict]` de checks; puede lanzar `CriticalQualityError` |
| `build_gold(...)` | Spark, cfg, tenant, fechas | `dict`: `metric_rows` |
| `run_for_tenant(...)` | orquestación por capa | `dict` resumen por tenant |
| `run_pipeline(...)` | env, tenant, layer, fechas, overrides CSV, spark opcional | exit code `0/1` + imprime **BATCH SUMMARY** |

Helpers internos Silver (no API pública): `_upsert_dim_materials`, `_merge_fact_deliveries`, `_write_quarantine`.

## Infrastructure

### Config

| Función | Rol |
|---|---|
| `load_config(env, tenant, overrides?)` | Merge `base` ← `env/<env>` ← `tenants/<t>` ← overrides |
| `resolve_tenants(cfg)` | `all` → lista `KNOWN_TENANTS`; si no, valida tenant |
| `abs_path`, `date_range_yyyymmdd`, `bronze_table`, `silver_*`, `gold_*`, `*_quarantine`, `quality_logs_path` | Rutas absolutas y ventanas de partición |
| `new_batch_id`, `new_run_id` | IDs de corrida |

### Transforms

| Función | Rol |
|---|---|
| `is_valid_fecha()` | Predicado Spark: `yyyyMMdd` parseable |
| `prepare_bronze(df, batch_id)` | `_tenant_id`, `_batch_id`, timestamp de ingesta |
| `normalize_units` / `classify_delivery_flags` | ST normalizado + flags routine/bonus |
| `apply_anomaly_rules` | Limpia / cuarentena / descarta (tipos inválidos) |
| `enrich_with_scd` | Join temporal inclusivo `[valid_from, valid_to]` |
| `build_fact_payload` | Columnas canónicas de `fact_deliveries` |
| `check_qty_st_positive` | DQ critical: qty ST > 0 |
| `check_delivery_type_domain` | DQ warning: tipo en dominio |
| `check_fk_material_resolved` | DQ critical: descripción/categoría presentes |
| `check_unit_normalized` | DQ info: CS × 20 = ST |
| `quality_result(...)` | Dict tipado hacia `quality_logs` |

### Interfaces

| Función | Rol |
|---|---|
| `build_parser()` | CLI: `--env`, `--tenant`, fechas, `--layer`, `--raw-deliveries`, `--raw-materials` |
| `main(argv?)` | Parse + `run_pipeline` |

Los módulos raíz (`bronze.py`, `silver.py`, `quality.py`, `cli.py`, …) reexportan estas APIs (shims).

---

## Suite de tests

| Archivo | Qué cubre |
|---|---|
| `tests/test_config.py` | Merge jerárquico, resolución de tenants, overrides de fechas/paths, env `batch2` |
| `tests/test_silver_transforms.py` | `cs_to_st`, normalización DF, dominio de tipos, anomalías, SCD histórico (no solo `is_current`) |
| `tests/test_scd_boundary.py` | Inclusividad en `valid_from`/`valid_to`, gap → cuarentena, determinismo de anomalías |
| `tests/test_idempotency.py` | Predicado `replaceWhere` estable + rango de fechas inclusivo |
| `tests/test_quality.py` | Checks detectan qty negativa, tipo fuera de dominio, FK material nula |

### Mapa test → función

| Test | Funciones bajo prueba |
|---|---|
| `test_load_base_and_dev_paths` | `load_config` |
| `test_env_override_changes_paths` | `load_config` (env qa) |
| `test_tenant_override_and_resolve` | `load_config`, `resolve_tenants` |
| `test_cli_overrides_dates` | `load_config(overrides=…)` |
| `test_batch2_env_and_raw_path_override` | `load_config` batch2 + override `raw_deliveries` |
| `test_cs_to_st_conversion` | `cs_to_st` |
| `test_normalize_units_dataframe` | `normalize_units` |
| `test_filter_invalid_delivery_types` | `is_valid_delivery_type` |
| `test_anomaly_quarantine_and_discard` | `apply_anomaly_rules` |
| `test_temporal_scd_join_uses_historical_version` | `enrich_with_scd` |
| `test_scd_inclusive_on_valid_from_and_valid_to` | `enrich_with_scd` (bordes) |
| `test_scd_day_after_valid_to_is_unmatched_when_gap` | `enrich_with_scd` (gap) |
| `test_anomaly_rules_are_deterministic` | `apply_anomaly_rules` ×2 |
| `test_replace_where_stable_for_same_window` | espejo de predicado Bronze |
| `test_date_range_inclusive_count` | `date_range_yyyymmdd` |
| `test_quality_checks_detect_failures` | `check_qty_*`, `check_delivery_*`, `check_fk_*` |

### Fuera de pytest (smoke)

| Script | Qué valida |
|---|---|
| `docker compose --profile smoke run --rm pipeline-smoke` | Pipeline completo tenant `sv`, 1 semana |
| `scripts/smoke_e2e.sh` / `.ps1` | Dos corridas + presencia de Gold parquet (idempotencia operativa) |
| `docker compose -f docker-compose.queries.yml run --rm queries` | Spark SQL demo (Gold/Silver/Bronze/quarantine/quality) |
