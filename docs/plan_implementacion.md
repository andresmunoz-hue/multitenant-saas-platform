# Plan de implementación — Senior Data Engineer (SAAS multi-tenant)

**Estado:** plan + modelo inicial listos. **Sin credenciales / sin Databricks cloud** en esta fase.  
**Repo local:** `multitenant-saas-platform` (no hacer commit hasta acordarlo).  
**Modelo de datos:** `docs/modelo_inicial.md`.

---

## 0. Objetivo y criterio de éxito

Entregar un MVP del pipeline Medallion **compatible con Databricks** (PySpark 3.5 + Delta 3.x), ejecutable en local, con:

1. Config jerárquica multi-tenant (OmegaConf)
2. Bronze / Silver (SCD2 + join temporal) / Gold
3. Cuarentena + `quality_logs`
4. CI (lint + pytest)
5. Mentoría (`bad_code` → review → `good_code`)
6. Docs: README, `observations.md`, `infra.md`, onboarding

**Prioridad explícita de la prueba:** MVP sólido > alcance amplio a medias.

---

## 1. Alcance por días (12–15 h)

### Día 1 — Setup + Bronze (~4 h)

| # | Tarea | Done when |
|---|---|---|
| 1.1 | Scaffold repo (Anexo B): `src/saas_pipeline`, `config/`, `tests/`, `mentoring/`, `.github/workflows` | Estructura creada |
| 1.2 | `pyproject.toml` / `requirements.txt` + `Makefile`: Python ≥3.11, PySpark 3.5.x, delta-spark 3.x, omegaconf, pytest, ruff | `make install` / `make spark-check` |
| 1.3 | YAML: `base` + `env/{dev,qa,main}` + `tenants/{sv,...}` | `config.load(env,tenant)` mergea overrides |
| 1.4 | `cli.py`: `--env --tenant --start-date --end-date --layer` | Help usable |
| 1.5 | Spark session factory local (Delta extensions) | Session crea y lee/escribe Delta |
| 1.6 | Bronze: CSV → Delta `deliveries` + tech cols + tenant lower + overwrite partición | Re-run no duplica partición |
| 1.7 | Seeds: `raw/*.csv` (ya copiados) + `.gitignore` para `data/` | Raw versionado; data/ no |

**Fuera de Día 1:** Auto Loader, Unity Catalog real, cloud auth.

### Día 2 — Silver + DQ (~5 h)

| # | Tarea | Done when |
|---|---|---|
| 2.1 | `dim_materials` SCD2 + MERGE (`material`,`valid_from`) | Historial + `is_current` consistente |
| 2.2 | Reglas 5.6: cuarentena / descarte / dedup (orden documentado) | Conteos alineados al perfil |
| 2.3 | Normalización CS→ST (×20), flags routine/bonus, filtro tipos | Solo 4 tipos en fact |
| 2.4 | `fact_deliveries` MERGE por business key | Idempotente |
| 2.5 | Enrichment **join temporal** (no solo `is_current`) | Precio/categoría históricos correctos |
| 2.6 | `quality.py` ≥3 checks + append a `quality_logs` | Severidades + `fail_on_critical` |
| 2.7 | Tests: unidades, filtro tipos, anomalías, SCD/join | ≥3 tests significativos |

### Día 3 — Gold + CI + mentoría + docs (~5 h)

| # | Tarea | Done when |
|---|---|---|
| 3.1 | Gold `daily_metrics_by_delivery_type` (recompute por fecha) | Métricas según 6.4 |
| 3.2 | Orquestación end-to-end por tenant / `all` + `fail_fast` | Un comando corre B→S→G |
| 3.3 | GitHub Actions: ruff + pytest + load YAML | Verde en PR |
| 3.4 | Mentoría: `bad_code.py` (anexo), `code_review.md` (≥4), `good_code.py`, nota junior | Completo |
| 3.5 | `docs/infra.md` (Terraform snippet onboarding tenant) | ~30–50 líneas ilustrativas |
| 3.6 | `docs/onboarding-tenant.md` + `docs/observations.md` (≥3) | Obligatorio |
| 3.7 | README reproducible + “Qué dejé fuera” | Demo local en <10 min |
| 3.8 | Commits incrementales + **un PR** (cuando se pida) | Historia limpia |

**Bonus solo si sobra tiempo:** 2ª Gold, pre-commit, Streamlit, Terraform validate.

---

## 2. Orden técnico de módulos

```text
config.py  →  spark_session  →  bronze.py
                              →  silver.py (dim → quarantine/filter → fact MERGE → enrich)
                              →  quality.py
                              →  gold.py
cli.py orquesta layers
```

**Flujo por tenant y rango de fechas:**

1. Resolver config (`base` ← `env` ← `tenant` ← CLI)
2. Generar `_run_id` / `_batch_id`
3. Bronze deliveries (particiones del rango)
4. Upsert `dim_materials`
5. Leer Bronze → aplicar reglas → quarantine / discard / dedup
6. MERGE `fact_deliveries` + join temporal
7. Quality checks → `quality_logs`; si critical & flag → stop
8. Gold recompute particiones afectadas

---

## 3. Estructura de repo a materializar

```text
saas-data-platform/   # este workspace
├── README.md
├── Makefile
├── pyproject.toml
├── .gitignore
├── .github/workflows/ci.yml
├── docs/
│   ├── modelo_inicial.md          ← hecho
│   ├── plan_implementacion.md     ← este archivo
│   ├── prueba_tecnica_texto_extraido.txt
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
├── config/
│   ├── base.yaml
│   ├── env/{dev,qa,main}.yaml
│   └── tenants/{sv,hn,ec,gt,jm,pe}.yaml
├── raw/
│   ├── global_mobility_data_entrega_productos.csv
│   └── materials_catalog.csv
├── src/saas_pipeline/
│   ├── __init__.py
│   ├── config.py
│   ├── spark.py
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── quality.py
│   └── cli.py
├── tests/
│   ├── test_config.py
│   ├── test_silver_transforms.py
│   └── test_quality.py
└── mentoring/
    ├── bad_code.py
    ├── good_code.py
    └── code_review.md
```

---

## 4. Pruebas mínimas (CI)

1. **CS → ST:** `cantidad=2, unidad=CS` → `40`
2. **Filtro tipos:** `COBR`/`Z99` no entran a fact; se contabilizan como descarte
3. **Cuarentena material:** SKU `XX*` → quarantine + reason
4. **SCD join temporal:** material con cambio de precio; fecha cae en versión histórica, no `is_current`
5. **Config:** merge `base+env+tenant` y paths por `env`

---

## 5. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Spark local + Delta setup lento | Makefile con deps pinneadas; smoke test temprano Día 1 |
| Join temporal con `valid_to=9999-12-31` | Cast a date; documentar inclusividad |
| Fecha inválida no particionable | Cuarentena **antes** de write Bronze particionado, o partición `_invalid` documentada en observations |
| `tenant=all` + fallo parcial | `fail_fast` según config; resumen final |
| Scope creep (bonus/streaming) | Congelar MVP al cierre Día 2; bonus solo buffer Día 3 |

---

## 6. Qué queda explícitamente fuera (ahora)

- Credenciales Databricks / ADLS / Unity Catalog reales
- MongoDB, Couchbase, Auto Loader en producción
- Terraform aplicado contra cuenta real
- Dashboard / 2ª Gold (salvo buffer)

Cuando el plan y el modelo estén OK, el siguiente paso es **implementar Día 1 (scaffold + Bronze)** en este repo, aún en local.

---

## 7. Checklist de entrega (cierre)

- [ ] Pipeline B→S→G local por tenant y `all`
- [ ] Idempotencia verificable (re-run)
- [ ] SCD2 + join temporal
- [ ] Cuarentena + quality_logs + fail_on_critical
- [ ] CI verde
- [ ] Mentoría completa
- [ ] README + observations + infra + onboarding
- [ ] Commits incrementales + PR público
- [ ] Listo para demo 10 min en sustentación

---

## 8. Próximo paso acordado

1. Validar este plan y `docs/modelo_inicial.md`
2. Recién entonces: scaffold + Bronze (sin cloud credentials)
3. Commits solo cuando se indiquen
