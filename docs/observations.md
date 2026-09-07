# Observaciones a la arquitectura

Mínimo 3 observaciones sustantivas sobre la arquitectura provista en la prueba.

## 1. Catálogo de materiales global vs schema por tenant

**Ángulo:** decisión / alternativa.

La arquitectura ubica `dim_materials` bajo `silver_<tenant>`, pero el CSV de catálogo es **global** (no tiene `pais`). Replicar el SCD2 en cada tenant simplifica grants y el join local, pero duplica storage y complica la gobernanza de una sola verdad.

**Alternativa propuesta:** tabla shared `saas_<env>.silver_shared.dim_materials` + vista o lectura cross-schema por tenant. Trade-off: onboarding y grants un poco más complejos a cambio de consistencia master-data.

**Cómo lo resolví:** réplica por tenant en el MVP (alineado al path `silver/<tenant>/dim_materials`), documentado aquí para sustentación.

## 2. Fechas inválidas y particionado Bronze

**Ángulo:** ambigüedad resuelta.

La política 5.6 manda a cuarentena fechas nulas/inválidas, pero Bronze exige partición por `fecha_proceso`. No está explicitado si esas filas nunca entran a Bronze o si viven en una partición sentinel.

**Resolución en la implementación:** filas con fecha inválida/nula **no** se escriben en `bronze/.../deliveries` particionado; van a `bronze_quarantine/<tenant>/deliveries` con `_quarantine_reason=invalid_or_null_fecha_proceso`. El resto de anomalías (cantidad, precio, material, SCD miss) se evalúan en Silver.

## 3. Inclusividad de `valid_to` en el join temporal

**Ángulo:** ambigüedad + horizonte.

El enunciado usa `BETWEEN valid_from AND valid_to`. En SCD2 a veces `valid_to` es exclusivo. El dataset usa `9999-12-31` y cortes del estilo `2025-03-31` / `2025-04-01`, compatible con **inclusive**.

**Resolución:** join inclusivo (`fecha >= valid_from AND fecha <= valid_to`). En H2 propondría normalizar a `valid_to` exclusivo + tests de frontera automatizados.

## 4. Horizonte 2–3 (mejoras tecnológicas)

- **Auto Loader + checkpointing** por tenant para landing incremental.
- **Unity Catalog** real: schemas `bronze_<tenant>` + grants automáticos vía Terraform module.
- **Expectations declarativas** (DQX / custom rules engine) versionadas junto al config del tenant.
- **Gold cross-tenant** en schema shared con row-level security para análisis regional.
- **Observabilidad:** métricas de descartes/cuarentena en un dashboard operativo (el MVP ya incluye Streamlit sobre Gold; el siguiente paso sería alertas y SLAs de calidad).
