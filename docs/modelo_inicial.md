# Modelo inicial — Plataforma SAAS multi-tenant

Modelo de datos de referencia para el pipeline Medallion (Bronze → Silver → Gold), alineado a la arquitectura de la prueba. **Sin credenciales ni infra cloud**: rutas locales Delta.

## 1. Hallazgos del briefing (texto oculto)

Revisión del `.docx` (XML interno):

| Chequeo | Resultado |
|---|---|
| `w:vanish` (texto oculto Word) | 0 ocurrencias |
| Caracteres zero-width | 0 |
| Comentarios / customXml | No presentes |
| Texto blanco (`#FFFFFF`) | Solo encabezados de tabla sobre fondo azul (UI, no secreto) |
| `w:sz="4"` / gris `B0B0B0` | Atributos de borde, no tipografía oculta |

**Conclusión:** no hay instrucciones ocultas adicionales. El contrato está en el texto visible (secciones 4–12). Texto extraído en `docs/prueba_tecnica_texto_extraido.txt`.

## 2. Fuentes RAW

| Archivo | Rol | Filas (aprox.) |
|---|---|---|
| `raw/global_mobility_data_entrega_productos.csv` | Hechos de entrega | 3115 |
| `raw/materials_catalog.csv` | Dimensión SCD2 de materiales | 35 (28 SKUs) |

**Tenants observados:** `ec`, `gt`, `hn`, `jm`, `pe`, `sv` (normalizar `pais` → `_tenant_id` minúscula).

**Anomalías intencionales detectadas (perfil):**

- `fecha_proceso` nula: 20; inválida (no YYYYMMDD): 5
- `cantidad` nula: 20; negativa: 20; cero: 21
- `tipo_entrega` fuera de alcance (`COBR`, `Z99`): 190 → **descarte**
- Materiales huérfanos (`XX*`): 24 filas → **cuarentena**
- Duplicados exactos (aprox.): 20 → **deduplicar**
- `precio` nulo: 0 en este sample (regla igual aplica)

## 3. Diagrama lógico

```text
RAW CSV
  │  overwrite por partición (fecha_proceso + tenant)
  ▼
BRONZE  deliveries[+tech cols]
  │  reglas 5.6 + MERGE + SCD2 + join temporal
  ▼
SILVER  fact_deliveries ──┬── dim_materials (SCD2)
                          │
                          ├── silver_quarantine/...
                          └── shared/quality_logs
  │  recompute por partición
  ▼
GOLD  daily_metrics_by_delivery_type
```

Paths locales (mapeo de Unity Catalog):

```text
data/bronze/<tenant>/deliveries/fecha_proceso=YYYYMMDD/
data/silver/<tenant>/fact_deliveries/fecha_proceso=YYYYMMDD/
data/silver/<tenant>/dim_materials/
data/gold/<tenant>/daily_metrics_by_delivery_type/
data/bronze_quarantine/<tenant>/deliveries/
data/silver_quarantine/<tenant>/fact_deliveries/
data/shared/quality_logs/
```

Naming Unity (documental, no implementado en local): `saas_<env>.{bronze|silver|gold}_<tenant>.<table>`.

## 4. Esquemas por capa

### 4.1 Bronze — `deliveries`

Esquema fuente + columnas técnicas. Sin limpieza de negocio.

| Columna | Tipo | Notas |
|---|---|---|
| `pais` | string | Original (mayúsculas) |
| `fecha_proceso` | string | YYYYMMDD; puede ser nulo/inválido |
| `transporte` | long | |
| `ruta` | long | |
| `tipo_entrega` | string | |
| `material` | string | |
| `precio` | decimal | |
| `cantidad` | decimal | |
| `unidad` | string | `CS` \| `ST` |
| `_ingestion_timestamp` | timestamp | UTC |
| `_source_file` | string | |
| `_tenant_id` | string | `lower(pais)` |
| `_batch_id` | string | id de corrida |

**Partición:** `fecha_proceso`, `_tenant_id`  
**Escritura:** overwrite por partición (`replaceWhere` o delete+append equivalente).

### 4.2 Silver — `dim_materials` (SCD Type 2)

| Columna | Tipo | Notas |
|---|---|---|
| `material` | string | Business key |
| `descripcion` | string | Versionado |
| `categoria` | string | Versionado |
| `precio_base` | decimal | Versionado |
| `valid_from` | date | |
| `valid_to` | date | `9999-12-31` = abierto |
| `is_current` | boolean | Auxiliar; **no** fuente única del join |
| `_tenant_id` | string | Dimensión compartida por tenant schema (copia o vista lógica por tenant según config) |
| `_batch_id` | string | |

**MERGE key:** `material` + `valid_from`  
**Regla:** una sola fila `is_current=true` por SKU.

> Nota de diseño: el catálogo es global en el CSV. En el modelo multi-tenant de la prueba cada schema de tenant tiene su `dim_materials`. Decisión inicial: **replicar el mismo catálogo en cada tenant** (onboarding simple). Alternativa (vista shared) → documentar en `observations.md` si se cambia.

### 4.3 Silver — `fact_deliveries`

| Columna | Tipo | Notas |
|---|---|---|
| `_tenant_id` | string | PK parcial |
| `fecha_proceso` | date | Parseada; PK parcial / partición |
| `transporte` | long | PK parcial |
| `ruta` | long | PK parcial |
| `material` | string | PK parcial |
| `tipo_entrega` | string | PK parcial; solo `ZPRE,ZVE1,Z04,Z05` |
| `precio_transaccion` | decimal | Renombre de `precio` fuente |
| `cantidad_original` | decimal | |
| `unidad_original` | string | |
| `cantidad_normalizada_st` | decimal | `CS → *20`, `ST → 1:1` |
| `is_routine_delivery` | boolean | `ZPRE` \| `ZVE1` |
| `is_bonus_delivery` | boolean | `Z04` \| `Z05` |
| `material_descripcion` | string | Del join temporal |
| `material_categoria` | string | Del join temporal |
| `precio_base` | decimal | Informativo (catálogo) |
| `_batch_id` | string | |
| `_updated_at` | timestamp | |

**Business key (MERGE):**  
`(_tenant_id, fecha_proceso, transporte, ruta, material, tipo_entrega)`

**Join temporal (obligatorio):**

```sql
fact.fecha_proceso BETWEEN dim.valid_from AND dim.valid_to
```

No usar solo `is_current = true`.

### 4.4 Cuarentena

Tablas paralelas con el payload de la fila + `_quarantine_reason` (`string`).

| Motivo | Destino | Persistencia |
|---|---|---|
| fecha nula/inválida | quarantine | Sí |
| cantidad nula/negativa/cero | quarantine | Sí |
| material no en catálogo | quarantine | Sí |
| precio nulo | quarantine | Sí |
| `tipo_entrega` ∉ {ZPRE,ZVE1,Z04,Z05} | — | **Descarte** (solo métrica) |
| duplicado exacto | — | Dedup (conservar 1) |

### 4.5 Shared — `quality_logs`

| Columna | Tipo |
|---|---|
| `_run_id` | string |
| `_batch_id` | string |
| `tenant_id` | string |
| `layer` | string |
| `table_name` | string |
| `check_name` | string |
| `check_severity` | string (`critical` \| `warning` \| `info`) |
| `records_checked` | long |
| `records_failed` | long |
| `check_passed` | boolean |
| `executed_at` | timestamp |

### 4.6 Gold — `daily_metrics_by_delivery_type`

Granularidad: `(_tenant_id, fecha_proceso, tipo_entrega)`

| Columna | Cálculo |
|---|---|
| `total_units` | `sum(cantidad_normalizada_st)` |
| `total_revenue` | `sum(cantidad_normalizada_st * precio_transaccion)` |
| `active_routes` | `count(distinct ruta)` |
| `active_transports` | `count(distinct transporte)` |

**Escritura:** recompute por partición de fecha.

## 5. Configuración (modelo de parámetros)

Jerarquía OmegaConf:

```text
config/base.yaml
config/env/{dev,qa,main}.yaml
config/tenants/{sv,hn,ec,gt,jm,pe}.yaml
```

Parámetros mínimos:

- `paths.bronze|silver|gold`, `paths.quarantine_root`, `paths.quality_logs`
- `execution.start_date`, `execution.end_date`, `execution.tenant` (`sv` \| `all`), `execution.fail_fast`
- `quality.fail_on_critical`
- `business.cs_to_st_factor: 20`
- `business.valid_delivery_types: [ZPRE, ZVE1, Z04, Z05]`

## 6. Checks de calidad iniciales (Silver, ≥3)

| check_name | Severidad | Regla |
|---|---|---|
| `fk_material_resolved` | critical | 0 facts sin match SCD2 temporal |
| `qty_st_positive` | critical | `cantidad_normalizada_st > 0` |
| `delivery_type_domain` | warning | solo dominio válido |
| `unit_normalized_st` | info | `unidad` efectiva ST / factor correcto |

Si `quality.fail_on_critical=true` → abortar Gold para ese tenant.

## 7. Decisiones abiertas (para `observations.md` al implementar)

1. Catálogo global vs copia por tenant.
2. Dónde aplicar cuarentena: entre Bronze→Silver (preferido) vs Bronze temprana para fechas inválidas que impiden partición.
3. Semántica de `valid_to` inclusiva vs exclusiva en el join.
4. Orden de reglas cuando una fila viola varios motivos (prioridad: fecha → cantidad/precio → material → tipo).
