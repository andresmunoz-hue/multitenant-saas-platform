# Observaciones adicionales (implementación / negocio)

Complemento a [observations.md](observations.md). Enfocadas en decisiones del código Silver y contratos de datos que conviene validar con negocio en sustentación.

## 1. Upsert de `dim_materials`: clave `material + valid_from` vs close/insert SCD2

**Ángulo:** contrato del origen + semántica SCD2.

En `_upsert_dim_materials` el feed se prepara tipando fechas y con `dropDuplicates(["material", "valid_from"])`, y el MERGE usa:

```text
t.material = s.material AND t.valid_from = s.valid_from AND t._tenant_id = s._tenant_id
→ whenMatchedUpdateAll / whenNotMatchedInsertAll
```

Eso asume que el CSV de catálogo **ya llega como versiones SCD2 materializadas** (cada fila trae `valid_from`, `valid_to`, `is_current`). En ese modelo:

- Un `valid_to` diligenciado / corregido sobre la **misma** versión (`mismo material + valid_from`) es **UPDATE** de atributos de esa versión, no un nuevo corte.
- Un `valid_from` nuevo es **INSERT** de una versión nueva.
- `dropDuplicates` solo defiende colisiones exactas en el batch; no define prioridad de negocio.

**Alternativas que dependen de negocio (no del MERGE actual):**

| Si el origen… | Entonces… |
|---|---|
| Solo manda el estado vigente (sin historial) | SCD2 “evento”: cerrar `is_current` (setear `valid_to`) + insertar fila nueva |
| Trae duplicados ambiguos del mismo `material+valid_from` con atributos distintos | Hace falta regla de prioridad (`row_number`, origen, timestamp), no solo dedupe |
| Puede republicar cambiando `valid_from` de una versión ya usada en hechos | Política de corrección histórica (reproceso / cuarentena) |

**Cómo lo resolví en el MVP:** MERGE por `material + valid_from` alineado al snapshot del catálogo de la prueba y a `modelo_inicial.md`. El join de hechos sigue siendo **temporal** (`fecha BETWEEN valid_from AND valid_to`), no solo `is_current`.

**Pendiente a validar con negocio:** no solape de ventanas por SKU, una sola fila `is_current=true`, y qué hacer ante correcciones de `valid_from` ya publicado.

## 2. `is_current` es auxiliar, no fuente del join

**Ángulo:** riesgo de implementación incorrecta.

Usar solo `is_current = true` al enriquecer entregas rompería hechos históricos (precio/categoría de la versión vigente en la `fecha_proceso`). El pipeline usa join temporal inclusivo; `is_current` queda como conveniencia operativa / DQ futura (“una vigente por material”).

## 3. Deduplicación de hechos vs dimensión

**Ángulo:** claves distintas por capa.

- **Dim materiales:** natural key de versión = `material + valid_from` (+ tenant en path multi-tenant).
- **Fact deliveries:** natural key de MERGE = tenant + `fecha_proceso` + `transporte` + `ruta` + `material` + `tipo_entrega`.

No se debe reutilizar la misma lógica de upsert: un cambio de `valid_to` en dim no implica reescribir facts; los facts se re-resuelven al re-correr Silver sobre la ventana de fechas si negocio pide reproceso.

## 4. Horizonte

- Expectation de no solape SCD2 + exactly-one `is_current` por material en `quality_logs`.
- Modo “event SCD2” opcional detrás de un flag de config si el contrato del catálogo cambia.
- Documentar en runbook cuándo un cambio de `valid_to` exige reproceso de Silver/Gold.

## 5. Ventajas de Clean Architecture en esta implementación

**Ángulo:** por qué se organizó el código así (no solo el diagrama de capas).

El mapa vive en [architecture.md](architecture.md). Lo que aporta **en este MVP** (lakehouse multi-tenant Medallion):

1. **Reglas de negocio testeables sin cluster** — `domain/` (`cs_to_st`, tipos de entrega, excepciones DQ) se valida con pytest barato; no hace falta un lakehouse completo para regresión de reglas.
2. **Mismo use case en local y Databricks** — `run_pipeline(..., spark=spark)` reutiliza la sesión del cluster; el CLI crea Spark local. Cambia el adapter de sesión, no el flujo B→S→Q→G.
3. **Orquestación legible** — `application/use_cases` expresa *qué* hace cada capa; MERGE, `replaceWhere`, schemas Delta quedan en `infrastructure/`. Facilita demo y code review.
4. **Cambiar infra sin reescribir reglas** — Auto Loader, UC shared dim, otro entrypoint (Job/API) tocan adapters; no reescriben dominio ni el contrato de anomalías.
5. **CLI delgado** — `interfaces/cli.py` solo parsea flags y delega; evita monolito `pipeline.py`.

**Qué no es:** no se abstrajo el DataFrame detrás de repositorios inventados. Spark sigue siendo el motor; Clean Architecture aquí es **dirección de dependencias**, no un framework extra.

**Frase de sustentación:** *“Las reglas y el flujo Medallion están desacoplados del runtime (CLI vs Databricks) y de los detalles de Delta, así puedo testear negocio barato y reusar el mismo use case en local o en el cluster.”*
