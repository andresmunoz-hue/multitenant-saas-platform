# Code review — `bad_code.py`

Revisión de la entrega hipotética de un ingeniero junior (Anexo A).

## 1. Pandas + loop fila a fila en un job Spark

**Qué está mal:** Lee el CSV con pandas e itera `iterrows()` para transformar.

**Por qué importa:** Pierde paralelismo de Spark, escala mal y mezcla dos engines sin necesidad.

**Cómo se corrige:** Leer con `spark.read.csv` y expresar la lógica con `withColumn` / `filter` / `select`.

## 2. Lógica de negocio hardcodeada

**Qué está mal:** Tipos válidos (`ZPRE`/`ZVE1`), factor `* 20` y país `"GT"` están literales.

**Por qué importa:** Impide reutilizar el job para otros tenants/reglas y dificulta tests.

**Cómo se corrige:** Parametrizar `tenant_id`, `valid_types` y `cs_to_st_factor` (idealmente desde config YAML).

## 3. Alcance incompleto vs reglas de negocio reales

**Qué está mal:** Solo procesa rutina; ignora bonificaciones (`Z04`/`Z05`), no cuarentena anomalías ni valida nulos.

**Por qué importa:** Métricas de revenue/unidades quedan sesgadas y errores de origen se pierden.

**Cómo se corrige:** Separar filtrado de dominio, cuarentena y agregación; validar `cantidad`/`precio` antes de calcular.

## 4. Escritura no idempotente / path hardcodeado

**Qué está mal:** `overwrite` a `/tmp/output/` + país, sin partición de negocio ni Delta, y side-effect al importar (`process(...)` al final del módulo).

**Por qué importa:** No es reproducible en CI/Databricks, ensucia `/tmp`, y dificulta importar el módulo en tests.

**Cómo se corrige:** Inyectar `output_path`, particionar por tenant, preferir Delta si el destino es el lakehouse, y mover la ejecución a `if __name__ == "__main__"`.

## 5. Naming e higiene

**Qué está mal:** `fecha` vs `fecha_proceso`, `country` vs tenant, sin tipado ni manejo de errores de IO.

**Por qué importa:** Rompe convenciones del equipo y encarece mantenimiento.

**Cómo se corrige:** snake_case alineado al modelo (`tenant_id`, `fecha_proceso`), type hints y fallos explícitos.

---

## Cómo se lo explicaría al junior

Empezaría reconociendo que el código “funciona” para un demo de un país, y luego enfocaría el feedback en **impacto** (escala, correctitud, reuso), no en estilo vacío. Haría un pair-programming corto: reescribir juntos el filtro y la conversión CS→ST en Spark, correr un test unitario de esa transformación, y comparar tiempos/claridad. Le pediría investigar por su cuenta: (1) por qué `iterrows` es un anti-patrón, (2) diferencia entre overwrite de carpeta vs overwrite de partición Delta, y (3) cómo modelar reglas de negocio en config en lugar de literales. Cerraría pidiendo un PR pequeño solo con el refactor de IO + parámetros, antes de meter calidad de datos.
