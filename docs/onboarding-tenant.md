# Onboarding de un tenant nuevo

## Local (esta prueba)

1. Confirmar que el CSV de entregas incluye filas con `pais=<CODE>` (mayúsculas en fuente).
2. Crear `config/tenants/<code>.yaml`:

```yaml
tenant_id: xx
execution:
  tenant: xx
```

3. Agregar `"xx"` a `KNOWN_TENANTS` en `src/saas_pipeline/config.py`.
4. Ejecutar:

```bash
python -m saas_pipeline --env dev --tenant xx --start-date 2025-01-01 --end-date 2025-06-30 --layer all
```

5. Verificar paths:

- `data/bronze/xx/deliveries/`
- `data/silver/xx/fact_deliveries/`
- `data/gold/xx/daily_metrics_by_delivery_type/`

## Producción (Databricks)

1. Aplicar módulo Terraform de `docs/infra.md` (`tenant_id=xx`).
2. Registrar el tenant en el job parameter / config service.
3. Apuntar `paths.*` del env a volúmenes/UC externos.
4. Correr backfill del rango histórico y activar schedule diario.
5. Validar `quality_logs` y grants del grupo `data-eng-xx`.
