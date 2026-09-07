# Infraestructura (Terraform) — onboarding de tenant

En producción, el onboarding de un tenant nuevo debería provisionar de forma idempotente:

1. Schemas Unity Catalog: `bronze_<tenant>`, `silver_<tenant>`, `gold_<tenant>` dentro de `saas_<env>`.
2. Paths ADLS Gen2: `abfss://data@<account>.dfs.core.windows.net/{bronze|silver|gold|*_quarantine}/<tenant>/...`
3. Grants: grupo `data-eng-<tenant>` con `USE SCHEMA` + `SELECT/MODIFY` según capa.
4. Secretos: scope Databricks con connection strings / SPN (no en git).
5. Job cluster policy / warehouse SQL opcional para consumo Gold.

No se requiere `terraform plan` contra una cuenta real en esta prueba. Snippet ilustrativo del módulo principal:

```hcl
variable "env" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "catalog_name" {
  type    = string
  default = null
}

locals {
  catalog = coalesce(var.catalog_name, "saas_${var.env}")
  layers  = ["bronze", "silver", "gold"]
  schemas = [for l in local.layers : "${l}_${var.tenant_id}"]
}

resource "databricks_schema" "tenant" {
  for_each     = toset(local.schemas)
  catalog_name = local.catalog
  name         = each.value
  comment      = "SAAS ${each.value} schema for tenant ${var.tenant_id}"
}

resource "databricks_directory" "adls_paths" {
  for_each = toset(concat(local.layers, ["bronze_quarantine", "silver_quarantine"]))
  path     = "/mnt/saas/${var.env}/${each.value}/${var.tenant_id}"
}

resource "databricks_grants" "tenant_schema" {
  for_each = databricks_schema.tenant
  schema   = "${local.catalog}.${each.value.name}"

  grant {
    principal  = "data-eng-${var.tenant_id}"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
  }
}

resource "databricks_secret_acl" "tenant_scope" {
  scope = "saas-${var.env}-${var.tenant_id}"
  principal = "data-eng-${var.tenant_id}"
  permission = "READ"
}

output "tenant_schemas" {
  value = [for s in databricks_schema.tenant : s.name]
}
```

Uso conceptual:

```hcl
module "tenant_sv" {
  source    = "./modules/tenant"
  env       = "dev"
  tenant_id = "sv"
}
```
