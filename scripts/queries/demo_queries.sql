-- Demo / interview SQL snippets (reference).
-- Prefer: docker compose -f docker-compose.queries.yml run --rm queries
-- Views created by scripts/queries/run_demo_queries.py:
--   bronze, silver_fact, silver_dim, gold, bronze_q, silver_q, quality

-- Gold KPIs
SELECT tipo_entrega,
       ROUND(SUM(total_units), 2) AS units_st,
       ROUND(SUM(total_revenue), 2) AS revenue
FROM gold
GROUP BY tipo_entrega
ORDER BY revenue DESC;

-- Silver daily detail
SELECT fecha_proceso, tipo_entrega,
       ROUND(SUM(cantidad_normalizada_st), 2) AS units_st,
       ROUND(SUM(cantidad_normalizada_st * precio_transaccion), 2) AS revenue
FROM silver_fact
GROUP BY fecha_proceso, tipo_entrega
ORDER BY fecha_proceso, tipo_entrega
LIMIT 30;

-- Quarantine audit
SELECT _quarantine_reason, COUNT(*) AS rows
FROM bronze_q
GROUP BY _quarantine_reason
ORDER BY rows DESC;

SELECT _quarantine_reason, COUNT(*) AS rows
FROM silver_q
GROUP BY _quarantine_reason
ORDER BY rows DESC;

-- Quality
SELECT check_name, check_severity, check_passed, records_failed, executed_at
FROM quality
ORDER BY executed_at DESC
LIMIT 20;
