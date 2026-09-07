"""Idempotency helpers: replaceWhere partition overwrite is stable for a date window."""

from saas_pipeline.infrastructure.config.paths import date_range_yyyymmdd


def build_bronze_replace_where(tenant: str, start: str, end: str) -> str:
    """Mirror bronze overwrite predicate used by ingest_bronze."""
    partitions = date_range_yyyymmdd(start, end)
    replace_list = ",".join(repr(p) for p in partitions)
    return f"_tenant_id = '{tenant}' AND fecha_proceso IN ({replace_list})"


def test_replace_where_stable_for_same_window():
    a = build_bronze_replace_where("sv", "2025-03-01", "2025-03-03")
    b = build_bronze_replace_where("sv", "2025-03-01", "2025-03-03")
    assert a == b
    assert "20250301" in a
    assert "20250302" in a
    assert "20250303" in a
    assert a.startswith("_tenant_id = 'sv'")


def test_date_range_inclusive_count():
    days = date_range_yyyymmdd("2025-07-01", "2025-07-07")
    assert len(days) == 7
    assert days[0] == "20250701"
    assert days[-1] == "20250707"
