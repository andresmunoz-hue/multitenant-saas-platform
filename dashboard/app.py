"""Streamlit dashboard: Gold metrics, quality logs, and quarantine."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

KNOWN_TENANTS = ("sv", "hn", "ec", "gt", "jm", "pe")

DATASETS = {
    "primary": {
        "label": "Primary (Jan–Jun 2025)",
        "gold": "gold",
        "quality": "shared/quality_logs",
        "quarantine_root": "",
    },
    "batch2": {
        "label": "Batch2 (Jul 2025)",
        "gold": "gold_batch2",
        "quality": "batch2/shared/quality_logs",
        "quarantine_root": "batch2",
    },
}


def data_root() -> Path:
    return Path(os.environ.get("DASHBOARD_DATA_ROOT", "data")).resolve()


def _read_delta_or_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        from deltalake import DeltaTable

        return DeltaTable(str(path)).to_pandas()
    except Exception:
        files = list(path.rglob("*.parquet"))
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def gold_table_path(dataset: str, tenant: str) -> Path:
    return data_root() / DATASETS[dataset]["gold"] / tenant / "daily_metrics_by_delivery_type"


def quality_logs_path(dataset: str) -> Path:
    return data_root() / DATASETS[dataset]["quality"]


def bronze_quarantine_path(dataset: str, tenant: str) -> Path:
    qroot = DATASETS[dataset]["quarantine_root"]
    base = data_root() / qroot if qroot else data_root()
    return base / "bronze_quarantine" / tenant / "deliveries"


def silver_quarantine_path(dataset: str, tenant: str) -> Path:
    qroot = DATASETS[dataset]["quarantine_root"]
    base = data_root() / qroot if qroot else data_root()
    return base / "silver_quarantine" / tenant / "fact_deliveries"


@st.cache_data(ttl=30)
def load_gold(dataset: str, tenant: str) -> pd.DataFrame:
    df = _read_delta_or_parquet(gold_table_path(dataset, tenant))
    if df.empty:
        return df
    if "fecha_proceso" in df.columns:
        df["fecha_proceso"] = df["fecha_proceso"].astype(str)
        df["fecha"] = pd.to_datetime(df["fecha_proceso"], format="%Y%m%d", errors="coerce")
    for col in ("total_units", "total_revenue", "active_routes", "active_transports"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=30)
def load_quality_logs(dataset: str) -> pd.DataFrame:
    return _read_delta_or_parquet(quality_logs_path(dataset))


@st.cache_data(ttl=30)
def load_quarantine(dataset: str, tenant: str, layer: str) -> pd.DataFrame:
    path = (
        bronze_quarantine_path(dataset, tenant)
        if layer == "bronze"
        else silver_quarantine_path(dataset, tenant)
    )
    return _read_delta_or_parquet(path)


def discover_tenants(dataset: str) -> list[str]:
    root = data_root() / DATASETS[dataset]["gold"]
    if not root.exists():
        return list(KNOWN_TENANTS)
    found = sorted(p.name for p in root.iterdir() if p.is_dir())
    return found or list(KNOWN_TENANTS)


def _render_gold(filtered: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total units (ST)", f"{filtered['total_units'].sum():,.0f}")
    c2.metric("Total revenue", f"{filtered['total_revenue'].sum():,.2f}")
    c3.metric("Active routes (avg)", f"{filtered['active_routes'].mean():.1f}")
    c4.metric("Active transports (avg)", f"{filtered['active_transports'].mean():.1f}")

    st.subheader("Revenue by day")
    daily = (
        filtered.groupby("fecha", as_index=False)["total_revenue"]
        .sum()
        .sort_values("fecha")
    )
    st.line_chart(daily.set_index("fecha")["total_revenue"])

    left, right = st.columns(2)
    with left:
        st.subheader("Units by delivery type")
        by_type = (
            filtered.groupby("tipo_entrega", as_index=False)["total_units"]
            .sum()
            .sort_values("total_units", ascending=False)
        )
        st.bar_chart(by_type.set_index("tipo_entrega")["total_units"])
    with right:
        st.subheader("Revenue by delivery type")
        by_type_rev = (
            filtered.groupby("tipo_entrega", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
        )
        st.bar_chart(by_type_rev.set_index("tipo_entrega")["total_revenue"])

    show_cols = [
        c
        for c in (
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
            "total_units",
            "total_revenue",
            "active_routes",
            "active_transports",
        )
        if c in filtered.columns
    ]
    st.subheader("Detail")
    st.dataframe(
        filtered[show_cols].sort_values(["fecha_proceso", "tipo_entrega"]),
        use_container_width=True,
        hide_index=True,
    )


def _render_quality(qdf: pd.DataFrame, tenant: str) -> None:
    if qdf.empty:
        st.info("No quality logs yet. Run Silver + quality checks first.")
        return
    view = qdf.copy()
    if "tenant_id" in view.columns:
        view = view[view["tenant_id"] == tenant]
    if view.empty:
        st.info(f"No quality rows for tenant `{tenant}`.")
        return

    if "check_passed" in view.columns:
        passed = int(view["check_passed"].sum()) if view["check_passed"].dtype != object else int(
            view["check_passed"].astype(str).str.lower().isin(("true", "1")).sum()
        )
        failed = len(view) - passed
        c1, c2, c3 = st.columns(3)
        c1.metric("Checks logged", len(view))
        c2.metric("Passed", passed)
        c3.metric("Failed", failed)

    if "check_name" in view.columns and "records_failed" in view.columns:
        st.subheader("Failed rows by check")
        by_check = (
            view.groupby("check_name", as_index=False)["records_failed"]
            .sum()
            .sort_values("records_failed", ascending=False)
        )
        st.bar_chart(by_check.set_index("check_name")["records_failed"])

    st.subheader("Recent quality events")
    st.dataframe(view.tail(100), use_container_width=True, hide_index=True)


def _render_quarantine(bronze_q: pd.DataFrame, silver_q: pd.DataFrame) -> None:
    c1, c2 = st.columns(2)
    c1.metric("Bronze quarantine rows", len(bronze_q))
    c2.metric("Silver quarantine rows", len(silver_q))

    if bronze_q.empty and silver_q.empty:
        st.info("No quarantine tables found for this tenant/dataset.")
        return

    if not bronze_q.empty:
        st.subheader("Bronze — invalid / null fecha_proceso")
        if "_quarantine_reason" in bronze_q.columns:
            st.bar_chart(bronze_q["_quarantine_reason"].value_counts())
        st.dataframe(bronze_q.head(200), use_container_width=True, hide_index=True)

    if not silver_q.empty:
        st.subheader("Silver — cantidad / precio / material / SCD miss")
        if "_quarantine_reason" in silver_q.columns:
            st.bar_chart(silver_q["_quarantine_reason"].value_counts())
        st.dataframe(silver_q.head(200), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="SAAS Delivery Ops", layout="wide")
    st.title("SAAS Delivery Ops")
    st.caption("Gold metrics · quality logs · quarantine (local Spark/Delta)")

    with st.sidebar:
        st.header("Filters")
        dataset = st.selectbox(
            "Dataset",
            list(DATASETS.keys()),
            format_func=lambda k: DATASETS[k]["label"],
        )
        tenants = discover_tenants(dataset)
        tenant = st.selectbox("Tenant", tenants, index=0)
        st.text(f"Data root: `{data_root()}`")
        if st.button("Refresh cache"):
            st.cache_data.clear()

    tab_gold, tab_quality, tab_quarantine = st.tabs(
        ["Gold metrics", "Quality", "Quarantine"]
    )

    with tab_gold:
        df = load_gold(dataset, tenant)
        if df.empty:
            st.warning(
                f"No Gold data for tenant `{tenant}` / dataset `{dataset}`. "
                "Run the pipeline first."
            )
            st.code(
                "docker compose --profile init run --rm pipeline-init\n"
                "# or smoke:\n"
                "docker compose --profile smoke run --rm pipeline-smoke",
                language="bash",
            )
        else:
            tipos = (
                sorted(df["tipo_entrega"].dropna().unique().tolist())
                if "tipo_entrega" in df
                else []
            )
            min_d, max_d = df["fecha"].min(), df["fecha"].max()
            with st.sidebar:
                selected_types = st.multiselect("Delivery types", tipos, default=tipos)
                date_range = st.date_input(
                    "Date range",
                    value=(
                        min_d.date() if pd.notna(min_d) else None,
                        max_d.date() if pd.notna(max_d) else None,
                    ),
                )
            filtered = df.copy()
            if selected_types:
                filtered = filtered[filtered["tipo_entrega"].isin(selected_types)]
            if (
                isinstance(date_range, (list, tuple))
                and len(date_range) == 2
                and date_range[0]
                and date_range[1]
            ):
                start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
                filtered = filtered[(filtered["fecha"] >= start) & (filtered["fecha"] <= end)]
            if filtered.empty:
                st.info("No rows match the selected filters.")
            else:
                _render_gold(filtered)

    with tab_quality:
        _render_quality(load_quality_logs(dataset), tenant)

    with tab_quarantine:
        _render_quarantine(
            load_quarantine(dataset, tenant, "bronze"),
            load_quarantine(dataset, tenant, "silver"),
        )


if __name__ == "__main__":
    main()
