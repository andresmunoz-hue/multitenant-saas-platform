"""Streamlit dashboard over Gold Delta metrics."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

KNOWN_TENANTS = ("sv", "hn", "ec", "gt", "jm", "pe")


def data_root() -> Path:
    return Path(os.environ.get("DASHBOARD_DATA_ROOT", "data")).resolve()


def gold_table_path(tenant: str) -> Path:
    return data_root() / "gold" / tenant / "daily_metrics_by_delivery_type"


def quality_logs_path() -> Path:
    return data_root() / "shared" / "quality_logs"


@st.cache_data(ttl=30)
def load_gold(tenant: str) -> pd.DataFrame:
    path = gold_table_path(tenant)
    if not path.exists():
        return pd.DataFrame()
    try:
        from deltalake import DeltaTable

        df = DeltaTable(str(path)).to_pandas()
    except Exception:
        # Fallback: read parquet files under the table path
        files = list(path.rglob("*.parquet"))
        if not files:
            return pd.DataFrame()
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

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
def load_quality_logs() -> pd.DataFrame:
    path = quality_logs_path()
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


def discover_tenants_with_data() -> list[str]:
    root = data_root() / "gold"
    if not root.exists():
        return list(KNOWN_TENANTS)
    found = sorted(p.name for p in root.iterdir() if p.is_dir())
    return found or list(KNOWN_TENANTS)


def main() -> None:
    st.set_page_config(
        page_title="SAAS Delivery Metrics",
        layout="wide",
    )

    st.title("SAAS Delivery Metrics")
    st.caption("Gold layer · daily metrics by delivery type (local Spark/Delta stack)")

    tenants = discover_tenants_with_data()
    with st.sidebar:
        st.header("Filters")
        tenant = st.selectbox("Tenant", tenants, index=0)
        st.text(f"Data root: `{data_root()}`")
        refresh = st.button("Refresh cache")
        if refresh:
            st.cache_data.clear()

    df = load_gold(tenant)
    if df.empty:
        st.warning(
            f"No Gold data for tenant `{tenant}`. "
            "Run the pipeline first (`docker compose run pipeline`)."
        )
        st.code(
            "docker compose run --rm pipeline\n"
            "docker compose up dashboard",
            language="bash",
        )
        return

    tipos = sorted(df["tipo_entrega"].dropna().unique().tolist()) if "tipo_entrega" in df else []
    min_d = df["fecha"].min()
    max_d = df["fecha"].max()

    with st.sidebar:
        selected_types = st.multiselect("Delivery types", tipos, default=tipos)
        date_range = st.date_input(
            "Date range",
            value=(min_d.date() if pd.notna(min_d) else None, max_d.date() if pd.notna(max_d) else None),
        )

    filtered = df.copy()
    if selected_types:
        filtered = filtered[filtered["tipo_entrega"].isin(selected_types)]
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2 and date_range[0] and date_range[1]:
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        filtered = filtered[(filtered["fecha"] >= start) & (filtered["fecha"] <= end)]

    if filtered.empty:
        st.info("No rows match the selected filters.")
        return

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

    st.subheader("Detail")
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
    st.dataframe(
        filtered[show_cols].sort_values(["fecha_proceso", "tipo_entrega"]),
        use_container_width=True,
        hide_index=True,
    )

    qdf = load_quality_logs()
    if not qdf.empty:
        st.subheader("Quality logs (shared)")
        if "tenant_id" in qdf.columns:
            qdf = qdf[qdf["tenant_id"] == tenant]
        st.dataframe(qdf.tail(50), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
