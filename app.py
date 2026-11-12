"""Streamlit dashboard para consultar las métricas de la capa Gold."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


def _query_gold(table: str) -> pd.DataFrame:
    """Load one known Gold table from Parquet."""
    path = (Path(os.getenv("ECOMMERCE_STORAGE_DIR", "storage")) / "gold" / f"{table}.parquet")
    if not path.exists():
        raise FileNotFoundError(f"No existe la tabla Gold {table}: ejecuta Dagster primero.")
    con = duckdb.connect()
    try:
        return con.execute("SELECT * FROM read_parquet(?)", [str(path.resolve())]).df()
    finally:
        con.close()


st.set_page_config(page_title="E-commerce Lakehouse", layout="wide")
st.title("E-commerce | Métricas Gold")

try:
    sales = _query_gold("monthly_sales_by_category")
    delivery = _query_gold("delivery_metrics")
    payments = _query_gold("payment_distribution")
except FileNotFoundError as error:
    st.info(str(error))
else:
    total_sales = float(sales["gross_sales"].sum()) if not sales.empty else 0.0
    total_orders = int(delivery["order_count"].sum()) if not delivery.empty else 0
    average_delivery = (
        float(delivery["average_delivery_days"].mean())
        if not delivery.empty and delivery["average_delivery_days"].notna().any()
        else 0.0
    )
    first, second, third = st.columns(3)
    first.metric("Ventas brutas (BRL)", f"R${total_sales:,.2f}")
    second.metric("Pedidos", f"{total_orders:,}")
    third.metric("Entrega promedio (días)", f"{average_delivery:.1f}")

    st.subheader("Ventas mensuales por categoría")
    if not sales.empty:
        sales["sales_month"] = pd.to_datetime(sales["sales_month"])
        st.bar_chart(
            sales.pivot_table(
                index="sales_month",
                columns="product_category_name",
                values="gross_sales",
                aggfunc="sum",
                fill_value=0,
            )
        )
        st.dataframe(sales, use_container_width=True)
    else:
        st.info("No hay ventas en la capa Gold.")

    left, right = st.columns(2)
    with left:
        st.subheader("Tiempos de entrega")
        st.dataframe(delivery, use_container_width=True)
    with right:
        st.subheader("Distribución de pagos")
        if not payments.empty:
            st.bar_chart(payments.set_index("payment_type")["payment_value"])
        st.dataframe(payments, use_container_width=True)
