"""Agregaciones Gold de ventas, logística, retención y pagos."""

from __future__ import annotations

from pathlib import Path

import duckdb


def build_gold(
    bronze_dir: Path, silver_dir: Path, gold_dir: Path
) -> dict[str, Path]:
    """Materializa datasets agregados aptos para consumo analítico.

    Args:
        bronze_dir: Directorio con pedidos y pagos fuente.
        silver_dir: Directorio con hechos y dimensiones depurados.
        gold_dir: Directorio de salida para agregados.

    Returns:
        Tabla Gold a ruta Parquet.
    """
    gold_dir.mkdir(parents=True, exist_ok=True)
    required = ("fct_orders", "dim_customers", "dim_products", "stg_payments")
    missing = [name for name in required if not (silver_dir / f"{name}.parquet").exists()]
    if missing:
        raise FileNotFoundError(f"Faltan tablas Silver requeridas: {', '.join(missing)}")
    con = duckdb.connect()
    try:
        for name, path in (
            ("fct_orders", silver_dir / "fct_orders.parquet"),
            ("dim_customers", silver_dir / "dim_customers.parquet"),
            ("dim_products", silver_dir / "dim_products.parquet"),
            ("stg_payments", silver_dir / "stg_payments.parquet"),
        ):
            _create_view(con, name, path)
        outputs: dict[str, Path] = {}
        for table, query in _gold_queries().items():
            destination = (gold_dir / f"{table}.parquet").resolve()
            escaped_destination = str(destination).replace("'", "''")
            con.execute(
                f"COPY ({query}) TO '{escaped_destination}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            outputs[table] = destination
        return outputs
    finally:
        con.close()


def _create_view(con: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    """Create a DuckDB view over a Parquet file."""
    escaped = str(path.resolve()).replace("'", "''")
    con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{escaped}')")


def _gold_queries() -> dict[str, str]:
    """Return SQL aggregations at their documented grains."""
    return {
        "monthly_sales_by_category": """
            SELECT DATE_TRUNC('month', f.purchase_timestamp)::DATE AS sales_month,
                   p.product_category_name,
                   COUNT(DISTINCT f.order_id) AS order_count,
                   SUM(f.item_amount) AS gross_sales,
                   AVG(f.item_amount) AS average_item_amount
            FROM fct_orders f
            JOIN dim_products p USING (product_id)
            WHERE f.purchase_timestamp IS NOT NULL
            GROUP BY 1, 2
        """,
        "delivery_metrics": """
            WITH order_grain AS (
                SELECT order_id, MIN(purchase_timestamp) AS purchase_timestamp,
                       MIN(delivered_timestamp) AS delivered_timestamp,
                       MIN(estimated_delivery_timestamp) AS estimated_delivery_timestamp
                FROM fct_orders GROUP BY order_id
            )
            SELECT DATE_TRUNC('month', purchase_timestamp)::DATE AS sales_month,
                   COUNT(*) AS order_count,
                   AVG(GREATEST(DATE_DIFF('day', purchase_timestamp, delivered_timestamp), 0))
                       FILTER (WHERE delivered_timestamp IS NOT NULL AND purchase_timestamp IS NOT NULL)
                       AS average_delivery_days,
                   SUM(CASE WHEN delivered_timestamp > estimated_delivery_timestamp
                            THEN 1 ELSE 0 END) AS late_order_count
            FROM order_grain
            WHERE purchase_timestamp IS NOT NULL
            GROUP BY 1
        """,
        "customer_retention": """
            WITH customer_orders AS (
                SELECT c.customer_unique_id, f.order_id,
                       DATE_TRUNC('month', MIN(f.purchase_timestamp))::DATE AS order_month
                FROM fct_orders f
                JOIN dim_customers c USING (customer_id)
                WHERE f.purchase_timestamp IS NOT NULL
                GROUP BY 1, 2
            )
            SELECT order_month,
                   COUNT(DISTINCT customer_unique_id) AS active_customers,
                   COUNT(DISTINCT customer_unique_id)
                     FILTER (WHERE order_number > 1) AS returning_customers
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY customer_unique_id ORDER BY order_month, order_id
                ) AS order_number
                FROM customer_orders
            )
            GROUP BY order_month
        """,
        "payment_distribution": """
            SELECT payment_type, COUNT(*) AS payment_count,
                   SUM(payment_value) AS payment_value,
                   AVG(payment_value) AS average_payment_value
            FROM stg_payments
            GROUP BY payment_type
        """,
    }
