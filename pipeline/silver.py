"""Limpieza, deduplicación y modelado dimensional en DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb


def build_silver(bronze_dir: Path, silver_dir: Path) -> dict[str, Path]:
    """Construye dimensiones, hecho de líneas de pedido y pagos limpios.

    Las dimensiones conservan sus identificadores de negocio como claves
    primarias. Los timestamps se parsean de forma tolerante a valores inválidos.

    Args:
        bronze_dir: Directorio de Parquet crudos.
        silver_dir: Directorio de salida para tablas Silver.

    Returns:
        Tabla a ruta Parquet.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    required = ("customers", "orders", "order_items", "payments", "products", "sellers")
    missing = [name for name in required if not (bronze_dir / f"{name}.parquet").exists()]
    if missing:
        raise FileNotFoundError(f"Faltan datasets Bronze requeridos: {', '.join(missing)}")
    con = duckdb.connect()
    try:
        for name in ("customers", "orders", "order_items", "payments", "products", "sellers"):
            path = bronze_dir / f"{name}.parquet"
            if path.exists():
                _create_view(con, name, path)
        statements = _silver_queries()
        outputs: dict[str, Path] = {}
        for table, query in statements.items():
            destination = (silver_dir / f"{table}.parquet").resolve()
            escaped_destination = str(destination).replace("'", "''")
            con.execute(
                f"COPY ({query}) TO '{escaped_destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            escaped_path = str(destination).replace("'", "''")
            con.execute(f"""CREATE VIEW "{table}" AS SELECT * FROM read_parquet('{escaped_path}')""")
            outputs[table] = destination
            if table == "dim_sellers":
                invalid_references = con.execute(
                    """
                    WITH order_versions AS (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY order_id ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM orders WHERE order_id IS NOT NULL
                    ),
                    item_versions AS (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY order_id, order_item_id ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM order_items
                        WHERE order_id IS NOT NULL AND order_item_id IS NOT NULL
                    )
                    SELECT COUNT(*)
                    FROM item_versions i
                    JOIN order_versions o ON o.order_id = i.order_id AND o.rn = 1
                    LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
                    LEFT JOIN dim_products p ON i.product_id = p.product_id
                    LEFT JOIN dim_sellers s ON i.seller_id = s.seller_id
                    WHERE i.rn = 1 AND (
                        c.customer_id IS NULL OR p.product_id IS NULL OR s.seller_id IS NULL
                    )
                    """
                ).fetchone()[0]
                if invalid_references:
                    raise ValueError(
                        f"Silver rechazó {invalid_references} líneas con referencias "
                        "faltantes a clientes, productos o vendedores."
                    )
        return outputs
    finally:
        con.close()


def _create_view(con: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    """Create a DuckDB view over a Parquet dataset."""
    escaped = str(path.resolve()).replace("'", "''")
    con.execute(f"""CREATE VIEW "{name}" AS SELECT * FROM read_parquet('{escaped}')""")


def _silver_queries() -> dict[str, str]:
    """Return SQL transformations with key de-duplication and null handling."""
    return {
        "dim_customers": """
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM customers
                WHERE customer_id IS NOT NULL
            )
            SELECT customer_id, customer_unique_id,
                   COALESCE(NULLIF(TRIM(customer_city), ''), 'unknown') AS customer_city,
                   COALESCE(NULLIF(TRIM(customer_state), ''), 'unknown') AS customer_state
            FROM ranked WHERE rn = 1
        """,
        "dim_products": """
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY product_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM products WHERE product_id IS NOT NULL
            )
            SELECT product_id,
                   COALESCE(NULLIF(TRIM(product_category_name), ''), 'unknown') AS product_category_name
            FROM ranked WHERE rn = 1
        """,
        "dim_sellers": """
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY seller_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM sellers WHERE seller_id IS NOT NULL
            )
            SELECT seller_id,
                   COALESCE(NULLIF(TRIM(seller_city), ''), 'unknown') AS seller_city,
                   COALESCE(NULLIF(TRIM(seller_state), ''), 'unknown') AS seller_state
            FROM ranked WHERE rn = 1
        """,
        "fct_orders": """
            WITH order_versions AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM orders
                WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
            ),
            item_versions AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id, order_item_id ORDER BY _ingested_at DESC
                ) AS rn
                FROM order_items
                WHERE order_id IS NOT NULL AND order_item_id IS NOT NULL
            ),
            payment_totals AS (
                SELECT order_id, SUM(TRY_CAST(payment_value AS DOUBLE)) AS payment_value
                FROM payments WHERE order_id IS NOT NULL GROUP BY order_id
            ),
            orders_clean AS (
                SELECT order_id, customer_id,
                       TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
                       TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_timestamp,
                       TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_timestamp,
                       order_status
                FROM order_versions WHERE rn = 1
            )
            SELECT o.order_id, TRY_CAST(i.order_item_id AS INTEGER) AS order_item_id,
                   o.customer_id, i.product_id, i.seller_id,
                   o.purchase_timestamp, o.delivered_timestamp,
                   o.estimated_delivery_timestamp,
                   COALESCE(NULLIF(TRIM(o.order_status), ''), 'unknown') AS order_status,
                   GREATEST(COALESCE(TRY_CAST(i.price AS DOUBLE), 0), 0) AS price,
                   GREATEST(COALESCE(TRY_CAST(i.freight_value AS DOUBLE), 0), 0) AS freight_value,
                   GREATEST(COALESCE(TRY_CAST(i.price AS DOUBLE), 0), 0)
                     + GREATEST(COALESCE(TRY_CAST(i.freight_value AS DOUBLE), 0), 0) AS item_amount,
                   COALESCE(p.payment_value, 0) AS order_payment_value,
                   CASE
                     WHEN o.delivered_timestamp IS NOT NULL AND o.purchase_timestamp IS NOT NULL
                     THEN GREATEST(DATE_DIFF('day', o.purchase_timestamp, o.delivered_timestamp), 0)
                     ELSE NULL
                   END AS delivery_days
            FROM orders_clean o
            JOIN item_versions i ON o.order_id = i.order_id AND i.rn = 1
            JOIN dim_customers c ON o.customer_id = c.customer_id
            JOIN dim_products d ON i.product_id = d.product_id
            JOIN dim_sellers s ON i.seller_id = s.seller_id
            LEFT JOIN payment_totals p ON o.order_id = p.order_id
        """,
        "stg_payments": """
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id, payment_sequential ORDER BY _ingested_at DESC
                ) AS rn
                FROM payments WHERE order_id IS NOT NULL
            )
            SELECT order_id,
                   TRY_CAST(payment_sequential AS INTEGER) AS payment_sequential,
                   COALESCE(NULLIF(TRIM(payment_type), ''), 'unknown') AS payment_type,
                   GREATEST(COALESCE(TRY_CAST(payment_value AS DOUBLE), 0), 0) AS payment_value,
                   GREATEST(COALESCE(TRY_CAST(payment_installments AS INTEGER), 0), 0) AS payment_installments
            FROM ranked WHERE rn = 1
        """,
    }
