"""Focused integration tests for data quality and rerun safety."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from data.sample_ecommerce_data import generate_sample_data
from pipeline.bronze import ingest_bronze
from pipeline.gold import build_gold
from pipeline.silver import build_silver


@pytest.fixture()
def lakehouse(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Generate an isolated end-to-end lakehouse for a test."""
    source = tmp_path / "source"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    generate_sample_data(source)
    ingest_bronze(source, bronze)
    build_silver(bronze, silver)
    build_gold(bronze, silver, gold)
    return bronze, silver, gold, source


def test_dimensional_model_has_unique_keys_and_valid_foreign_keys(
    lakehouse: tuple[Path, Path, Path, Path],
) -> None:
    """All fact references resolve to one row in the corresponding dimension."""
    _, silver, _, _ = lakehouse
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW f AS SELECT * FROM read_parquet('{silver / 'fct_orders.parquet'}')")
        for dimension in ("customers", "products", "sellers"):
            con.execute(
                f"CREATE VIEW d_{dimension} AS SELECT * FROM read_parquet("
                f"'{silver / f'dim_{dimension}.parquet'}')"
            )
            dimension_key = {
                "customers": "customer_id",
                "products": "product_id",
                "sellers": "seller_id",
            }[dimension]
            assert con.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT {dimension_key}) FROM d_{dimension}"
            ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT (order_id, order_item_id)) FROM f"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM f LEFT JOIN d_customers USING (customer_id) "
            "WHERE d_customers.customer_id IS NULL"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM f LEFT JOIN d_products USING (product_id) "
            "WHERE d_products.product_id IS NULL"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM f LEFT JOIN d_sellers USING (seller_id) "
            "WHERE d_sellers.seller_id IS NULL"
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_reingestion_is_idempotent_and_corrects_existing_keys(
    lakehouse: tuple[Path, Path, Path, Path],
) -> None:
    """Replaying unchanged data or a corrected source does not multiply keys."""
    bronze, _, _, source = lakehouse
    orders_path = source / "orders.csv"
    orders = pd.read_csv(orders_path)
    orders.loc[orders["order_id"] == "order_0001", "order_status"] = "approved"
    orders.to_csv(orders_path, index=False)
    ingest_bronze(source, bronze)
    ingest_bronze(source, bronze)
    con = duckdb.connect()
    try:
        path = str(bronze / "orders.parquet").replace("\\", "/").replace("'", "''")
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()[0]
        distinct_count = con.execute(
            f"SELECT COUNT(DISTINCT order_id) FROM read_parquet('{path}')"
        ).fetchone()[0]
        assert count == distinct_count == 40
        corrected_status = con.execute(
            f"SELECT order_status FROM read_parquet('{path}') "
            "WHERE order_id = 'order_0001'"
        ).fetchone()[0]
        assert corrected_status == "approved"
    finally:
        con.close()


def test_bronze_ingests_json_files(tmp_path: Path) -> None:
    """Olist-style JSON arrays are mapped to the same canonical dataset."""
    source = tmp_path / "source"
    bronze = tmp_path / "bronze"
    source.mkdir()
    (source / "olist_customers_dataset.json").write_text(
        json.dumps(
            [
                {
                    "customer_id": "customer_json",
                    "customer_unique_id": "person_json",
                    "customer_city": "city",
                    "customer_state": "SP",
                }
            ]
        ),
        encoding="utf-8",
    )
    ingest_bronze(source, bronze)
    con = duckdb.connect()
    try:
        path = str(bronze / "customers.parquet").replace("\\", "/").replace("'", "''")
        customer_id = con.execute(
            f"SELECT customer_id FROM read_parquet('{path}')"
        ).fetchone()[0]
        assert customer_id == "customer_json"
    finally:
        con.close()


def test_gold_metrics_are_nonnegative_and_not_null(
    lakehouse: tuple[Path, Path, Path, Path],
) -> None:
    """Business metrics contain no negative or null values for generated data."""
    _, _, gold, _ = lakehouse
    con = duckdb.connect()
    try:
        path = str(gold / "monthly_sales_by_category").replace("\\", "/").replace("'", "''")
        result = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}.parquet') "
            "WHERE gross_sales IS NULL OR gross_sales < 0 "
            "OR average_item_amount IS NULL OR average_item_amount < 0 "
            "OR order_count IS NULL OR order_count < 0"
        ).fetchone()[0]
        assert result == 0
        delivery_path = str(gold / "delivery_metrics").replace("\\", "/").replace("'", "''")
        invalid_delivery = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{delivery_path}.parquet') "
            "WHERE order_count IS NULL OR order_count < 0 "
            "OR late_order_count IS NULL OR late_order_count < 0 "
            "OR average_delivery_days IS NULL OR average_delivery_days < 0"
        ).fetchone()[0]
        assert invalid_delivery == 0
        payments_path = str(gold / "payment_distribution").replace("\\", "/").replace("'", "''")
        invalid_payments = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{payments_path}.parquet') "
            "WHERE payment_count IS NULL OR payment_count < 0 "
            "OR payment_value IS NULL OR payment_value < 0 "
            "OR average_payment_value IS NULL OR average_payment_value < 0"
        ).fetchone()[0]
        assert invalid_payments == 0
        retention_path = str(gold / "customer_retention").replace("\\", "/").replace("'", "''")
        invalid_retention = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{retention_path}.parquet') "
            "WHERE active_customers IS NULL OR active_customers < 0 "
            "OR returning_customers IS NULL OR returning_customers < 0 "
            "OR returning_customers > active_customers"
        ).fetchone()[0]
        assert invalid_retention == 0
    finally:
        con.close()
