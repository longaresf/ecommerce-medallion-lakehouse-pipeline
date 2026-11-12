"""Genera un conjunto reproducible de datos transaccionales para desarrollo."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def generate_sample_data(output_dir: Path, seed: int = 42) -> dict[str, Path]:
    """Escribe CSVs relacionales pequeños y reproducibles.

    Args:
        output_dir: Directorio de salida para los CSV.
        seed: Semilla para los valores aleatorios.

    Returns:
        Rutas a los archivos generados, por nombre de entidad.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    customers = pd.DataFrame(
        {
            "customer_id": [f"customer_{i:03d}" for i in range(1, 21)],
            "customer_unique_id": [f"person_{i:03d}" for i in range(1, 21)],
            "customer_city": [f"city_{i % 5}" for i in range(20)],
            "customer_state": ["SP", "RJ", "MG", "PR", "BA"] * 4,
        }
    )
    products = pd.DataFrame(
        {
            "product_id": [f"product_{i:03d}" for i in range(1, 9)],
            "product_category_name": [
                "electronics", "home", "books", "fashion",
                "electronics", "home", "books", "fashion",
            ],
        }
    )
    sellers = pd.DataFrame(
        {
            "seller_id": [f"seller_{i:03d}" for i in range(1, 5)],
            "seller_city": ["sao paulo", "rio", "curitiba", "salvador"],
            "seller_state": ["SP", "RJ", "PR", "BA"],
        }
    )
    order_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    start = pd.Timestamp("2024-01-01")
    for order_num in range(1, 41):
        order_id = f"order_{order_num:04d}"
        purchased = start + pd.Timedelta(days=order_num * 3)
        delivered = purchased + pd.Timedelta(days=int(rng.integers(2, 13)))
        customer_id = f"customer_{(order_num - 1) % len(customers) + 1:03d}"
        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_status": "delivered",
                "order_purchase_timestamp": purchased.isoformat(),
                "order_delivered_customer_date": delivered.isoformat(),
                "order_estimated_delivery_date": (purchased + pd.Timedelta(days=15)).isoformat(),
            }
        )
        item_count = int(rng.integers(1, 4))
        total = 0.0
        for item_num in range(1, item_count + 1):
            price = round(float(rng.uniform(15, 250)), 2)
            freight = round(float(rng.uniform(3, 25)), 2)
            total += price + freight
            item_rows.append(
                {
                    "order_id": order_id,
                    "order_item_id": item_num,
                    "product_id": f"product_{int(rng.integers(1, 9)):03d}",
                    "seller_id": f"seller_{int(rng.integers(1, 5)):03d}",
                    "price": price,
                    "freight_value": freight,
                }
            )
        payment_rows.append(
            {
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": ["credit_card", "boleto", "debit_card"][order_num % 3],
                "payment_installments": int(rng.integers(1, 7)),
                "payment_value": round(total, 2),
            }
        )

    datasets = {
        "customers": customers,
        "orders": pd.DataFrame(order_rows),
        "order_items": pd.DataFrame(item_rows),
        "payments": pd.DataFrame(payment_rows),
        "products": products,
        "sellers": sellers,
    }
    return {
        name: _write_csv(frame, output_dir / f"{name}.csv")
        for name, frame in datasets.items()
    }


def _write_csv(frame: pd.DataFrame, path: Path) -> Path:
    """Escribe un DataFrame a CSV y devuelve su ruta."""
    frame.to_csv(path, index=False)
    return path


def main() -> None:
    """Punto de entrada para generar datos desde la línea de comandos."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    paths = generate_sample_data(args.output_dir, args.seed)
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
