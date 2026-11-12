"""Assets Dagster para ejecutar secuencialmente Bronze, Silver y Gold."""

import os
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset

from pipeline.bronze import ingest_bronze
from pipeline.gold import build_gold
from pipeline.silver import build_silver


def _paths() -> tuple[Path, Path, Path, Path]:
    """Resolve source and storage directories from environment overrides."""
    source = Path(os.getenv("ECOMMERCE_SOURCE_DIR", "data/generated"))
    storage = Path(os.getenv("ECOMMERCE_STORAGE_DIR", "storage"))
    return source, storage / "bronze", storage / "silver", storage / "gold"


@asset
def bronze_layer(context: AssetExecutionContext) -> dict[str, str]:
    """Ingest source files into the idempotent Bronze layer."""
    source, bronze, _, _ = _paths()
    outputs = ingest_bronze(source, bronze)
    context.log.info("Bronze datasets available: %s", sorted(outputs))
    return {name: str(path) for name, path in outputs.items()}


@asset
def silver_layer(
    context: AssetExecutionContext, bronze_layer: dict[str, str]
) -> dict[str, str]:
    """Build the dimensional model and cleansed payment records."""
    _, bronze, silver, _ = _paths()
    context.log.info("Building Silver from %d Bronze datasets.", len(bronze_layer))
    outputs = build_silver(bronze, silver)
    context.log.info("Silver tables built: %s", sorted(outputs))
    return {name: str(path) for name, path in outputs.items()}


@asset
def gold_layer(
    context: AssetExecutionContext,
    bronze_layer: dict[str, str],
    silver_layer: dict[str, str],
) -> dict[str, str]:
    """Build business aggregates consumed by the dashboard."""
    _, bronze, silver, gold = _paths()
    context.log.info(
        "Building Gold from %d Bronze and %d Silver datasets.",
        len(bronze_layer),
        len(silver_layer),
    )
    outputs = build_gold(bronze, silver, gold)
    context.log.info("Gold tables built: %s", sorted(outputs))
    return {name: str(path) for name, path in outputs.items()}


defs = Definitions(assets=[bronze_layer, silver_layer, gold_layer])
