"""Ingesta idempotente de archivos transaccionales a archivos Parquet Bronze."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

DATASET_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "payments": ("order_id", "payment_sequential"),
    "products": ("product_id",),
    "sellers": ("seller_id",),
}
_ALIASES = {
    "customer": "customers",
    "customers": "customers",
    "order": "orders",
    "orders": "orders",
    "order_item": "order_items",
    "order_items": "order_items",
    "items": "order_items",
    "order_payment": "payments",
    "order_payments": "payments",
    "payment": "payments",
    "payments": "payments",
    "product": "products",
    "products": "products",
    "seller": "sellers",
    "sellers": "sellers",
}


def ingest_bronze(source_dir: Path, bronze_dir: Path) -> dict[str, Path]:
    """Ingiere CSV/JSON y hace upsert por clave natural en Parquet.

    Un registro reingerido con la misma clave reemplaza a su versión anterior,
    lo que permite incorporar correcciones retroactivas sin duplicar filas.

    Args:
        source_dir: Directorio con archivos CSV y JSON de origen.
        bronze_dir: Directorio donde se escriben los Parquet Bronze.

    Returns:
        Rutas a los datasets escritos o ya existentes.

    Raises:
        FileNotFoundError: Si no existe el directorio de origen.
        ValueError: Si un archivo no se puede asociar a un dataset conocido.
    """
    if not source_dir.is_dir():
        raise FileNotFoundError(f"No existe el directorio de entrada: {source_dir}")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    inputs = sorted(
        path
        for path in source_dir.iterdir()
        if path.is_file() and path.suffix.lower() in {".csv", ".json", ".jsonl"}
    )
    grouped: dict[str, list[Path]] = {}
    for path in inputs:
        dataset = _dataset_name(path.stem)
        if dataset is None:
            raise ValueError(f"No se reconoce el dataset del archivo: {path.name}")
        grouped.setdefault(dataset, []).append(path)

    output_paths: dict[str, Path] = {}
    for dataset, files in grouped.items():
        frames = [_read_source(path) for path in files]
        attributed_frames = []
        for path, frame in zip(files, frames):
            frame = frame.copy()
            frame["_source_file"] = path.name
            frame["_ingested_at"] = datetime.now(timezone.utc)
            attributed_frames.append(frame)
        incoming = pd.concat(attributed_frames, ignore_index=True, sort=False)
        target = bronze_dir / f"{dataset}.parquet"
        _upsert_parquet(incoming, target, DATASET_KEYS[dataset])
        output_paths[dataset] = target

    for dataset in DATASET_KEYS:
        target = bronze_dir / f"{dataset}.parquet"
        if target.exists():
            output_paths.setdefault(dataset, target)
    return output_paths


def _dataset_name(filename: str) -> str | None:
    """Map common Olist and synthetic filenames to a canonical dataset name."""
    name = filename.lower()
    if name.startswith("olist_"):
        name = name[len("olist_") :]
    for suffix in ("_dataset", "_data"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return _ALIASES.get(name)


def _read_source(path: Path) -> pd.DataFrame:
    """Read CSV or newline-delimited/array JSON via DuckDB."""
    con = duckdb.connect()
    try:
        if path.suffix.lower() == ".csv":
            return con.execute(
                "SELECT * FROM read_csv_auto(?, header = true, sample_size = -1)",
                [str(path.resolve())],
            ).df()
        return con.execute("SELECT * FROM read_json_auto(?)", [str(path.resolve())]).df()
    finally:
        con.close()


def _upsert_parquet(incoming: pd.DataFrame, target: Path, keys: tuple[str, ...]) -> None:
    """Merge incoming rows with an existing Parquet snapshot deterministically."""
    missing = [key for key in keys if key not in incoming.columns]
    if missing:
        raise ValueError(f"Faltan claves {missing} en el dataset {target.stem}")
    con = duckdb.connect()
    try:
        con.register("incoming_rows", incoming)
        if target.exists():
            escaped = str(target.resolve()).replace("'", "''")
            con.execute(f"CREATE TEMP TABLE merged AS SELECT * FROM read_parquet('{escaped}')")
            con.execute("INSERT INTO merged BY NAME SELECT * FROM incoming_rows")
        else:
            con.execute("CREATE TEMP TABLE merged AS SELECT * FROM incoming_rows")
        order = ", ".join(f'"{key}"' for key in keys)
        con.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (_row_number)
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {order}
                        ORDER BY _ingested_at DESC, _source_file DESC
                    ) AS _row_number
                    FROM merged
                )
                WHERE _row_number = 1
            )
            TO '{str(target.resolve()).replace("'", "''")}' (FORMAT PARQUET)
            """
        )
    finally:
        con.close()
