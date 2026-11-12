# E-commerce Medallion Lakehouse Pipeline

Pipeline analítico de referencia para transacciones de e-commerce. Implementa capas **Bronze, Silver y Gold** con Python, DuckDB, Parquet y Dagster, y expone agregados con Streamlit. El repositorio incluye datos sintéticos reproducibles; el dataset de Olist se descarga por separado desde Kaggle.

## Arquitectura

```text
CSV / JSON (Olist o sintéticos)
        │
        ▼
Bronze: Parquet crudo + claves de origen + upsert idempotente
        │
        ▼
Silver: dimensiones y hechos depurados con DuckDB SQL
        │
        ▼
Gold: ventas, logística, retención y pagos
        │
        ├── Streamlit
        └── consumidores analíticos
```

### Capas y modelo dimensional

- **Bronze** conserva las columnas de entrada y agrega `_source_file` y `_ingested_at`. Los CSV/JSON se normalizan con DuckDB; cada entidad se consolida en un Parquet. Las claves naturales (incluidas las compuestas de ítems y pagos) se usan para hacer upsert y permitir reejecuciones y correcciones sin duplicar registros.
- **Silver** limpia textos y números, trata fechas inválidas con conversiones tolerantes y deduplica versiones mediante `ROW_NUMBER()`. `dim_customers`, `dim_products` y `dim_sellers` tienen claves de negocio estables. `fct_orders` tiene grano de **línea de pedido** (`order_id`, `order_item_id`) y referencias válidas a las tres dimensiones. `stg_payments` conserva el grano de pago para no duplicar importes al unirlo a líneas.
- **Gold** genera `monthly_sales_by_category`, `delivery_metrics`, `customer_retention` y `payment_distribution`, cada una con granularidad explícita en SQL. Los agregados se materializan en Parquet.

El SQL usa CTEs y funciones de ventana en las deduplicaciones y el cálculo de retención. DuckDB ejecuta lectura, transformaciones y agregaciones; Pandas se utiliza para preparar datos sintéticos y presentar resultados en el dashboard.

## Requisitos

- Python 3.11 recomendado (dependencias fijadas en `requirements.txt`).
- Para contenedores: Docker y Docker Compose.
- Opcional: cuenta de Kaggle para descargar Olist. Consulta [data/README.md](data/README.md).

## Inicio rápido local

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m data.sample_ecommerce_data --output-dir data/generated
dagster dev -m orchestration.defs
```

Abre `http://localhost:3000`, materializa los assets `bronze_layer`, `silver_layer` y `gold_layer` (o ejecuta el job desde la interfaz) y después inicia el dashboard en otra terminal:

```bash
streamlit run app.py
```

El dashboard se abre en `http://localhost:8501`. Configura `ECOMMERCE_SOURCE_DIR` para cambiar el directorio de CSV/JSON y `ECOMMERCE_STORAGE_DIR` para cambiar el directorio de almacenamiento (por defecto `data/generated` y `storage`).

## Docker Compose

Genera primero los archivos de ejemplo:

```bash
python -m data.sample_ecommerce_data --output-dir data/generated
docker compose up --build
```

Dagster queda disponible en `http://localhost:3000` y Streamlit en `http://localhost:8501`. Los directorios locales `data/` y `storage/` se montan como volúmenes.

## Pruebas

```bash
pytest -q
```

Las pruebas construyen el pipeline con archivos temporales y verifican unicidad de la clave del hecho, integridad referencial, idempotencia de la ingesta y métricas Gold no negativas/no nulas donde corresponde.

## Estructura

```text
data/             Guía de Kaggle y generador sintético
pipeline/         Ingesta Bronze y transformaciones Silver/Gold
orchestration/    Assets y dependencias Dagster
tests/            Pruebas de integridad end-to-end
app.py            Dashboard Streamlit
storage/          Parquet materializado (ignorado por Git)
```

## Notas operativas

- La ejecución es local y no requiere credenciales de nube.
- El pipeline espera los campos estándar de Olist o equivalentes, con identificadores de entidades y pedidos compatibles.
- Bronze usa claves naturales para reemplazar la versión previamente ingerida de una entidad. Para auditoría histórica inmutable, retén las fuentes originales fuera de `storage/`.
- No se incluyen datos de Olist, credenciales ni artefactos generados en el control de versiones.
