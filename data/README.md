# Datos de entrada

## Dataset público de Olist

1. Crea una cuenta en [Kaggle](https://www.kaggle.com/) y acepta las condiciones de uso del dataset **Brazilian E-Commerce Public Dataset by Olist**.
2. Descarga el archivo ZIP desde la página del dataset de Olist.
3. Extrae los CSV en una carpeta local, por ejemplo `data/olist/`. Este proyecto reconoce nombres de archivo como `olist_customers_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_products_dataset.csv` y `olist_sellers_dataset.csv`.
4. Genera datos de ejemplo (opcional) con `python -m data.sample_ecommerce_data --output-dir data/generated`. Para procesar Olist en su lugar, configura `ECOMMERCE_SOURCE_DIR=data/olist`.
5. Ejecuta el pipeline desde la raíz del proyecto: `dagster dev -m orchestration.defs`.

No se distribuyen datos de Olist en este repositorio. El usuario es responsable de aceptar y cumplir las condiciones de Kaggle y Olist.

Los archivos CSV y JSON se normalizan a Parquet en `storage/bronze/`. No modifiques manualmente archivos de `storage/`; vuelve a ejecutar la ingesta para incorporar correcciones.
