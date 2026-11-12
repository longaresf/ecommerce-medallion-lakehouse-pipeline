# E-commerce Medallion Lakehouse Pipeline 📊

[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active-brightgreen.svg)](#)
[![Tests](https://github.com/longaresf/ecommerce-medallion-lakehouse-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/longaresf/ecommerce-medallion-lakehouse-pipeline/actions/workflows/tests.yml)

A robust, production-ready data lakehouse and analytics pipeline for e-commerce transactions. This repository demonstrates an end-to-end architecture implementing **Medallion design patterns (Bronze, Silver, Gold)** using Python, DuckDB, Parquet, and **Dagster** orchestration, paired with an interactive Streamlit dashboard.

## 📋 Project Overview

Building reliable data platforms requires systematic ingestion, strict relational modeling, and robust orchestration. This project implements an enterprise-grade lakehouse pipeline that ingests raw transactional sources idempotently, transforms them into a clean dimensional model (Star Schema), aggregates business metrics, and automates execution workflows with Dagster.

---

## ✨ Core Pipeline Architecture

```text
CSV / JSON (Olist or Synthetic)
       │
       ▼
Bronze: Raw Parquet + source metadata + idempotent upserts
       │
       ▼
Silver: Cleaned dimensions & fact tables powered by DuckDB SQL
       │
       ▼
Gold: Business aggregations (sales, logistics, retention, payments)
       │
       ├── Streamlit Dashboard
       └── Downstream Consumers

    Bronze Layer (pipeline/bronze.py): Ingests raw CSV/JSON files, appends ingestion timestamps and source metadata, and normalizes entities into Parquet files with idempotent upserts based on natural keys.

    Silver Layer (pipeline/silver.py): Cleans text/numeric fields, handles missing or invalid dates gracefully, and deduplicates records. Builds a dimensional model consisting of dim_customers, dim_products, dim_sellers, and fct_orders (at the order-item grain) alongside staged payment tables.

    Gold Layer (pipeline/gold.py): Constructs analytical aggregation tables (monthly_sales_by_category, delivery_metrics, customer_retention, payment_distribution) ready for consumption.

    Orchestration (orchestration/defs.py): Defines modular Dagster Assets that automate the complete sequential data flow from Bronze through Gold.

    Interactive Dashboard (app.py): Streamlit web application providing visual exploration of key e-commerce metrics and lakehouse tables.

    Automated Testing (tests/test_pipeline_integrity.py): Unit tests validating primary key uniqueness, foreign key integrity, idempotency, and non-negative aggregate metrics using pytest.

🛠️ Tech Stack

    Language: Python 3.11+

    Data Engine & Storage: DuckDB, Apache Parquet, PyArrow

    Data Engineering & Orchestration: Dagster, Pandas, NumPy

    Interactive UI & Visualization: Streamlit, Plotly

    Testing & Containerization: Pytest, Docker, Docker Compose

📁 Repository Structure
Plaintext

├── data/                       # Kaggle dataset guide and synthetic data generator script
├── pipeline/                   # Modular lakehouse layers (bronze, silver, gold)
├── orchestration/              # Dagster assets and job definitions
├── tests/                      # End-to-end data integrity and pipeline unit tests
├── app.py                      # Interactive Streamlit dashboard
├── Dockerfile                  # Container image configuration
├── docker-compose.yml          # Local multi-service orchestration setup
├── requirements.txt            # Project Python dependencies
└── README.md                   # Project documentation

🚀 Getting Started
1. Clone the Repository
Bash

git clone [https://github.com/longaresf/ecommerce-medallion-lakehouse-pipeline.git](https://github.com/longaresf/ecommerce-medallion-lakehouse-pipeline.git)
cd ecommerce-medallion-lakehouse-pipeline

2. Configure Virtual Environment & Install Dependencies
Bash

python -m venv .venv
# Linux / macOS:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

3. Generate Sample Data & Run Dagster
Bash

python -m data.sample_ecommerce_data --output-dir data/generated
dagster dev -m orchestration.defs

Open http://localhost:3000 to materialize assets and execute the pipeline.
4. Run Unit Tests
Bash

pytest -q

📊 Running the Interactive Dashboard

Launch the local Streamlit application to explore the processed gold tables and KPIs:
Bash

streamlit run app.py

🐋 Running via Docker Compose

Generate the sample data first, then spin up the containerized environment:
Bash

python -m data.sample_ecommerce_data --output-dir data/generated
docker compose up --build

💡 Why Lakehouse Architecture & Dagster Matter

Traditional data architectures often struggle with maintaining data quality across raw data silos and downstream reporting. Implementing a Medallion architecture (Bronze for raw auditability, Silver for conformed dimensional modeling, and Gold for business-ready aggregates) ensures high data reliability, traceability, and performance. Combined with code-defined orchestration via Dagster, data pipelines become fully reproducible, testable, and robust against pipeline failures.
✒️ Author

Francisco Longares — Data Scientist & AI Solutions Developer — longaresf