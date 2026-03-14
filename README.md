# Olist E-Commerce Data Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Tables](https://img.shields.io/badge/Delta%20Tables-003366?style=flat)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)

## Overview
An end-to-end declarative data pipeline built on Databricks to ingest and process 100K+ e-commerce records from the Olist Brazilian public dataset, covering orders, payments, sellers, and customer reviews.

## Architecture
```
Raw CSV/Parquet → Bronze (Raw) → Silver (Cleaned/Joined) → Gold (Aggregated)
```

## Tech Stack
- Databricks & PySpark
- Spark Declarative Pipelines (DLT)
- Delta Tables
- Databricks Asset Bundles (DAB)
- GitHub CI/CD

## Medallion Architecture
| Layer | Description |
|-------|-------------|
| Bronze | Raw ingested CSV/Parquet files as-is |
| Silver | Cleaned, joined and standardized tables |
| Gold | Aggregated KPIs and analytical datasets |

## Pipeline Features
1. Declarative pipeline with DLT expectations
2. Data quality constraints and schema enforcement
3. Incremental processing
4. Infrastructure as Code using DAB with YAML job config
5. Version-controlled deployment via GitHub CI/CD

## Project Structure
```
├── pipelines/       # DLT pipeline definitions
├── notebooks/       # PySpark transformation notebooks
├── bundles/         # DAB configuration (YAML)
├── data/            # Sample data files
└── README.md
```

## How to Run
1. Clone the repo: `git clone https://github.com/sivaprakash1352k/real_world_project`
2. Configure Databricks CLI: `databricks configure`
3. Deploy using DAB: `databricks bundle deploy`
4. Run the pipeline: `databricks bundle run`
