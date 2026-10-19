# 🚖 End-to-End NYC Taxi Data Pipeline (Medallion Architecture)

![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue)
![Architecture](https://img.shields.io/badge/Architecture-Medallion_(Bronze_Silver_Gold)-green)
![GCP](https://img.shields.io/badge/Cloud-Google_Cloud_Platform-yellow)
![Docker](https://img.shields.io/badge/Container-Docker_Compose-blue)
![dbt](https://img.shields.io/badge/Transformation-dbt_Core-orange)

An enterprise-grade, scalable **End-to-End Data Engineering Pipeline** built using the **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**. 

This project processes millions of NYC TLC Taxi Trip records, demonstrating both **Batch Processing** and **Stream-Ready Infrastructure**. It is uniquely engineered with a **Dual-Deployment Strategy**, allowing it to run seamlessly on **Google Cloud Platform (GCP)** or **100% locally using Docker** (zero cloud cost).

---

## 🏛️ System Architecture

```text
                            ┌────────────────────────────────────────────────────────┐
                            │               DATA SOURCE (NYC TLC API)                │
                            └───────────────────────────┬────────────────────────────┘
                                                        │
                                                        ▼
                            ┌────────────────────────────────────────────────────────┐
                            │           ORCHESTRATION & INGESTION ENGINE             │
                            │               Apache Airflow / Python                  │
                            └───────────┬────────────────────────────────┬───────────┘
                                        │                                │
                                        ▼                                ▼
┌────────────────────────────────────────────────────────┐  ┌────────────────────────────────────────────────────────┐
│                   CLOUD ENVIRONMENT                    │  │                    LOCAL DOCKER MODE                   │
│                                                        │  │                                                        │
│  [BRONZE LAYER]                                        │  │  [BRONZE LAYER]                                        │
│  Google Cloud Storage (GCS)                            │  │  MinIO Object Storage (S3 API)                         │
│  • Raw Parquet files stored directly in-memory         │  │  • Local Parquet storage emulation                     │
│                                                        │  │                                                        │
│  [SILVER LAYER]                                        │  │  [SILVER LAYER]                                        │
│  PostgreSQL / BigQuery Staging                         │  │  PostgreSQL Container (`silver_db`)                    │
│  • Bulk COPY FROM STDIN optimization (10x faster)      │  │  • Data cleansing, filtering, type casting             │
│  • Metadata tracking (`silver_processed_files`)        │  │  • Incremental metadata tracking                       │
│                                                        │  │                                                        │
│  [GOLD LAYER]                                          │  │  [GOLD LAYER]                                          │
│  BigQuery Data Mart (dbt Core)                         │  │  PostgreSQL / DuckDB (dbt Core)                        │
│  • Star Schema (`fact_monthly_trips`, `dim_payment`)   │  │  • Local analytical data mart (`gold` schema)          │
│                                                        │  │                                                        │
│  [PRESENTATION LAYER]                                  │  │  [PRESENTATION LAYER]                                  │
│  Looker Studio (Business BI)                           │  │  Grafana Dashboard (Operational & Real-time)           │
└────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────┘