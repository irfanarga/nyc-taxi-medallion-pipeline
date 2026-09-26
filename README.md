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
│  PostgreSQL / BigQuery Staging                         │  │  PostgreSQL Container                                  │
│  • Deduplication, schema validation, data cleansing    │  │  • Data cleansing, filtering, type casting             │
│                                                        │  │                                                        │
│  [GOLD LAYER]                                          │  │  [GOLD LAYER]                                          │
│  BigQuery Data Mart (dbt Core)                         │  │  DuckDB / PostgreSQL (dbt Core)                        │
│  • Star Schema (Fact & Dimension Tables)               │  │  • Local analytical data mart                          │
│  • Partitioned & Clustered for cost optimization       │  │                                                        │
│                                                        │  │                                                        │
│  [PRESENTATION LAYER]                                  │  │  [PRESENTATION LAYER]                                  │
│  Looker Studio (Business BI)                           │  │  Grafana Dashboard (Operational & Real-time)           │
└────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features & Engineering Highlights

* **Decoupled Storage & Compute:** Ingestion scripts process data in-memory (`io.BytesIO`) before pushing to Cloud/Object Storage, eliminating local VM disk bottlenecks.
* **Medallion Architecture:**
  * 🟤 **Bronze Layer:** Raw, immutable Parquet files preserved for compliance and reprocessing.
  * ⚪ **Silver Layer:** Cleaned, deduplicated, and validated data with derived features (e.g., trip duration, fare per mile).
  * 🟡 **Gold Layer:** Modeled into a **Star Schema** (`fact_trips`, `dim_locations`, `dim_payment`) optimized for analytical querying.
* **dbt Data Transformation:** Leverages `dbt Core` for modular SQL transformations, schema testing (`not_null`, `unique`), and incremental models.
* **Cost & Performance Optimization:** BigQuery Gold tables are partitioned by `pickup_month` and clustered by `borough` and `payment_type`.
* **Dual Visualization:**
  * **Looker Studio:** Executive & Business Intelligence dashboard.
  * **Grafana:** Operational monitoring dashboard tracking pipeline throughput and system health.

---

## 🛠️ Tech Stack

| Domain | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow |
| **Ingestion & Processing** | Python, Pandas / PySpark |
| **Cloud Infrastructure (GCP)** | Compute Engine (VM), Google Cloud Storage (GCS), BigQuery |
| **Local Infrastructure (Docker)** | MinIO (Object Storage), PostgreSQL, DuckDB |
| **Data Transformation** | dbt Core (`dbt-bigquery` / `dbt-postgres`) |
| **Monitoring & BI** | Looker Studio, Grafana |
| **Containerization** | Docker & Docker Compose |

---

## 🚀 Quickstart Guide (Local Execution)

Follow these steps to run the complete pipeline on your local machine using Docker without needing a GCP account.

### Prerequisites
* Docker Desktop & Docker Compose installed
* Git

### 1. Clone the Repository
```bash
git clone [https://github.com/your-username/nyc-taxi-medallion-pipeline.git](https://github.com/your-username/nyc-taxi-medallion-pipeline.git)
cd nyc-taxi-medallion-pipeline
```

### 2. Configure Environment Variables
Copy the example environment file and adjust if needed:
```bash
cp .env.example .env
```

### 3. Spin Up Infrastructure
Start all services (Airflow, Postgres, MinIO, Grafana):
```bash
docker-compose up -d
```

### 4. Access Web UIs
* **Apache Airflow:** `http://localhost:8080` (User: `admin` | Pass: `admin`)
* **MinIO Console (S3/GCS Emulation):** `http://localhost:9001` (User: `minioadmin` | Pass: `minioadmin`)
* **Grafana Dashboards:** `http://localhost:3000` (User: `admin` | Pass: `admin`)

---

## 📊 Data Mart Model (Gold Layer)

The Gold Layer follows a **Star Schema** design:

```text
                       ┌────────────────────────┐
                       │     dim_locations      │
                       ├────────────────────────┤
                       │ PK  location_id        │
                       │     borough            │
                       │     zone_name          │
                       └───────────┬────────────┘
                                   │
                                   │ 1:N
                                   ▼
┌───────────────────────┐    ┌───────────────────────────────────┐
│   dim_payment_type    │    │         fact_monthly_trips        │
├───────────────────────┤    ├───────────────────────────────────┤
│ PK  payment_type_id   │1:N │ FK  pickup_location_id            │
│     payment_name      ├───►│ FK  dropoff_location_id           │
└───────────────────────┘    │ FK  payment_type_id               │
                             │     trip_month                    │
                             │     total_trips                   │
                             │     total_passengers              │
                             │     total_distance_miles          │
                             │     total_gross_revenue           │
                             │     avg_tip_percentage            │
                             └───────────────────────────────────┘
```

---

## 🛡️ License & Data Privacy

This repository uses publicly available dataset provided by the **NYC Taxi & Limousine Commission (TLC)**. No confidential or proprietary company data is contained in this project.
