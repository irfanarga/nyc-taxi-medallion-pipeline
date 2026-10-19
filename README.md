# 🚖 End-to-End NYC Taxi Data Pipeline (Medallion Architecture)

![Data Engineering](https://img.shields.io/badge/Domain-Data_Engineering-blue)
![Architecture](https://img.shields.io/badge/Architecture-Medallion_(Bronze_Silver_Gold)-green)
![GCP](https://img.shields.io/badge/Cloud-Google_Cloud_Platform-yellow)
![Docker](https://img.shields.io/badge/Container-Docker_Compose-blue)
![dbt](https://img.shields.io/badge/Transformation-dbt_Core-orange)

An enterprise-grade, scalable **End-to-End Data Engineering Pipeline** built using the **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**. 

This project processes millions of NYC TLC Taxi Trip records (from **January 2025 to the present**), demonstrating both **Batch Processing** and **Stream-Ready Infrastructure**. It is uniquely engineered with a **Dual-Deployment Strategy**, allowing it to run seamlessly on **Google Cloud Platform (GCP)** or **100% locally using Docker** (zero cloud cost) via a single environment switch.

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
│  BigQuery Staging (`silver_dataset`)                   │  │  PostgreSQL Container (`silver_db`)                    │
│  • Deduplication, schema validation, data cleansing    │  │  • Data cleansing, filtering, type casting             │
│  • Native Python BigQuery Load Jobs                    │  │  • Fast bulk COPY FROM STDIN optimization              │
│                                                        │  │  • Incremental metadata tracking (`silver_processed`)  │
│                                                        │  │                                                        │
│  [GOLD LAYER]                                          │  │  [GOLD LAYER]                                          │
│  BigQuery Data Mart (`gold` schema)                    │  │  PostgreSQL Data Mart (`gold` schema)                  │
│  • Star Schema (Fact & Dimension Tables via dbt)       │  │  • Star Schema built using dbt-postgres                │
│  • Partitioned & Clustered for cost optimization       │  │                                                        │
│                                                        │  │                                                        │
│  [PRESENTATION LAYER]                                  │  │  [PRESENTATION LAYER]                                  │
│  Looker Studio (Executive & Business BI)               │  │  Grafana Dashboard (Operational & System Health)       │
└────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features & Engineering Highlights

- **Decoupled Storage & Compute:** Ingestion scripts process raw Parquet files in-memory (`io.BytesIO`) before streaming to Object Storage (MinIO/GCS), eliminating local VM disk I/O bottlenecks.
- **Automated Time-Window Ingestion:** Dynamically calculates monthly target URLs from **January 2025 to the current month**, with graceful fallback handling for unreleased TLC monthly partitions (HTTP 404).
- **Incremental & Idempotent Execution:**
  - **Bronze:** Checks object existence in bucket storage before downloading to skip existing raw files.
  - **Silver:** Tracks processed files using `silver_processed_files` metadata table across both PostgreSQL and BigQuery to guarantee idempotency.
- **High-Performance Bulk Loading:** Employs PostgreSQL native `COPY FROM STDIN` protocol (`psql_insert_copy`) in local mode, reducing loading times for millions of records from ~20 minutes to <2 minutes.
- **Cross-Database dbt Abstraction (Jinja SQL):** Uses native Jinja conditionals (`{% if target.name == 'gcp' %}`) within dbt staging and mart models to seamlessly adapt SQL syntax between BigQuery (ANSI SQL, `INT64`, `NUMERIC`) and PostgreSQL (`INT`, `BIGINT`, `::` casting).
- **Medallion Architecture:**
  - 🟤 **Bronze Layer:** Raw, immutable Parquet files preserved for compliance and reprocessing.
  - ⚪ **Silver Layer:** Cleaned, deduplicated, and validated records enriched with derived metrics (e.g., trip duration in minutes).
  - 🟡 **Gold Layer:** Modeled into a **Star Schema** (`fact_monthly_trips`, `dim_payment_type`) optimized for fast analytical queries.
- **GCP Security & IAM Hardening:** Configured Service Account credentials (`config/gcp_key.json` with `chmod 644` permissions) and resolved Organization Policy constraints (`iam.disableServiceAccountKeyCreation`).
- **Dual Dashboard Ecosystem:**
  - **Grafana (Local):** Real-time operational dashboard monitoring file ingestion logs, row counts, and local revenue aggregates.
  - **Looker Studio (GCP):** Executive BI dashboard connected directly to BigQuery Gold data marts.

---

## 🛠️ Tech Stack

| Domain | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow (Docker, LocalExecutor) |
| **Ingestion & Processing** | Python, Pandas, PyArrow, SQLAlchemy, Boto3, Google Cloud Storage SDK |
| **Cloud Infrastructure (GCP)** | Compute Engine (VM), Google Cloud Storage (GCS), BigQuery |
| **Local Infrastructure (Docker)** | MinIO (S3 Emulation), PostgreSQL (`silver_db`), Grafana |
| **Data Transformation** | dbt Core (`dbt-bigquery` / `dbt-postgres`) |
| **Monitoring & BI** | Looker Studio (Business BI), Grafana (Operational Monitoring) |
| **Containerization** | Docker & Docker Compose |

---

## 🚀 Quickstart Guide

### ⚙️ Special Setup: Native MinIO Compilation (For VM / GCP Rate-Limit Workarounds)

If running on a cloud VM (GCP/AWS) where Docker Hub registry rate limits or corrupt image pulls occur, compile the official `minio` and `mc` binaries natively using Go toolchain before running Docker Compose:

#### 1. Clean Environment & Install Go Toolchain
```bash
# Delete corrupt or existing local bin folder
rm -rf ./bin

# Download & install Go 1.22
wget https://go.dev/dl/go1.22.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

#### 2. Build MinIO & MC Binary from Official GitHub Source
```bash
go install github.com/minio/minio@latest
go install github.com/minio/mc@latest
```

#### 3. Move Compiled Binaries to Project Directory
```bash
mkdir -p ./bin
cp ~/go/bin/minio ./bin/minio
cp ~/go/bin/mc ./bin/mc
chmod +x ./bin/minio ./bin/mc
```

#### 4. Verify Binary Versions
```bash
./bin/minio --version
```

---

### 🐳 Execution Steps

#### 1. Clone the Repository & Configure Environment
```bash
git clone https://github.com/your-username/nyc-taxi-medallion-pipeline.git
cd nyc-taxi-medallion-pipeline
cp .env.example .env
```

Edit your `.env` file to select the active environment (`LOCAL` vs `GCP`):

```env
# Set ENVIRONMENT to 'GCP' or 'LOCAL'
ENVIRONMENT=GCP
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=nyc-taxi-bronze-your-bucket
BIGQUERY_DATASET=silver_dataset
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp_key.json
```

#### 2. Configure Service Account Permissions
Place your GCP Service Account JSON key inside `config/gcp_key.json` and set read permissions:
```bash
chmod 644 config/gcp_key.json
```

#### 3. Spin Up Docker Pipeline Infrastructure
```bash
# Set permissions for Airflow DAGs, Scripts, and dbt project
chmod -R 777 dags/ scripts/ dbt_project/

# Force container recreation to load updated .env configuration
docker compose down
docker compose up -d
```

#### 4. Execute Medallion Pipeline Tasks

##### Option A: Airflow Web UI
1. Open Apache Airflow UI at `http://<YOUR-VM-IP>:8080` (Default Credentials: `admin` / `admin`).
2. Locate the DAG **`nyc_taxi_medallion_pipeline`**.
3. Toggle the DAG switch to **ON**, then click **Trigger DAG (▶)**.

##### Option B: Command Line Interface (CLI)
```bash
# Step 1: Ingest to Bronze (MinIO / GCS)
docker exec -it airflow python /opt/airflow/scripts/ingest_to_bronze.py

# Step 2: Cleanse & Fast Bulk Load to Silver (PostgreSQL / BigQuery)
docker exec -it airflow python /opt/airflow/scripts/transform_to_silver.py

# Step 3: Run Gold Layer Star Schema Transformations (dbt Core)
docker exec -it airflow python3 -m dbt.cli.main run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

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

This repository uses a publicly available dataset provided by the **NYC Taxi & Limousine Commission (TLC)**. No confidential or proprietary company data is contained in this project.