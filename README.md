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
* **Automated Time-Window Ingestion:** Dynamically fetches NYC Yellow Taxi data from **January 2025 to the latest available month**, with graceful fallback handling for unreleased monthly partitions (HTTP 404).
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
| **Ingestion & Processing** | Python, Pandas, PyArrow |
| **Cloud Infrastructure (GCP)** | Compute Engine (VM), Google Cloud Storage (GCS), BigQuery |
| **Local Infrastructure (Docker)** | MinIO (Object Storage), PostgreSQL, DuckDB, Apache Kafka |
| **Data Transformation** | dbt Core (`dbt-bigquery` / `dbt-postgres`) |
| **Monitoring & BI** | Looker Studio, Grafana |
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
wget [https://go.dev/dl/go1.22.5.linux-amd64.tar.gz](https://go.dev/dl/go1.22.5.linux-amd64.tar.gz)
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

#### 2. Build MinIO & MC Binary from Official GitHub Source
```bash
go install [github.com/minio/minio@latest](https://github.com/minio/minio@latest)
go install [github.com/minio/mc@latest](https://github.com/minio/mc@latest)
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
git clone [https://github.com/your-username/nyc-taxi-medallion-pipeline.git](https://github.com/your-username/nyc-taxi-medallion-pipeline.git)
cd nyc-taxi-medallion-pipeline
cp .env.example .env
```

#### 2. Spin Up Docker Pipeline Infrastructure
```bash
# Set permissions for Airflow DAGs & Scripts
chmod -R 777 dags/ scripts/

# Start all containers
docker compose down
docker compose up -d
```

#### 3. Trigger Bronze Ingestion via Airflow UI
1. Open Apache Airflow UI at `http://<YOUR-VM-IP>:8080` (Default Credentials: `admin` / `admin`).
2. Locate the DAG **`nyc_taxi_bronze_ingestion`**.
3. Toggle the DAG switch to **ON**, then click **Trigger DAG (▶)**.
4. Monitor execution logs in real-time as raw Parquet files are streamed directly into **MinIO** (`s3://bronze-bucket/raw/`) or **GCS** (`gs://<your-bucket>/raw/`).

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