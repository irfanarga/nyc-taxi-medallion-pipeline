import os
import io
import csv
import pandas as pd
from io import StringIO
import boto3
from botocore.client import Config

ENV = os.getenv("ENVIRONMENT", "LOCAL").upper()

# PostgreSQL Config (digunakan jika ENV == LOCAL)
POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "silver_db")
POSTGRES_HOST = "postgres" if os.path.exists("/.dockerenv") else "localhost"
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# GCP BigQuery Config (digunakan jika ENV == GCP)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "silver_dataset")


def psql_insert_copy(table, conn, keys, data_iter):
    """Fungsi percepatan insert data menggunakan PostgreSQL COPY FROM STDIN (Mode LOCAL)."""
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join([f'"{k}"' for k in keys])
        table_name = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)


def init_tracking_table():
    """Membuat tabel metadata penanda file yang sudah diproses di BigQuery atau PostgreSQL."""
    if ENV == "GCP":
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET}.silver_processed_files` (
                file_name STRING,
                processed_at TIMESTAMP,
                records_inserted INT64
            );
        """
        client.query(query).result()
    else:
        # Kode PostgreSQL Lokal
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URI)
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS silver_processed_files (
                    file_name VARCHAR(255) PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    records_inserted INT
                );
            """))


def is_file_processed(file_name: str) -> bool:
    """Mengecek apakah file sudah pernah dimasukkan ke Silver Layer (BigQuery / PostgreSQL)."""
    if ENV == "GCP":
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"""
            SELECT 1 FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.silver_processed_files`
            WHERE file_name = @file_name LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("file_name", "STRING", file_name)]
        )
        results = client.query(query, job_config=job_config).result()
        return results.total_rows > 0
    else:
        # Kode PostgreSQL Lokal
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URI)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM silver_processed_files WHERE file_name = :f"),
                {"f": file_name}
            ).fetchone()
            return result is not None


def log_processed_file(file_name: str, records_inserted: int):
    """Mencatat file yang sukses diproses ke tabel metadata (BigQuery / PostgreSQL)."""
    if ENV == "GCP":
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.silver_processed_files"
        rows_to_insert = [{
            "file_name": file_name,
            "processed_at": pd.Timestamp.now().isoformat(),
            "records_inserted": records_inserted
        }]
        client.insert_rows_json(table_id, rows_to_insert)
    else:
        # Kode PostgreSQL Lokal
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URI)
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO silver_processed_files (file_name, records_inserted) VALUES (:f, :r)"),
                {"f": file_name, "r": records_inserted}
            )


def get_bronze_file_list():
    file_keys = []
    if ENV == "GCP":
        from google.cloud import storage
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="raw/")
        file_keys = [blob.name for blob in blobs if blob.name.endswith(".parquet")]
    else:
        minio_host = "minio" if os.path.exists("/.dockerenv") else "localhost"
        endpoint = f"http://{minio_host}:9000"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            config=Config(signature_version='s3v4')
        )
        bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-bucket")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="raw/")
        if 'Contents' in response:
            file_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(".parquet")]
            
    return file_keys


def download_from_bronze(file_key: str) -> io.BytesIO:
    file_stream = io.BytesIO()
    if ENV == "GCP":
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))
        blob = bucket.blob(file_key)
        blob.download_to_file(file_stream)
    else:
        minio_host = "minio" if os.path.exists("/.dockerenv") else "localhost"
        endpoint = f"http://{minio_host}:9000"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            config=Config(signature_version='s3v4')
        )
        s3_client.download_fileobj(os.getenv("MINIO_BUCKET_NAME", "bronze-bucket"), file_key, file_stream)
        
    file_stream.seek(0)
    return file_stream


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID'])

    df = df[
        (df['passenger_count'] > 0) &
        (df['trip_distance'] > 0) &
        (df['fare_amount'] > 0) &
        (df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime'])
    ]

    df['trip_duration_minutes'] = (
        (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60.0
    ).round(2)

    df = df[(df['trip_duration_minutes'] >= 1.0) & (df['trip_duration_minutes'] <= 1440.0)]
    df = df.drop_duplicates(subset=['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID'])

    cleaned_count = len(df)
    print(f"🧹 Cleansing Selesai: {initial_count:,} baris ➔ {cleaned_count:,} baris")
    return df


def run_silver_transformation():
    print("==================================================")
    print(f"  START INCREMENTAL TRANSFORMATION TO SILVER (Mode: {ENV})")
    print("==================================================")
    
    file_keys = get_bronze_file_list()
    if not file_keys:
        print("⚠️ Tidak ada file Parquet di Bronze Layer.")
        return

    # Inisialisasi tabel tracking berdasarkan mode ENV
    init_tracking_table()

    for key in file_keys:
        file_name = key.split("/")[-1]
        
        if is_file_processed(file_name):
            print(f"⏩ [SKIP] {file_name} sudah pernah diproses ke Silver. Melewati...")
            continue

        print(f"🔄 Memproses file baru: {key}")
        try:
            file_stream = download_from_bronze(key)
            df_raw = pd.read_parquet(file_stream)
            df_cleaned = clean_and_transform(df_raw)

            records_count = len(df_cleaned)

            if ENV == "GCP":
                # Export ke BigQuery
                from google.cloud import bigquery
                bq_client = bigquery.Client(project=GCP_PROJECT_ID)
                table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.silver_yellow_taxi"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job = bq_client.load_table_from_dataframe(df_cleaned, table_id, job_config=job_config)
                job.result()
                print(f"🚀 Sukses memuat data ke BigQuery: {table_id}")
            else:
                # Export ke PostgreSQL Lokal (Kode awal tetap dipertahankan)
                from sqlalchemy import create_engine
                engine = create_engine(DATABASE_URI)
                df_cleaned.to_sql(
                    name='silver_yellow_taxi',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=100000,
                    method=psql_insert_copy
                )
                print(f"🚀 Sukses memuat data ke PostgreSQL: silver_yellow_taxi")
            
            log_processed_file(file_name, records_count)
            print(f"✅ Sukses memasukkan {records_count:,} baris dari {file_name} ke Silver Layer\n")
        except Exception as e:
            print(f"❌ Gagal memproses {key}: {str(e)}")


if __name__ == "__main__":
    run_silver_transformation()