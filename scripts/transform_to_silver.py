import os
import io
import csv
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config

ENV = os.getenv("ENVIRONMENT", "LOCAL").upper()

POSTGRES_USER = os.getenv("POSTGRES_USER", "db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "db_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "silver_db")
POSTGRES_HOST = "postgres" if os.path.exists("/.dockerenv") else "localhost"
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def psql_insert_copy(table, conn, keys, data_iter):
    """Fungsi percepatan insert data menggunakan PostgreSQL COPY FROM STDIN."""
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

def init_tracking_table(engine):
    """Membuat tabel metadata penanda file yang sudah diproses."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_processed_files (
                file_name VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                records_inserted INT
            );
        """))

def is_file_processed(engine, file_name: str) -> bool:
    """Mengecek apakah file sudah pernah dimasukkan ke Silver Layer."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM silver_processed_files WHERE file_name = :f"),
            {"f": file_name}
        ).fetchone()
        return result is not None

def log_processed_file(engine, file_name: str, records_inserted: int):
    """Mencatat file yang sukses diproses ke tabel metadata."""
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

    engine = create_engine(DATABASE_URI)
    init_tracking_table(engine)

    for key in file_keys:
        file_name = key.split("/")[-1]
        
        if is_file_processed(engine, file_name):
            print(f"⏩ [SKIP] {file_name} sudah pernah diproses ke Silver. Melewati...")
            continue

        print(f"🔄 Memproses file baru: {key}")
        try:
            file_stream = download_from_bronze(key)
            df_raw = pd.read_parquet(file_stream)
            df_cleaned = clean_and_transform(df_raw)

            records_count = len(df_cleaned)
            df_cleaned.to_sql(
                name='silver_yellow_taxi',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=100000,
                method=psql_insert_copy
            )
            
            log_processed_file(engine, file_name, records_count)
            print(f"✅ Sukses memasukkan {records_count:,} baris dari {file_name} ke Silver Layer\n")
        except Exception as e:
            print(f"❌ Gagal memproses {key}: {str(e)}")

if __name__ == "__main__":
    run_silver_transformation()