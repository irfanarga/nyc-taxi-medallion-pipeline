import os
import io
import requests
from datetime import datetime

# Mode Ingestion: LOCAL (MinIO) atau GCP (GCS)
ENV = os.getenv("ENVIRONMENT", "LOCAL").upper()

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_months_from_2025_to_present():
    """Menghasilkan daftar tuple (tahun, bulan_str) dari Januari 2025 hingga bulan saat ini."""
    months = []
    start_year = 2025
    start_month = 1
    
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    y, m = start_year, start_month
    while (y < current_year) or (y == current_year and m <= current_month):
        months.append((y, f"{m:02d}"))
        m += 1
        if m > 12:
            m = 1
            y += 1
            
    return months

def download_data_to_memory(url: str) -> io.BytesIO:
    """Mengunduh file Parquet langsung ke RAM tanpa menggunakan disk lokal."""
    print(f"📥 Mengunduh: {url}")
    response = requests.get(url, stream=True)
    if response.status_code == 404:
        print(f"⚠️ Data belum dirilis/tersedia di server NYC TLC (HTTP 404). Melewati...")
        return None
    response.raise_for_status()
    
    file_stream = io.BytesIO(response.content)
    size_mb = len(file_stream.getvalue()) / (1024 * 1024)
    print(f"✅ Download Selesai! Ukuran di RAM: {size_mb:.2f} MB")
    return file_stream

def upload_to_minio(file_stream: io.BytesIO, file_name: str):
    """Mengunggah file dari RAM ke MinIO (Local Bronze Layer)."""
    import boto3
    from botocore.client import Config

    minio_host = "minio" if os.path.exists("/.dockerenv") else "localhost"
    endpoint = f"http://{minio_host}:9000"

    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-bucket")

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )

    file_stream.seek(0)
    s3_client.upload_fileobj(file_stream, bucket_name, f"raw/{file_name}")
    print(f"🎉 Sukses disimpan ke MinIO: s3://{bucket_name}/raw/{file_name}")

def upload_to_gcs(file_stream: io.BytesIO, file_name: str):
    """Mengunggah file dari RAM ke Google Cloud Storage (GCP Bronze Layer)."""
    from google.cloud import storage

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Error: GCS_BUCKET_NAME belum diatur di file .env!")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"raw/{file_name}")

    file_stream.seek(0)
    blob.upload_from_file(file_stream, content_type="application/octet-stream")
    print(f"🎉 Sukses disimpan ke GCS: gs://{bucket_name}/raw/{file_name}")

def run_ingestion_from_2025():
    target_months = get_months_from_2025_to_present()
    print("==================================================")
    print(f"  START INGESTION (2025 - TERBARU) TO BRONZE LAYER (Mode: {ENV})")
    print(f"  Target Periode: {target_months[0][0]}-{target_months[0][1]} s/d {target_months[-1][0]}-{target_months[-1][1]}")
    print("==================================================")

    success_count = 0
    for year, month in target_months:
        file_name = f"yellow_tripdata_{year}-{month}.parquet"
        url = f"{BASE_URL}/{file_name}"
        
        try:
            file_stream = download_data_to_memory(url)
            if file_stream is None:
                continue

            if ENV == "GCP":
                upload_to_gcs(file_stream, file_name)
            else:
                upload_to_minio(file_stream, file_name)
                
            success_count += 1
            print("-" * 50)
        except Exception as e:
            print(f"❌ Gagal memproses {file_name}: {str(e)}")

    print(f"\n✨ Ingestion Selesai! Berhasil mengunggah {success_count} file ke Bronze Layer.")

if __name__ == "__main__":
    run_ingestion_from_2025()