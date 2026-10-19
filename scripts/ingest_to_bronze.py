import os
import io
import requests
from datetime import datetime

ENV = os.getenv("ENVIRONMENT", "LOCAL").upper()
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_months_from_2025_to_present():
    months = []
    start_year, start_month = 2025, 1
    now = datetime.now()
    y, m = start_year, start_month
    while (y < now.year) or (y == now.year and m <= now.month):
        months.append((y, f"{m:02d}"))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months

def is_file_in_bronze(file_name: str) -> bool:
    """Mengecek apakah file sudah tersimpan di MinIO / GCS."""
    if ENV == "GCP":
        from google.cloud import storage
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if not bucket_name:
            return False
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"raw/{file_name}")
        return blob.exists()
    else:
        import boto3
        from botocore.client import Config
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
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f"raw/{file_name}")
            return True
        except:
            return False

def download_data_to_memory(url: str) -> io.BytesIO:
    print(f"📥 Mengunduh: {url}")
    response = requests.get(url, stream=True)
    if response.status_code == 404:
        print(f"⚠️ Data belum dirilis di server NYC TLC (HTTP 404). Melewati...")
        return None
    response.raise_for_status()
    file_stream = io.BytesIO(response.content)
    size_mb = len(file_stream.getvalue()) / (1024 * 1024)
    print(f"✅ Download Selesai! Ukuran di RAM: {size_mb:.2f} MB")
    return file_stream

def upload_to_minio(file_stream: io.BytesIO, file_name: str):
    import boto3
    from botocore.client import Config
    minio_host = "minio" if os.path.exists("/.dockerenv") else "localhost"
    endpoint = f"http://{minio_host}:9000"
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version='s3v4')
    )
    file_stream.seek(0)
    s3_client.upload_fileobj(file_stream, os.getenv("MINIO_BUCKET_NAME", "bronze-bucket"), f"raw/{file_name}")
    print(f"🎉 Sukses disimpan ke MinIO: raw/{file_name}")

def upload_to_gcs(file_stream: io.BytesIO, file_name: str):
    from google.cloud import storage
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"raw/{file_name}")
    file_stream.seek(0)
    blob.upload_from_file(file_stream, content_type="application/octet-stream")
    print(f"🎉 Sukses disimpan ke GCS: raw/{file_name}")

def run_ingestion_from_2025():
    target_months = get_months_from_2025_to_present()
    print("==================================================")
    print(f"  START INCREMENTAL INGESTION TO BRONZE LAYER (Mode: {ENV})")
    print("==================================================")

    for year, month in target_months:
        file_name = f"yellow_tripdata_{year}-{month}.parquet"
        url = f"{BASE_URL}/{file_name}"
        
        # 🔍 Pengecekan Incremental:
        if is_file_in_bronze(file_name):
            print(f"⏩ [SKIP] {file_name} sudah ada di Bronze Layer. Melewati download.")
            continue

        try:
            file_stream = download_data_to_memory(url)
            if file_stream is None:
                continue

            if ENV == "GCP":
                upload_to_gcs(file_stream, file_name)
            else:
                upload_to_minio(file_stream, file_name)
                
            print("-" * 50)
        except Exception as e:
            print(f"❌ Gagal memproses {file_name}: {str(e)}")

if __name__ == "__main__":
    run_ingestion_from_2025()