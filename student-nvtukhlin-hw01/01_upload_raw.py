import os

import boto3

ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET = "raw"
STUDENT = "nvtukhlin"
DATASET = "wikipageviews"
INGESTION_DATE = "2026-09-16"

LOCAL_DIR = "/data/hw01"
SOURCE_FILES = [
    "pageviews-20150501-010000.gz",
    "pageviews-20150501-020000.gz",
]


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    prefix = f"{STUDENT}/{DATASET}/ingestion_date={INGESTION_DATE}"

    for fname in SOURCE_FILES:
        local_path = os.path.join(LOCAL_DIR, fname)
        if not os.path.exists(local_path):
            raise SystemExit(f"Файл не найден: {local_path}")
        key = f"{prefix}/{fname}"
        size_mb = os.path.getsize(local_path) / 1024 / 1024
        print(f"Загружаем {local_path} ({size_mb:.1f} МиБ) -> s3://{BUCKET}/{key} ...")
        s3.upload_file(local_path, BUCKET, key)

    print(f"\nОбъекты в s3://{BUCKET}/{prefix}/:")
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
        print(f"  s3://{BUCKET}/{obj['Key']}  {obj['Size'] / 1024 / 1024:.1f} МиБ")


if __name__ == "__main__":
    main()
