import csv
import gzip
import os

import boto3

LOCAL_DIR = "/data/hw01"
OUT_PATH = os.path.join(LOCAL_DIR, "wikipageviews.csv")

# (имя файла, дата, час) -> одна непересекающаяся порция на файл
SOURCE_FILES = [
    ("pageviews-20150501-010000.gz", "2015-05-01", 1),
    ("pageviews-20150501-020000.gz", "2015-05-01", 2),
]

DOMAIN_FILTER = "en"

ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET = "datalake"
STUDENT = "nvtukhlin"
DATASET = "wikipageviews"
STAGING_KEY = f"{STUDENT}/{DATASET}/staging_csv/wikipageviews.csv"


def main():
    total_written = 0
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(
            ["domain_code", "page_title", "count_views", "total_response_size", "event_date", "event_hour"]
        )
        for fname, event_date, event_hour in SOURCE_FILES:
            in_path = os.path.join(LOCAL_DIR, fname)
            if not os.path.exists(in_path):
                raise SystemExit(f"Файл не найден: {in_path}")
            written_here = 0
            with gzip.open(in_path, "rt", encoding="utf-8", errors="strict") as in_f:
                for line in in_f:
                    parts = line.rstrip("\n").split(" ")
                    if len(parts) != 4:
                        continue
                    domain_code, page_title, count_views, total_response_size = parts
                    if domain_code != DOMAIN_FILTER:
                        continue
                    writer.writerow(
                        [domain_code, page_title, count_views, total_response_size, event_date, event_hour]
                    )
                    written_here += 1
            print(f"{fname}: domain_code=='{DOMAIN_FILTER}' строк -> {written_here}")
            total_written += written_here

    size_mb = os.path.getsize(OUT_PATH) / 1024 / 1024
    print(f"\nИтого строк: {total_written}")
    print(f"Записан {OUT_PATH} ({size_mb:.1f} МиБ)")

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    print(f"Загружаем -> s3://{BUCKET}/{STAGING_KEY} ...")
    s3.upload_file(OUT_PATH, BUCKET, STAGING_KEY)
    print("Готово.")


if __name__ == "__main__":
    main()
