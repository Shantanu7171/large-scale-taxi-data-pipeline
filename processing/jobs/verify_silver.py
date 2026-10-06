import io
import os

import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()

# MINIO CONFIG
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BRONZE_BUCKET = "taxi-bronze"
SILVER_BUCKET = "taxi-silver"

BRONZE_PREFIX = "yellow_taxi/year=2026/month=01/"
SILVER_PREFIX = "yellow_taxi/year=2026/month=01/"

# VALIDATE CONFIGURATION
if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError(
        "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the .env file."
    )

# MINIO / S3 CLIENT

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# LIST OBJECTS
def list_objects(bucket, prefix):

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    return [
        obj["Key"]
        for obj in response.get("Contents", [])
    ]

# COUNT BRONZE RECORDS
def count_bronze_records(keys):

    total = 0

    for key in keys:

        if not key.endswith(".jsonl"):
            continue

        response = s3.get_object(
            Bucket=BRONZE_BUCKET,
            Key=key
        )

        data = response["Body"].read()

        df = pl.read_ndjson(io.BytesIO(data))

        total += df.height

    return total

# COUNT SILVER RECORDS
def count_silver_records(keys):

    total = 0

    for key in keys:

        if not key.endswith(".parquet"):
            continue

        response = s3.get_object(
            Bucket=SILVER_BUCKET,
            Key=key
        )

        data = response["Body"].read()

        df = pl.read_parquet(io.BytesIO(data))

        total += df.height

    return total


# MAIN
def main():

    print("=" * 70)
    print("SILVER LAYER VERIFICATION")
    print("=" * 70)

    bronze_keys = list_objects(
        BRONZE_BUCKET,
        BRONZE_PREFIX
    )

    silver_keys = list_objects(
        SILVER_BUCKET,
        SILVER_PREFIX
    )

    bronze_files = [
        key for key in bronze_keys
        if key.endswith(".jsonl")
    ]

    silver_files = [
        key for key in silver_keys
        if key.endswith(".parquet")
    ]

    print(f"\nBronze files : {len(bronze_files)}")
    print(f"Silver files : {len(silver_files)}")

    print("\nCounting records...")

    bronze_records = count_bronze_records(
        bronze_files
    )

    silver_records = count_silver_records(
        silver_files
    )

    print("\n" + "-" * 70)
    print("RECORD COUNT")
    print("-" * 70)

    print(f"Bronze records : {bronze_records:,}")
    print(f"Silver records : {silver_records:,}")
    print(f"Difference     : {bronze_records - silver_records:,}")

    print("\n" + "-" * 70)
    print("VERIFICATION")
    print("-" * 70)

    if bronze_records == silver_records:
        print("✓ Record count matches")
    else:
        print("✗ Record count mismatch")

    if len(bronze_files) == len(silver_files):
        print("✓ File count matches")
    else:
        print("✗ File count mismatch")

    print("\n" + "=" * 70)
    print("SILVER VERIFICATION COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()