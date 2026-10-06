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

BRONZE_KEY = "yellow_taxi/year=2026/month=01/batch_00001.jsonl"
SILVER_KEY = "yellow_taxi/year=2026/month=01/batch_00001.parquet"

# VALIDATE CONFIGURATION

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError(
        "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the .env file."
    )


# S3 / MINIO CONNECTION

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)



# READ BRONZE

def read_bronze():

    response = s3.get_object(
        Bucket=BRONZE_BUCKET,
        Key=BRONZE_KEY
    )

    data = response["Body"].read()

    return pl.read_ndjson(io.BytesIO(data))

# READ SILVER
def read_silver():

    response = s3.get_object(
        Bucket=SILVER_BUCKET,
        Key=SILVER_KEY
    )

    data = response["Body"].read()

    return pl.read_parquet(io.BytesIO(data))

# COMPARISON
def compare():

    bronze = read_bronze()
    silver = read_silver()

    print("=" * 70)
    print("BRONZE vs SILVER COMPARISON")
    print("=" * 70)

    # ROW COUNT
    print("\n1. ROW COUNT")
    print("-" * 70)

    print(f"Bronze rows : {bronze.height:,}")
    print(f"Silver rows : {silver.height:,}")
    print(f"Rows removed: {bronze.height - silver.height:,}")

    # COLUMN COUNT
    print("\n2. COLUMN COUNT")
    print("-" * 70)

    print(f"Bronze columns : {len(bronze.columns)}")
    print(f"Silver columns : {len(silver.columns)}")

    # COLUMNS
    print("\n3. COLUMNS ADDED IN SILVER")
    print("-" * 70)

    added_columns = [
        col for col in silver.columns
        if col not in bronze.columns
    ]

    for col in added_columns:
        print(f"+ {col}")

    print("\n4. COLUMNS REMOVED FROM BRONZE")
    print("-" * 70)

    removed_columns = [
        col for col in bronze.columns
        if col not in silver.columns
    ]

    for col in removed_columns:
        print(f"- {col}")

    # DATA TYPES
    print("\n5. DATA TYPE CHANGES")
    print("-" * 70)

    for col in silver.columns:

        if col in bronze.columns:

            bronze_type = bronze.schema[col]
            silver_type = silver.schema[col]

            if bronze_type != silver_type:

                print(
                    f"{col}: "
                    f"{bronze_type} → {silver_type}"
                )

    # SAMPLE DATA
    print("\n6. BRONZE SAMPLE")
    print("-" * 70)

    print(
        bronze.select(
            [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "trip_distance",
            ]
        ).head(3)
    )

    print("\n7. SILVER SAMPLE")
    print("-" * 70)

    print(
        silver.select(
            [
                "pickup_datetime",
                "dropoff_datetime",
                "trip_duration_minutes",
                "trip_distance",
            ]
        ).head(3)
    )

    # SCHEMA
    print("\n8. BRONZE SCHEMA")
    print("-" * 70)

    print(bronze.schema)

    print("\n9. SILVER SCHEMA")
    print("-" * 70)

    print(silver.schema)


    # FINAL SUMMARY

    print("\n" + "=" * 70)
    print("TRANSFORMATION SUMMARY")
    print("=" * 70)

    print(f"Bronze rows          : {bronze.height:,}")
    print(f"Silver rows          : {silver.height:,}")
    print(f"Rows removed         : {bronze.height - silver.height:,}")
    print(f"Bronze columns       : {len(bronze.columns)}")
    print(f"Silver columns       : {len(silver.columns)}")
    print(f"New Silver columns   : {len(added_columns)}")
    print(f"Removed columns      : {len(removed_columns)}")

    print("=" * 70)


if __name__ == "__main__":
    try:
        compare()

    except Exception as e:
        print("\nERROR:", e)