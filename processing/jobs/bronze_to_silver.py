import io
import os

import boto3
import polars as pl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MINIO CONFIGURATION
MINIO_ENDPOINT = "http://minio:9000"
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


# S3 / MINIO CLIENT
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# CREATE SILVER BUCKET
def create_silver_bucket():
    try:
        s3.head_bucket(Bucket=SILVER_BUCKET)
        print(f"Silver bucket already exists: {SILVER_BUCKET}")

    except Exception:
        s3.create_bucket(Bucket=SILVER_BUCKET)
        print(f"Created silver bucket: {SILVER_BUCKET}")



# GET BRONZE OBJECTS

def get_bronze_objects():

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=BRONZE_PREFIX
    )

    objects = response.get("Contents", [])

    return [
        obj["Key"]
        for obj in objects
        if obj["Key"].endswith(".jsonl")
    ]


# READ BRONZE OBJECT
def read_bronze_object(key):

    print()
    print("=" * 60)
    print(f"Processing: {key}")
    print("=" * 60)

    response = s3.get_object(
        Bucket=BRONZE_BUCKET,
        Key=key
    )

    data = response["Body"].read()

    print(f"Downloaded: {len(data):,} bytes")

    df = pl.read_ndjson(io.BytesIO(data))

    print(f"Bronze rows: {df.height:,}")

    return df


# TRANSFORM BRONZE → SILVER

def transform_data(df):

    print("Starting transformations...")

    # 1. Convert timestamps
    df = df.with_columns(
        [
            pl.col("tpep_pickup_datetime")
            .str.strptime(
                pl.Datetime,
                format="%Y-%m-%d %H:%M:%S",
                strict=False
            )
            .alias("pickup_datetime"),

            pl.col("tpep_dropoff_datetime")
            .str.strptime(
                pl.Datetime,
                format="%Y-%m-%d %H:%M:%S",
                strict=False
            )
            .alias("dropoff_datetime"),
        ]
    )


    # 2. Calculate trip duration
    df = df.with_columns(
        (
            (
                pl.col("dropoff_datetime")
                - pl.col("pickup_datetime")
            ).dt.total_seconds()
            / 60
        )
        .alias("trip_duration_minutes")
    )

    # 3. Data quality filtering
    before_count = df.height

    df = df.filter(
        pl.col("pickup_datetime").is_not_null()
        & pl.col("dropoff_datetime").is_not_null()
        & (pl.col("dropoff_datetime") >= pl.col("pickup_datetime"))
        & (pl.col("trip_distance") >= 0)
    )

    after_count = df.height

    removed = before_count - after_count

    print(f"Rows before cleaning : {before_count:,}")
    print(f"Rows after cleaning  : {after_count:,}")
    print(f"Rows removed         : {removed:,}")

    # 4. Remove original string timestamp columns
    df = df.drop(
        [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ]
    )

    # 5. Reorder important columns
    first_columns = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_duration_minutes",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
    ]

    remaining_columns = [
        column
        for column in df.columns
        if column not in first_columns
    ]

    df = df.select(
        first_columns + remaining_columns
    )

    return df


# WRITE SILVER PARQUET
def write_silver(df, bronze_key):

    filename = bronze_key.split("/")[-1]

    silver_filename = filename.replace(
        ".jsonl",
        ".parquet"
    )

    silver_key = (
        SILVER_PREFIX
        + silver_filename
    )

    buffer = io.BytesIO()

    df.write_parquet(buffer)

    buffer.seek(0)

    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=silver_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(
        f"Silver written → "
        f"s3://{SILVER_BUCKET}/{silver_key}"
    )


# MAIN PIPELINE
def main():

    print("=" * 60)
    print("BRONZE → SILVER PIPELINE")
    print("=" * 60)

    create_silver_bucket()

    bronze_objects = get_bronze_objects()

    print()
    print(f"Bronze objects found: {len(bronze_objects)}")

    if not bronze_objects:
        print("No Bronze objects found.")
        return

    total_input = 0
    total_output = 0

    for key in bronze_objects:

        df = read_bronze_object(key)

        total_input += df.height

        silver_df = transform_data(df)

        total_output += silver_df.height

        write_silver(
            silver_df,
            key
        )

    print()
    print("=" * 60)
    print("PIPELINE COMPLETED")
    print("=" * 60)
    print(f"Input records  : {total_input:,}")
    print(f"Output records : {total_output:,}")
    print(f"Records removed: {total_input - total_output:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()