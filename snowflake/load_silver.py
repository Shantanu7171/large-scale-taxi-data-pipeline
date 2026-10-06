import io
import os

import boto3
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas




load_dotenv()


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SILVER_BUCKET = "taxi-silver"
SILVER_PREFIX = "yellow_taxi/year=2026/month=01/"



# SNOWFLAKE CONFIG
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv(
    "SNOWFLAKE_WAREHOUSE",
    "COMPUTE_WH"
)

SNOWFLAKE_DATABASE = "TAXI_DATA"
SNOWFLAKE_SCHEMA = "SILVER"
SNOWFLAKE_TABLE = "TAXI_TRIPS"


# VALIDATE SNOWFLAKE CONFIG
def validate_config():

    required_values = {
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    }

    missing = [
        key
        for key, value in required_values.items()
        if not value
    ]

    if missing:
        raise ValueError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing)
        )


# MINIO CONNECTION
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# GET SILVER FILES
def get_silver_files():

    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix=SILVER_PREFIX
    )

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    return sorted(files)


# CONNECT TO SNOWFLAKE
def get_snowflake_connection():

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# LOAD ONE PARQUET FILE
def load_file(conn, silver_key):

    print()
    print("-" * 70)
    print(f"Processing: {silver_key}")
    print("-" * 70)

 
    # Download Parquet from MinIO
    response = s3.get_object(
        Bucket=SILVER_BUCKET,
        Key=silver_key
    )

    data = response["Body"].read()

    print(f"Downloaded: {len(data):,} bytes")

    # Read Parquet using Pandas
    df = pd.read_parquet(
        io.BytesIO(data)
    )

    print(f"Rows: {len(df):,}")

    # Normalize datetime precision before Snowflake load
    df["pickup_datetime"] = pd.to_datetime(
    df["pickup_datetime"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    df["dropoff_datetime"] = pd.to_datetime(
        df["dropoff_datetime"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert column names to uppercase
    column_mapping = {
    "VendorID": "VENDOR_ID",
    "pickup_datetime": "PICKUP_DATETIME",
    "dropoff_datetime": "DROPOFF_DATETIME",
    "trip_duration_minutes": "TRIP_DURATION_MINUTES",
    "passenger_count": "PASSENGER_COUNT",
    "trip_distance": "TRIP_DISTANCE",
    "PULocationID": "PU_LOCATION_ID",
    "DOLocationID": "DO_LOCATION_ID",
    "payment_type": "PAYMENT_TYPE",
    "RatecodeID": "RATECODE_ID",
    "store_and_fwd_flag": "STORE_AND_FWD_FLAG",
    "fare_amount": "FARE_AMOUNT",
    "extra": "EXTRA",
    "mta_tax": "MTA_TAX",
    "tip_amount": "TIP_AMOUNT",
    "tolls_amount": "TOLLS_AMOUNT",
    "improvement_surcharge": "IMPROVEMENT_SURCHARGE",
    "total_amount": "TOTAL_AMOUNT",
    "congestion_surcharge": "CONGESTION_SURCHARGE",
    "Airport_fee": "AIRPORT_FEE",
    "cbd_congestion_fee": "CBD_CONGESTION_FEE",
}

    df = df.rename(columns=column_mapping)
     

    # Load DataFrame into Snowflake
    success, nchunks, nrows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=SNOWFLAKE_TABLE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=False,
        overwrite=False,
    )

    if not success:
        raise RuntimeError(
            f"Failed to load file: {silver_key}"
        )

    print(
        f"Loaded into Snowflake: {nrows:,} rows"
    )

    return nrows


# MAIN PIPELINE
def main():

    print("=" * 70)
    print("MINIO SILVER → SNOWFLAKE")
    print("=" * 70)

    # Validate configuration
    validate_config()

    # Find Silver files
    silver_files = get_silver_files()

    print(
        f"Silver files found: {len(silver_files)}"
    )

    if not silver_files:

        print("No Silver Parquet files found.")

        return

    # Connect to Snowflake

    print("Connecting to Snowflake...")

    conn = get_snowflake_connection()

    print("Snowflake connection successful.")

    try:

        # Clean existing table for initial full load
        cursor = conn.cursor()

        cursor.execute(
            f"""
            TRUNCATE TABLE
            {SNOWFLAKE_DATABASE}.
            {SNOWFLAKE_SCHEMA}.
            {SNOWFLAKE_TABLE}
            """
        )

        cursor.close()

        print("Snowflake table truncated.")
        print("Starting Silver → Snowflake load...")

        # Load all Silver files
        total_loaded = 0

        for silver_key in silver_files:

            rows_loaded = load_file(
                conn,
                silver_key
            )

            total_loaded += rows_loaded

        # Verify Snowflake row count
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {SNOWFLAKE_DATABASE}.
                 {SNOWFLAKE_SCHEMA}.
                 {SNOWFLAKE_TABLE}
            """
        )

        snowflake_count = cursor.fetchone()[0]

        cursor.close()

        # Final result
        print()
        print("=" * 70)
        print("LOAD COMPLETED")
        print("=" * 70)

        print(
            f"Files loaded          : {len(silver_files):,}"
        )

        print(
            f"Rows loaded           : {total_loaded:,}"
        )

        print(
            f"Snowflake row count   : {snowflake_count:,}"
        )

        print()

        if total_loaded == snowflake_count:

            print(
                "✓ Snowflake record count verified"
            )

        else:

            print(
                "✗ Record count mismatch"
            )

        print("=" * 70)

    finally:

        conn.close()

        print(
            "Snowflake connection closed."
        )


if __name__ == "__main__":

    try:

        main()

    except Exception as e:

        print()
        print("=" * 70)
        print("ERROR")
        print("=" * 70)
        print(e)
        print("=" * 70)