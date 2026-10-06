import json
import io
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "taxi_trips"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET_NAME = "taxi-bronze"

BATCH_SIZE = 1000


# Validate configuration
if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError(
        "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the .env file."
    )

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="taxi-bronze-consumer",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


# Upload batch to MinIO
def upload_batch(records, batch_number):

    timestamp = datetime.now(timezone.utc)

    key = (
        f"yellow_taxi/"
        f"year=2026/"
        f"month=01/"
        f"batch_{batch_number:05d}.jsonl"
    )

    buffer = io.StringIO()

    for record in records:

        buffer.write(
            json.dumps(
                record,
                default=str
            )
        )

        buffer.write("\n")

    data = buffer.getvalue().encode("utf-8")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=data,
        ContentType="application/x-ndjson",
    )

    print(
        f"Uploaded {len(records):,} records → "
        f"s3://{BUCKET_NAME}/{key}"
    )


# Consume Kafka
def consume():

    records = []
    batch_number = 1
    total_records = 0

    print("=" * 60)
    print("KAFKA → MINIO BRONZE CONSUMER")
    print("=" * 60)

    print(f"Kafka topic : {TOPIC_NAME}")
    print(f"MinIO bucket: {BUCKET_NAME}")
    print("=" * 60)

    try:

        for message in consumer:

            records.append(message.value)

            if len(records) >= BATCH_SIZE:

                upload_batch(
                    records,
                    batch_number
                )

                total_records += len(records)

                print(
                    f"Total processed: "
                    f"{total_records:,}"
                )

                records = []
                batch_number += 1

    except KeyboardInterrupt:

        print("\nStopping consumer...")

        if records:

            upload_batch(
                records,
                batch_number
            )

        print(
            f"Total processed: "
            f"{total_records + len(records):,}"
        )

    finally:

        consumer.close()

        print("Consumer closed.")


# Main

if __name__ == "__main__":
    consume()