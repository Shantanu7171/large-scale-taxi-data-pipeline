import json
import time
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer


# -----------------------------------
# Configuration
# -----------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "taxi_trips"

BATCH_SIZE = 1000
DELAY_SECONDS = 0.5

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    value_serializer=lambda value: json.dumps(
        value,
        default=str
    ).encode("utf-8"),

    linger_ms=10,
    batch_size=32768,
)

# Find Parquet file
def find_parquet_file():

    files = list(RAW_DATA_DIR.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No Parquet file found in {RAW_DATA_DIR}"
        )

    return files[0]


# Send Parquet data to Kafka
def send_data():

    parquet_file = find_parquet_file()

    print("=" * 60)
    print("NYC TAXI DATA → KAFKA")
    print("=" * 60)

    print(f"File  : {parquet_file.name}")
    print(f"Topic : {TOPIC_NAME}")
    print(f"Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 60)

    parquet = pq.ParquetFile(parquet_file)

    total_rows = parquet.metadata.num_rows

    print(f"Total records: {total_rows:,}")
    print("Starting ingestion...")
    print()

    sent_records = 0

    # Read Parquet incrementally
    for batch in parquet.iter_batches(
        batch_size=BATCH_SIZE
    ):

        records = batch.to_pylist()

        for record in records:

            producer.send(
                TOPIC_NAME,
                value=record
            )

        producer.flush()

        sent_records += len(records)

        print(
            f"Sent: {sent_records:,} / "
            f"{total_rows:,}"
        )

        time.sleep(DELAY_SECONDS)

    producer.flush()

    print()
    print("=" * 60)
    print("SUCCESS")
    print(f"Total records sent: {sent_records:,}")
    print("=" * 60)

# Main
if __name__ == "__main__":

    try:
        send_data()

    except Exception as e:
        print()
        print("ERROR:", e)

    finally:
        producer.close()