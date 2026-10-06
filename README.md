# Large-Scale Taxi Data Pipeline

An end-to-end data engineering pipeline for processing NYC Yellow Taxi trip data using Apache Kafka, MinIO, Polars, Apache Airflow, Snowflake, and Streamlit.

The project demonstrates a modern data engineering workflow covering data ingestion, streaming, object storage, data transformation, workflow orchestration, cloud data warehousing, and analytics visualization.

---

## Architecture

```text
NYC Yellow Taxi Parquet Data
            │
            ▼
      Kafka Producer
            │
            ▼
      Apache Kafka
            │
            ▼
        MinIO Bronze
       (Raw JSONL Data)
            │
            ▼
     Apache Airflow DAG
            │
            ▼
    Polars Transformation
            │
            ▼
        MinIO Silver
      (Parquet Data)
            │
            ▼
         Snowflake
      (Analytical Layer)
            │
            ▼
        Streamlit
       Dashboard