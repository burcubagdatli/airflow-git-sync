import pandas as pd
from sqlalchemy import create_engine
import os

# ---------------- CONFIG ----------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

POSTGRES_CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql://airflow:airflow@postgres:5432/traindb"
)

INPUT_FILE = "/tmp/dirty_store_transactions.csv"
TABLE_NAME = "clean_data_transactions"
# ---------------------------------------


def main():
    print("🚀 Starting data cleaning job")

    # CSV read
    df = pd.read_csv(INPUT_FILE)

    # Basic cleaning
    df = df.drop_duplicates()
    df = df.dropna()

    # Normalize columns
    df.columns = [c.lower().strip() for c in df.columns]

    # Write to Postgres (full load)
    engine = create_engine(POSTGRES_CONN)
    df.to_sql(
        TABLE_NAME,
        engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print(f"✅ Data successfully written to public.{TABLE_NAME}")


if __name__ == "__main__":
    main()
