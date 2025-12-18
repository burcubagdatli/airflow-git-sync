import pandas as pd
from sqlalchemy import create_engine
import os

# ---------------- CONFIG ----------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

POSTGRES_CONN = "postgresql://airflow:airflow@postgres:5432/traindb"

INPUT_PATH = "/tmp/dirty_store_transactions.csv"
OUTPUT_TABLE = "clean_data_transactions"
# ----------------------------------------


def main():
    print("Starting data cleaning job...")

    # Read CSV (MinIO mount veya wget ile alınmış varsayılır)
    df = pd.read_csv(INPUT_PATH)

    # Basic cleaning
    df = df.drop_duplicates()
    df = df.dropna()

    # Column normalization
    df.columns = [c.lower() for c in df.columns]

    # Write to Postgres
    engine = create_engine(POSTGRES_CONN)
    df.to_sql(
        OUTPUT_TABLE,
        engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print(f"Data successfully written to {OUTPUT_TABLE}")


if __name__ == "__main__":
    main()
