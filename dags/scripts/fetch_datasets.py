import requests
import pandas as pd
import os
from datetime import datetime

def fetch_dataset():
    print("Membuka keran data: Ingesting dataset...")
    base_dir = "./DustiniaDelixia_Groceria"
    output_dir = "/opt/airflow/data_lake"

    os.makedirs(output_dir, exist_ok=True)

    csv_files = [
        "customers.csv",
        "geolocation.csv",
        "order_items.csv",
        "order_reviews.csv",
        "orders.csv",
        "sellers.csv",
    ]

    for file in csv_files:
        source_path = os.path.join(base_dir, file)
        if not os.path.exists(source_path):
            print(f"{file} tidak ditemukan")
            continue

        df = pd.read_csv(source_path)

        parquet_name = file.replace(".csv", ".parquet")
        output_path = os.path.join(output_dir, parquet_name)

        df.to_parquet(output_path, index=False)
        print(f"✅ {file} -> {output_path}")

    print("🎉 Semua dataset berhasil dimasukkan ke Data Lake")

if __name__ == "__main__":
    fetch_dataset()
    