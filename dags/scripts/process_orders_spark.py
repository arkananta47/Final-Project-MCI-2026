from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import glob
import os

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")

    # CAST TYPE
    df_raw = df_raw \
        .withColumn("order_number",
            F.col("order_number").cast("int")) \
        .withColumn("order_dow",
            F.col("order_dow").cast("int")) \
        .withColumn("order_hour_of_day",
            F.col("order_hour_of_day").cast("int")) \
        .withColumn("days_since_prior_order",
            F.col("days_since_prior_order").cast("int")) \
        .withColumn("aisle_id",
            F.col("aisle_id").cast("int")) \
        .withColumn("department_id",
            F.col("department_id").cast("int")) \
        .withColumn("add_to_cart_order",
            F.col("add_to_cart_order").cast("int")) \
        .withColumn("reordered",
            F.col("reordered").cast("int"))

    print("Menghitung analytics...")

    # TOP PRODUCTS
    trending_products = df_raw.groupBy(
        "product_id",
        "product_name",
        "department"
    ).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items"),
        F.sum("reordered").alias("total_reordered")
    ).orderBy(
        F.desc("total_items")
    ).limit(30)

    # CATEGORY SUMMARY
    category_summary = df_raw.groupBy(
        "department"
    ).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items_sold"),
        F.avg("reordered").alias("reorder_rate")
    ).orderBy(
        F.desc("total_items_sold")
    )

    # DAILY ORDERS
    daily_orders = df_raw.groupBy(
        "order_date"
    ).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items")
    ).orderBy("order_date")

    # TO PANDAS
    df_products = trending_products.toPandas()
    df_categories = category_summary.toPandas()
    df_daily = daily_orders.toPandas()
    df_all = df_raw.toPandas()

    # CLICKHOUSE
    print("Memuat ke ClickHouse Warehouse...")

    # --- PERBAIKAN MULAI DI SINI ---
    # Tambahkan parameter user dan password sesuai dengan pengaturan ClickHouse Anda
    # Jika Anda menggunakan default bawaan docker, biasanya user='default' dan password='' (kosong)
    # ATAU jika Anda mengatur password di docker-compose.yml, masukkan di sini.
    client = Client(
        host='clickhouse-server',
        user='kelompok28',
        password='kelompok28'
    )
    # --- PERBAIKAN SELESAI ---

    client.execute(
        'CREATE DATABASE IF NOT EXISTS mci2026_db'
    )

    # TABLE: orders_trending_products
    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_trending_products'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_trending_products (
            product_id String,
            product_name String,
            department String,
            total_orders Int32,
            total_items Int32,
            total_reordered Int32
        )
        ENGINE = MergeTree()
        ORDER BY total_items
    ''')

    data_products = [
        tuple(x)
        for x in df_products.to_numpy()
    ]

    if data_products:
        client.execute(
            'INSERT INTO mci2026_db.orders_trending_products VALUES',
            data_products
        )

    print(f"✅ orders_trending_products: {len(data_products)} rows")

    # TABLE: orders_category_summary
    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_category_summary'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_category_summary (
            department String,
            total_orders Int32,
            total_items_sold Int32,
            reorder_rate Float64
        )
        ENGINE = MergeTree()
        ORDER BY total_items_sold
    ''')

    data_categories = [
        tuple(x)
        for x in df_categories.to_numpy()
    ]

    if data_categories:
        client.execute(
            'INSERT INTO mci2026_db.orders_category_summary VALUES',
            data_categories
        )

    print(f"✅ orders_category_summary: {len(data_categories)} rows")

    # TABLE: orders_daily_orders
    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders_daily_orders'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders_daily_orders (
            order_date String,
            total_orders Int32,
            total_items Int32
        )
        ENGINE = MergeTree()
        ORDER BY order_date
    ''')

    data_daily = [
        tuple(x)
        for x in df_daily.to_numpy()
    ]

    if data_daily:
        client.execute(
            'INSERT INTO mci2026_db.orders_daily_orders VALUES',
            data_daily
        )

    print(f"✅ orders_daily_orders: {len(data_daily)} rows")

    # TABLE: orders
    client.execute(
        'DROP TABLE IF EXISTS mci2026_db.orders'
    )

    client.execute('''
        CREATE TABLE mci2026_db.orders (
            order_id String,
            user_id String,
            order_number Int32,
            order_dow Int32,
            order_hour_of_day Int32,
            days_since_prior_order Int32,
            eval_set String,
            product_id String,
            product_name String,
            aisle_id Int32,
            aisle String,
            department_id Int32,
            department String,
            add_to_cart_order Int32,
            reordered Int32,
            order_date String,
            updated_at String,
            ingested_at String
        )
        ENGINE = MergeTree()
        PARTITION BY substring(order_date, 1, 7)
        ORDER BY (order_date, order_id)
    ''')

    data_all = [
        tuple(x)
        for x in df_all.to_numpy()
    ]

    if data_all:
        client.execute(
            'INSERT INTO mci2026_db.orders VALUES',
            data_all
        )

    print(f"✅ orders: {len(data_all)} rows")

    # Menghapus file .parquet yang sudah diproses agar tidak menumpuk
    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error deleting {f}: {e}")

    spark.stop()

    print("✅ Pipeline selesai")


if __name__ == "__main__":
    run_spark_analytics()