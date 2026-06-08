from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import glob
import os

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("DustiniaDelixia_Operational_Analytics") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    orders = spark.read.parquet(
        "file:///opt/airflow/data_lake/raw/orders.parquet"
    )
    order_items = spark.read.parquet(
        "file:///opt/airflow/data_lake/raw/order_items.parquet"
    )
    reviews = spark.read.parquet(
        "file:///opt/airflow/data_lake/raw/order_reviews.parquet"
    )
    customers = spark.read.parquet(
        "file:///opt/airflow/data_lake/raw/customers.parquet"
    )
    sellers = spark.read.parquet(
        "file:///opt/airflow/data_lake/raw/sellers.parquet"
    )

    print("Melakukan join dataset...")
    
    df = orders \
        .join(
            order_items,
            "order_id",
            "left"
        ) \
        .join(
            reviews,
            "order_id",
            "left"
        ) \
        .join(
            customers,
            "customer_id",
            "left"
        ) \
        .join(
            sellers,
            "seller_id",
            "left"
        )

    print("Konversi timestamp...")

    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date"
    ]

    for col_name in timestamp_cols:
        df = df.withColumn(
            col_name,
            F.to_timestamp(F.col(col_name))
        )

    print("📊 Menghitung KPI Operasional...")

    # KPI 1 : Delivery delay
    df = df.withColumn(
        "delivery_delay_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        )
    )

    # KPI 2 : Processing time
    df = df.withColumn(
        "processing_time_days",
        (
            F.unix_timestamp(
                "order_approved_at"
            )
            -
            F.unix_timestamp(
                "order_purchase_timestamp"
            )
        ) / 86400
    )

    # KPI 3 : Shipping time
    df = df.withColumn(
        "shipping_time_days",
        (
            F.unix_timestamp(
                "order_delivered_customer_date"
            )
            -
            F.unix_timestamp(
                "order_delivered_carrier_date"
            )
        ) / 86400
    )

    # KPI 4 : SLA breach
    df = df.withColumn(
        "is_late",
        F.when(
            F.col("order_delivered_customer_date")
            >
            F.col("shipping_limit_date"),
            1
        ).otherwise(0)
    )

    # KPI 5 : Month
    df = df.withColumn(
        "order_month",
        F.date_format(
            "order_purchase_timestamp",
            "yyyy-MM"
        )
    )

    print("📈 Membuat KPI Summary...")

    kpi_summary = df.agg(
        F.countDistinct(
            "order_id"
        ).alias(
            "total_orders"
        ),

        F.avg(
            "delivery_delay_days"
        ).alias(
            "avg_delay_days"
        ),

        F.avg(
            "processing_time_days"
        ).alias(
            "avg_processing_days"
        ),

        F.avg(
            "shipping_time_days"
        ).alias(
            "avg_shipping_days"
        ),

        F.avg(
            "review_score"
        ).alias(
            "avg_review_score"
        ),

        F.avg(
            "is_late"
        ).alias(
            "late_rate"
        )
    )

    print("📍 Membuat State Summary...")

    state_summary = df.groupBy(
        "customer_state"
    ).agg(
        F.countDistinct(
            "order_id"
        ).alias(
            "total_orders"
        ),

        F.avg(
            "delivery_delay_days"
        ).alias(
            "avg_delay_days"
        ),

        F.avg(
            "is_late"
        ).alias(
            "late_rate"
        ),

        F.avg(
            "review_score"
        ).alias(
            "avg_review_score"
        )
    )

    print("🏪 Membuat Seller Summary...")

    seller_summary = df.groupBy(
        "seller_id"
    ).agg(
        F.countDistinct(
            "order_id"
        ).alias(
            "total_orders"
        ),

        F.avg(
            "is_late"
        ).alias(
            "sla_breach_rate"
        ),

        F.avg(
            "delivery_delay_days"
        ).alias(
            "avg_delay_days"
        ),

        F.avg(
            "review_score"
        ).alias(
            "avg_review_score"
        )
    )

    print("📅 Membuat Monthly Trend...")

    monthly_trend = df.groupBy(
        "order_month"
    ).agg(
        F.countDistinct(
            "order_id"
        ).alias(
            "total_orders"
        ),

        F.avg(
            "delivery_delay_days"
        ).alias(
            "avg_delay_days"
        ),

        F.avg(
            "is_late"
        ).alias(
            "late_rate"
        )
    ).orderBy(
        "order_month"
    )

    print("💾 Konversi ke Pandas...")

    df_fact = df.toPandas()
    df_kpi = kpi_summary.toPandas()
    df_state = state_summary.toPandas()
    df_seller = seller_summary.toPandas()
    df_month = monthly_trend.toPandas()

    # CLICKHOUSE
    print("Memuat ke ClickHouse Warehouse...")

    # --- PERBAIKAN MULAI DI SINI ---
    # Tambahkan parameter user dan password sesuai dengan pengaturan ClickHouse Anda
    # Jika Anda menggunakan default bawaan docker, biasanya user='default' dan password='' (kosong)
    # ATAU jika Anda mengatur password di docker-compose.yml, masukkan di sini.
    client = Client(
        host="clickhouse-server",
        user="aqil",
        password="aqil"
    )

    client.execute(
        "CREATE DATABASE IF NOT EXISTS fpmci2026_db"
    )

    # TABLE: fact_operational_orders
    client.execute(
        "DROP TABLE IF EXISTS fpmci2026_db.fact_operational_orders"
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.fact_operational_orders (
            order_id String,
            customer_id String,
            seller_id String,
            customer_state String,
            seller_state String,
            review_score Float64,
            delivery_delay_days Float64,
            processing_time_days Float64,
            shipping_time_days Float64,
            is_late UInt8,
            order_month String
        )
        ENGINE = MergeTree()
        ORDER BY order_id
    """)

    fact_data = df_fact[
        [
            "order_id",
            "customer_id",
            "seller_id",
            "customer_state",
            "seller_state",
            "review_score",
            "delivery_delay_days",
            "processing_time_days",
            "shipping_time_days",
            "is_late",
            "order_month"
        ]
    ].fillna(0)

    client.execute(
        """INSERT INTO fpmci2026_db.fact_operational_orders VALUES""",
        [
            tuple(x)
            for x in fact_data.to_numpy()
        ]
    )

    print("fact_operational_orders loaded")

    # TABLE: KPI summary
    client.execute(
        "DROP TABLE IF EXISTS fpmci2026_db.kpi_summary"
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.kpi_summary (
            total_orders Int32,
            avg_delay_days Float64,
            avg_processing_days Float64,
            avg_shipping_days Float64,
            avg_review_score Float64,
            late_rate Float64
        )
        ENGINE = MergeTree()
        ORDER BY total_orders
    """)

    client.execute(
        """INSERT INTO fpmci2026_db.kpi_summary VALUES""",
        [
            tuple(x)
            for x in df_kpi.to_numpy()
        ]
    )

    print("✅ kpi_summary loaded")

    # TABLE: State summary
    client.execute(
        "DROP TABLE IF EXISTS fpmci2026_db.state_summary"
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.state_summary (
            customer_state String,
            total_orders Int32,
            avg_delay_days Float64,
            late_rate Float64,
            avg_review_score Float64
        )
        ENGINE = MergeTree()
        ORDER BY customer_state
    """)

    client.execute(
        """INSERT INTO fpmci2026_db.state_summary VALUES""",
        [
            tuple(x)
            for x in df_state.to_numpy()
        ]
    )

    print("✅ state_summary loaded")

    # TABLE: Seller summary
    client.execute(
        "DROP TABLE IF EXISTS fpmci2026_db.seller_summary"
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.seller_summary (
            seller_id String,
            total_orders Int32,
            sla_breach_rate Float64,
            avg_delay_days Float64,
            avg_review_score Float64
        )
        ENGINE = MergeTree()
        ORDER BY seller_id
    """)

    client.execute(
        """INSERT INTO fpmci2026_db.seller_summary VALUES""",
        [
            tuple(x)
            for x in df_seller.to_numpy()
        ]
    )

    print("✅ seller_summary loaded")

    # TABLE: Monthly trend
    client.execute(
        "DROP TABLE IF EXISTS fpmci2026_db.monthly_trend"
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.monthly_trend (
            order_month String,
            total_orders Int32,
            avg_delay_days Float64,
            late_rate Float64
        )
        ENGINE = MergeTree()
        ORDER BY order_month
    """)

    client.execute(
        """INSERT INTO fpmci2026_db.monthly_trend VALUES""",
        [
            tuple(x)
            for x in df_month.to_numpy()
        ]
    )

    print("✅ monthly_trend loaded")

    spark.stop()

    print("✅ Pipeline selesai")


if __name__ == "__main__":
    run_spark_analytics()
