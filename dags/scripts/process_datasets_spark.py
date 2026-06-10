from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import pandas as pd
import re

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("DustiniaDelixia_Operational_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    orders = spark.read.parquet(
        "file:///opt/airflow/data_lake/orders.parquet"
    )
    order_items = spark.read.parquet(
        "file:///opt/airflow/data_lake/order_items.parquet"
    )
    reviews = spark.read.parquet(
        "file:///opt/airflow/data_lake/order_reviews.parquet"
    )
    customers = spark.read.parquet(
        "file:///opt/airflow/data_lake/customers.parquet"
    )
    sellers = spark.read.parquet(
        "file:///opt/airflow/data_lake/sellers.parquet"
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
                "order_delivered_carrier_date"
            )
            -
            F.unix_timestamp(
                "order_approved_at"
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

    # DATA CLEANING UNTUK CLICKHOUSE
    string_cols_fact = [
        "order_id",
        "customer_id",
        "seller_id",
        "customer_state",
        "seller_state",
        "order_month"
    ]

    for col in string_cols_fact:
        if col in df_fact.columns:
            df_fact[col] = (
                df_fact[col]
                .fillna("")
                .astype(str)
            )

    numeric_cols_fact = [
        "review_score",
        "delivery_delay_days",
        "processing_time_days",
        "shipping_time_days",
        "is_late"
    ]

    for col in numeric_cols_fact:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(
                df_fact[col],
                errors="coerce"
            ).fillna(0)

    if "customer_state" in df_state.columns:
        df_state["customer_state"] = (
            df_state["customer_state"]
            .fillna("")
            .astype(str)
        )

    if "seller_id" in df_seller.columns:
        df_seller["seller_id"] = (
            df_seller["seller_id"]
            .fillna("")
            .astype(str)
        )

    if "order_month" in df_month.columns:
        df_month["order_month"] = (
            df_month["order_month"]
            .fillna("")
            .astype(str)
        )

    df_kpi = df_kpi.fillna(0)

    # Bagian Stemming Bahasa Portugis (ide untuk sentiment analysis)
    positive_keywords = [
        "rapido",
        "antes",
        "prazo",
        "adiantado",
        "parabens",
        "excelente",
        "bem-embalado",
        "certinho",
        "recomendo"
    ]

    negative_keywords = [
        "atraso",
        "atrasado",
        "demora",
        "demorou",
        "amassado",
        "quebrado",
        "danificado",
        "eternidade",
        "rastreamento",
        "correios",
        "horrivel",
        "pessimo",
        "extraviado",
        "sumiu",
        "reclamacao"
    ]

    def analyze_sentiment(text):
        if text is None:
            return "neutral", ""
        text = str(text).lower()

        for pos in positive_keywords:
            if pos in text:
                return "positive", pos
        for neg in negative_keywords:
            if neg in text:
                return "negative", neg

        return "neutral", ""
    
    review_sentiment = (
        df
        .select(
            "order_id",
            "review_score",
            "review_comment_message",
            "delivery_delay_days",
            "customer_state",
            "seller_id"
        )
        .toPandas()
    )

    review_sentiment[
        ["sentiment_label", "keyword"]
    ] = review_sentiment[
        "review_comment_message"
    ].apply(
        lambda x: pd.Series(
            analyze_sentiment(x)
        )
    )

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

    # FACT TABLE
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
    ].copy()

    # STRING COLUMNS
    string_cols = [
        "order_id",
        "customer_id",
        "seller_id",
        "customer_state",
        "seller_state",
        "order_month"
    ]

    for col in string_cols:
        fact_data[col] = (
            fact_data[col]
            .astype("string")
            .fillna("")
            .astype(str)
        )

    # NUMERIC COLUMNS
    fact_data["review_score"] = pd.to_numeric(
        fact_data["review_score"],
        errors="coerce"
    ).fillna(0.0)

    fact_data["delivery_delay_days"] = pd.to_numeric(
        fact_data["delivery_delay_days"],
        errors="coerce"
    ).fillna(0.0)

    fact_data["processing_time_days"] = pd.to_numeric(
        fact_data["processing_time_days"],
        errors="coerce"
    ).fillna(0.0)

    fact_data["shipping_time_days"] = pd.to_numeric(
        fact_data["shipping_time_days"],
        errors="coerce"
    ).fillna(0.0)

    fact_data["is_late"] = pd.to_numeric(
        fact_data["is_late"],
        errors="coerce"
    ).fillna(0).astype(int)

    
    client.execute(
        """INSERT INTO fpmci2026_db.fact_operational_orders VALUES""",
        fact_data.values.tolist()
    )

    print("fact_operational_orders loaded")
    print(df_kpi.dtypes)
    print(df_state.dtypes)
    print(df_seller.dtypes)
    print(df_month.dtypes)

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

    df_kpi["total_orders"] = (
        pd.to_numeric(
            df_kpi["total_orders"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    data_kpi = [
        (
            int(row["total_orders"]),
            float(row["avg_delay_days"]),
            float(row["avg_processing_days"]),
            float(row["avg_shipping_days"]),
            float(row["avg_review_score"]),
            float(row["late_rate"])
        )
        for _, row in df_kpi.iterrows()
    ]

    client.execute(
        "INSERT INTO fpmci2026_db.kpi_summary VALUES",
        data_kpi
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

    df_state["total_orders"] = (
        pd.to_numeric(
            df_state["total_orders"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    data_state = [
        (
            str(row["customer_state"]),
            int(row["total_orders"]),
            float(row["avg_delay_days"]),
            float(row["late_rate"]),
            float(row["avg_review_score"])
        )
        for _, row in df_state.iterrows()
    ]

    client.execute(
        "INSERT INTO fpmci2026_db.state_summary VALUES",
        data_state
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

    df_seller["seller_id"] = (
        df_seller["seller_id"]
        .astype("string")
        .fillna("")
        .astype(str)
    )

    df_seller["total_orders"] = (
        pd.to_numeric(
            df_seller["total_orders"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    data_seller = [
        (
            str(row["seller_id"]),
            int(row["total_orders"]),
            float(row["sla_breach_rate"]),
            float(row["avg_delay_days"]),
            float(row["avg_review_score"])
        )
        for _, row in df_seller.iterrows()
    ]

    client.execute(
        "INSERT INTO fpmci2026_db.seller_summary VALUES",
        data_seller
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

    df_month["order_month"] = (
        df_month["order_month"]
        .astype("string")
        .fillna("")
        .astype(str)
    )

    df_month["total_orders"] = (
        pd.to_numeric(
            df_month["total_orders"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    data_month = [
        (
            str(row["order_month"]),
            int(row["total_orders"]),
            float(row["avg_delay_days"]),
            float(row["late_rate"])
        )
        for _, row in df_month.iterrows()
    ]

    client.execute(
        "INSERT INTO fpmci2026_db.monthly_trend VALUES",
        data_month
    )

    print("✅ monthly_trend loaded")

    # TABLE: sentiement review
    client.execute("""
        DROP TABLE IF EXISTS fpmci2026_db.fact_review_sentiment"""
    )

    client.execute("""
        CREATE TABLE fpmci2026_db.fact_review_sentiment (
            order_id String,
            review_score Int32,
            sentiment_label String,
            keyword String,
            delivery_delay_days Float64,
            customer_state String,
            seller_id String
        )
        ENGINE = MergeTree()
        ORDER BY order_id
    """)

    review_sentiment["review_comment_message"] = (
        review_sentiment["review_comment_message"]
        .fillna("")
        .astype(str)
    )

    sentiment_data = [
        (
            str(row["order_id"]),
            int(
                0 if pd.isna(row["review_score"])
                else row["review_score"]
            ),
            str(row["sentiment_label"]),
            str(row["keyword"]),
            float(
                0 if pd.isna(row["delivery_delay_days"])
                else row["delivery_delay_days"]
            ),
            str(row["customer_state"]),
            str(row["seller_id"])
        )
        for _, row in review_sentiment.iterrows()
    ]

    client.execute(
        """INSERT INTO fpmci2026_db.fact_review_sentiment VALUES""",
        sentiment_data
    )

    print("✅ review_sentiment loaded")

    spark.stop()

    print("✅ Pipeline selesai")


if __name__ == "__main__":
    run_spark_analytics()
