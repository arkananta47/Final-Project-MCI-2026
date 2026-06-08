import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    print("Membuka keran data: API Orders...")
    url = "http://96.9.212.102:8000/orders"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    orders = data["orders"]
    parsed_data = []

    for order in orders:
        order_id = str(order.get("order_id"))
        user_id = str(order.get("user_id"))
        order_number = int(order.get("order_number", 0))
        order_dow = int(order.get("order_dow", 0))
        order_hour_of_day = int(
            order.get("order_hour_of_day", 0)
        )
        days_since_prior_order = (
            int(order.get("days_since_prior_order"))
            if order.get("days_since_prior_order") is not None
            else 0
        )
        eval_set = str(order.get("eval_set"))
        order_date = datetime.now().strftime("%Y-%m-%d")
        products = order.get("products", [])

        for product in products:
            parsed_data.append({
                "order_id": order_id,
                "user_id": user_id,
                "order_number": order_number,
                "order_dow": order_dow,
                "order_hour_of_day": order_hour_of_day,
                "days_since_prior_order":
                    days_since_prior_order,
                "eval_set": eval_set,
                "product_id": str(
                    product.get("product_id")
                ),
                "product_name": str(
                    product.get("product_name")
                ),
                "aisle_id": int(
                    product.get("aisle_id", 0)
                ),
                "aisle": str(
                    product.get("aisle")
                ),
                "department_id": int(
                    product.get("department_id", 0)
                ),
                "department": str(
                    product.get("department")
                ),
                "add_to_cart_order": int(
                    product.get("add_to_cart_order", 0)
                ),
                "reordered": int(
                    product.get("reordered", 0)
                ),
                "order_date": order_date,
                "updated_at": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "ingested_at": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            })

    df = pd.DataFrame(parsed_data)

    output_dir = "/opt/airflow/data_lake/orders"

    os.makedirs(output_dir, exist_ok=True)
    output_path = (
        f"{output_dir}/orders_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    df.to_parquet(output_path, index=False)

    print(f"✅ Berhasil menyimpan {len(df)} rows")

if __name__ == "__main__":
    fetch_orders()