from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Inspect Gold Orders")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(
            str(SILVER_DATA_PATH / "olist_orders_dataset")
        )

        print("Distribuição de status:")
        (
            df.groupBy("order_status")
            .count()
            .orderBy(col("count").desc())
            .show()
        )

        print("\nDisponibilidade das datas:")
        for column in df.columns:
            if column.endswith("timestamp") or column.endswith("date"):
                null_count = df.filter(col(column).isNull()).count()
                print(f"{column}: {null_count} nulos")

        print("\nIntervalo das datas:")
        df.select(
            min("order_purchase_timestamp").alias("primeira_compra"),
            max("order_purchase_timestamp").alias("ultima_compra"),
        ).show()

        print("\nAmostra de pedidos:")
        df.select(
            "order_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ).show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()