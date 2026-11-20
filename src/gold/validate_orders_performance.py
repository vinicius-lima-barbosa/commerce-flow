from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, sum

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DATA_PATH = PROJECT_ROOT / "data" / "gold"

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Validate Orders Performance")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(
            str(GOLD_DATA_PATH / "orders_performance")
        )

        print("Total de registros:")
        print(df.count())

        print("\nDuplicidades em order_id:")
        duplicates = (
            df.groupBy("order_id")
            .count()
            .filter(col("count") > 1)
        )
        print(duplicates.count())

        print("\nPedidos entregues sem data de entrega:")
        delivered_missing_date = df.filter(
            (col("order_status") == "delivered")
            & col("order_delivered_customer_date").isNull()
        )
        print(delivered_missing_date.count())

        print("\nDistribuição de métricas por status:")
        (
            df.groupBy("order_status")
            .agg(
                count("*").alias("total_orders"),
                count("delivery_days").alias("orders_with_delivery_days"),
                count("delivery_delay_days").alias("orders_with_delay"),
            )
            .orderBy(col("total_orders").desc())
            .show()
        )

        print("\nResumo das métricas de entrega:")
        (
            df.filter(col("order_status") == "delivered")
            .agg(
                min("delivery_days").alias("min_delivery_days"),
                max("delivery_days").alias("max_delivery_days"),
                avg("delivery_days").alias("avg_delivery_days"),
                min("delivery_delay_days").alias("min_delay_days"),
                max("delivery_delay_days").alias("max_delay_days"),
                avg("delivery_delay_days").alias("avg_delay_days"),
                sum(
                    col("is_delivered_late").cast("integer")
                ).alias("late_deliveries")
            )
            .show()
        )

        print("\nDistribuição de entregas antecipadas, no prazo e atrasadas:")
        (
            df.filter(col("order_status") == "delivered")
            .groupBy("is_delivered_late")
            .count()
            .orderBy("is_delivered_late")
            .show()
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    run()