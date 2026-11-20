from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round,
    when,
    try_divide
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DATA_PATH = PROJECT_ROOT / "data" / "gold"

def transform_monthly_orders_performance(spark: SparkSession):
    source_path = GOLD_DATA_PATH / "orders_performance"

    df = spark.read.parquet(str(source_path))

    monthly_df = (
        df.groupBy("purchase_year", "purchase_month")
        .agg(
            count("*").alias("total_orders"),
            sum(
                when(col("order_status") == "delivered", 1).otherwise(0)
            ).alias("delivered_orders"),
            sum(
                when(col("order_status") == "canceled", 1).otherwise(0)
            ).alias("canceled_orders"),
            sum(
                when(col("is_delivered_late") == True, 1).otherwise(0)
            ).alias("late_deliveries"),
            avg("delivery_days").alias("avg_delivery_days"),
            count("delivery_days").alias("orders_with_delivery_data"),
        )
    )

    monthly_df = (
        monthly_df
        .withColumn(
            "delivery_rate",
            round(
                col("delivered_orders") / col("total_orders") * 100,
                2,
            ),
        )
        .withColumn(
            "cancellation_rate",
            round(
                col("canceled_orders") / col("total_orders") * 100,
                2,
            ),
        )
        .withColumn(
            "late_delivery_rate",
            round(
                try_divide(col("late_deliveries"), col("orders_with_delivery_data")) * 100,
                2,
            ),
        )
        .withColumn("avg_delivery_days", round(col("avg_delivery_days"), 2))
    )

    return monthly_df.orderBy("purchase_year", "purchase_month")

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Gold - Monthly Orders Performance")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_monthly_orders_performance(spark)

        destination_path = GOLD_DATA_PATH / "monthly_orders_performance"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: monthly_orders_performance")
        print(f"Registros: {df.count()}")

        df.printSchema()
        df.show(50, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()