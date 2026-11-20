from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
    year,
    month,
    dayofmonth,
    when,
    to_date,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_DATA_PATH = PROJECT_ROOT / "data" / "gold"

def transform_orders_performance(spark: SparkSession):
    source_path = SILVER_DATA_PATH / "olist_orders_dataset"

    df = spark.read.parquet(str(source_path))

    df = df.withColumn(
        "purchase_date",
        to_date(col("order_purchase_timestamp")),
    )

    df = (
        df
        .withColumn("purchase_year", year(col("purchase_date")))
        .withColumn("purchase_month", month(col("purchase_date")))
        .withColumn("purchase_day", dayofmonth(col("purchase_date")))
        .withColumn(
            "delivery_days",
            when(
                col("order_status") == "delivered",
                datediff(
                    col("order_delivered_customer_date"),
                    col("purchase_date"),
                ),
            ),
        )
        .withColumn(
            "delivery_delay_days",
            when(
                col("order_status") == "delivered",
                datediff(
                    to_date(col("order_delivered_customer_date")),
                    to_date(col("order_estimated_delivery_date")),
                ),
            ),
        )
        .withColumn(
            "is_delivered_late",
            when(
                col("order_status") == "delivered",
                col("delivery_delay_days") > 0,
            ),
        )
    )

    df = df.select(
        "order_id",
        "order_status",
        "purchase_date",
        "purchase_year",
        "purchase_month",
        "purchase_day",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "delivery_delay_days",
        "is_delivered_late",
    )

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Gold - Orders Performance")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_orders_performance(spark)

        destination_path = GOLD_DATA_PATH / "orders_performance"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: orders_performance")
        print(f"Registros: {df.count()}")

        df.printSchema()
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()