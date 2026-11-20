from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

def transform_orders(spark: SparkSession):
    source_path = BRONZE_DATA_PATH / "olist_orders_dataset"

    df = spark.read.parquet(str(source_path))

    for column in TIMESTAMP_COLUMNS:
        df = df.withColumn(
            column,
            to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"),
        )

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver - Orders")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_orders(spark)

        destination_path = SILVER_DATA_PATH / "olist_orders_dataset"

        df.write.mode("overwrite").parquet(str(destination_path))

        print(f"Dataset processado: olist_orders_dataset")
        print(f"Registros: {df.count()}")

        df.printSchema()

    finally:
        spark.stop()

if __name__ == "__main__":
    run()