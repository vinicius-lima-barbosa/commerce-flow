from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DecimalType

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

def transform_order_items(spark: SparkSession):
    source_path = BRONZE_DATA_PATH / "olist_order_items_dataset"

    df = spark.read.parquet(str(source_path))

    df = (
        df
        .withColumn("order_item_id", col("order_item_id").cast("integer"))
        .withColumn(
            "shipping_limit_date",
            to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("price", col("price").cast(DecimalType(10, 2))) 
        .withColumn("freight_value", col("freight_value").cast(DecimalType(10, 2)))
    )

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver - Order Items")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_order_items(spark)

        destination_path = SILVER_DATA_PATH / "olist_order_items_dataset"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: olist_order_items_dataset")
        print(f"Registros: {df.count()}")

        df.printSchema()

    finally:
        spark.stop()

if __name__ == "__main__":
    run()