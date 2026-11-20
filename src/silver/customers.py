from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, trim, upper

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

def transform_customers(spark: SparkSession):
    source_path = BRONZE_DATA_PATH / "olist_customers_dataset"

    df = spark.read.parquet(str(source_path))

    df = (
        df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("customer_unique_id", trim(col("customer_unique_id")))
        .withColumn(
            "customer_zip_code_prefix",
            trim(col("customer_zip_code_prefix")),
        )
        .withColumn("customer_city", initcap(trim(col("customer_city"))))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
    )

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver - Customers")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_customers(spark)

        destination_path = SILVER_DATA_PATH / "olist_customers_dataset"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: olist_customers_dataset")
        print(f"Registros: {df.count()}")

        df.printSchema()
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()