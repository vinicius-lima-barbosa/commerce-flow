from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, trim, upper

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

def transform_sellers(spark: SparkSession):
    source_path = BRONZE_DATA_PATH / "olist_sellers_dataset"

    df = spark.read.parquet(str(source_path))

    df = (
        df
        .withColumn("seller_id", trim(col("seller_id")))
        .withColumn(
            "seller_zip_code_prefix",
            trim(col("seller_zip_code_prefix")),
        )
        .withColumn("seller_city", initcap(trim(col("seller_city"))))
        .withColumn("seller_state", upper(trim(col("seller_state"))))
    )

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver - Sellers")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_sellers(spark)

        destination_path = SILVER_DATA_PATH / "olist_sellers_dataset"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: olist_sellers_dataset")
        print(f"Registros: {df.count()}")

        df.printSchema()
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()