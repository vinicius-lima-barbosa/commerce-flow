from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "silver"

INTEGER_COLUMNS = [
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

def transform_products(spark: SparkSession):
    source_path = BRONZE_DATA_PATH / "olist_products_dataset"

    df = spark.read.parquet(str(source_path))

    df = (
        df
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed(
            "product_description_lenght",
            "product_description_length",
        )
    )

    df = df.withColumn(
        "product_category_name",
        trim(col("product_category_name")),
    )

    for column in INTEGER_COLUMNS:
        df = df.withColumn(column, col(column).cast("integer"))

    return df

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver - Products")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = transform_products(spark)

        destination_path = SILVER_DATA_PATH / "olist_products_dataset"

        df.write.mode("overwrite").parquet(str(destination_path))

        print("Dataset processado: olist_products_dataset")
        print(f"Registros: {df.count()}")

        df.printSchema()
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()