from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Inspect Order Items")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(
            str(BRONZE_DATA_PATH / "olist_order_items_dataset")
        )

        print(f"Total de registros: {df.count()}")
        df.printSchema()

        print("\nValores nulos ou vazios:")
        for column in df.columns:
            null_count = df.filter(
                col(column).isNull() | (trim(col(column)) == "")
            ).count()

            print(f"{column}: {null_count}")

        print("\nValores negativos:")
        for column in ["price", "freight_value"]:
            negative_count = df.filter(
                col(column).cast("double") < 0
            ).count()

            print(f"{column}: {negative_count}")

        print("\nDuplicidades na chave order_id + order_item_id:")

        duplicates = (
            df.groupBy("order_id", "order_item_id")
            .agg(count("*").alias("total"))
            .filter(col("total") > 1)
        )

        print(f"Chaves duplicadas: {duplicates.count()}")
        duplicates.show()

        print("\nAmostra:")
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()