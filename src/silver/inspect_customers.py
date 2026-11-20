from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Inspect Customers")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(
            str(BRONZE_DATA_PATH / "olist_customers_dataset")
        )

        print(f"Total de registros: {df.count()}")
        df.printSchema()

        print("\nValores nulos ou vazios:")
        for column in df.columns:
            null_count = df.filter(
                col(column).isNull() | (trim(col(column)) == "")
            ).count()

            print(f"{column}: {null_count}")

        print("\nDuplicidades em customer_id:")

        duplicates = (
            df.groupBy("customer_id")
            .agg(count("*").alias("total"))
            .filter(col("total") > 1)
        )

        print(f"IDs duplicados: {duplicates.count()}")
        duplicates.show()

        print("\nEstados encontrados:")
        df.select("customer_state").distinct().orderBy("customer_state").show()

        print("\nAmostra:")
        df.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()