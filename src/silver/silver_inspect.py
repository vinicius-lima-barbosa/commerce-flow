from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"

DATASETS = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
]

def inspect_dataset(spark: SparkSession, dataset: str) -> None:
    path = BRONZE_DATA_PATH / dataset
    df = spark.read.parquet(str(path))

    print(f"\n{'=' * 60}")
    print(f"DATASET: {dataset}")
    print(f"{'=' * 60}")

    print(f"Registros: {df.count()}")

    print("\nSchema:")
    df.printSchema()

    null_counts = df.select(
        *[
            count(
                when(
                    col(column).isNull() | (col(column) == ""),
                    column,
                )
            ).alias(column)
            for column in df.columns
        ]
    )

    print("\nValores nulos ou vazios:")
    null_counts.show(truncate=False)

    print("\nAmostra:")
    df.show(5, truncate=False)

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Silver Inspection")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        for dataset in DATASETS:
            inspect_dataset(spark, dataset)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()