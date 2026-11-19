from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"

DATASETS = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
]

def ingest_csv(spark: SparkSession, dataset: str) -> None:
    source_path = RAW_DATA_PATH / f"{dataset}.csv"
    destination_path = BRONZE_DATA_PATH / dataset

    df = (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .csv(str(source_path))
    )

    df.write.mode("overwrite").parquet(str(destination_path))

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Bronze")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        for dataset in DATASETS:
            ingest_csv(spark, dataset)
            print(f"Dataset processado: {dataset}")

    finally:
        spark.stop()

if __name__ == "__main__":
    run()