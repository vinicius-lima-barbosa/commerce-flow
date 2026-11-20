from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DATA_PATH = PROJECT_ROOT / "data" / "gold"

def run() -> None:
    spark = (
        SparkSession.builder
        .appName("CommerceFlow - Validate Monthly Orders")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        orders_df = spark.read.parquet(
            str(GOLD_DATA_PATH / "orders_performance")
        )

        monthly_df = spark.read.parquet(
            str(GOLD_DATA_PATH / "monthly_orders_performance")
        )

        print("Total na tabela detalhada:")
        print(orders_df.count())

        print("\nTotal contabilizado na agregação:")
        print(monthly_df.agg(
            sum("total_orders")
        ).first()[0])

        print("\nDuplicidades na granularidade mensal:")
        duplicates = (
            monthly_df.groupBy("purchase_year", "purchase_month")
            .count()
            .filter(col("count") > 1)
        )

        print(duplicates.count())

    finally:
        spark.stop()

if __name__ == "__main__":
    run()