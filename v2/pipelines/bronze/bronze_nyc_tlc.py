# Resumo:
# - Le os Parquets brutos da NYC TLC e salva como Delta Bronze.
# - Mantem a estrutura original para preservar o dado de entrada.

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

from v2.config.paths import nyc_tlc_bronze_dir, nyc_tlc_raw_dir
from v2.config.spark import create_spark


def run_bronze_nyc_tlc(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
    limit_rows: int | None = None,
) -> DataFrame:
    df = spark.read.parquet(input_path)

    if limit_rows:
        df = df.limit(limit_rows)

    df = (
        df.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
        .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
        .withColumn("payment_type", col("payment_type").cast(IntegerType()))
    )

    df.write.format("delta").mode(mode).save(output_path)

    return df


# Rodar localmente com poetry
def main() -> int:
    parser = argparse.ArgumentParser(description="Create NYC TLC Bronze Delta table")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(nyc_tlc_raw_dir(args.year))
    output_path = args.output if args.output else str(nyc_tlc_bronze_dir(args.year))

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("Format: parquet -> delta")
    if args.limit:
        print(f"Limit : {args.limit} rows")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not Path(input_path).exists():
        print(f"Input path not found: {input_path}")
        return 1

    spark = create_spark("BronzeNYCTLC")

    try:
        df_bronze = run_bronze_nyc_tlc(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
            limit_rows=args.limit,
        )

        print("Bronze NYC TLC saved.")
        df_bronze.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_bronze.count()}")

        return 0
    finally:
        spark.stop()


def is_local_path(path: str) -> bool:
    return "://" not in path


if __name__ == "__main__":
    raise SystemExit(main())
