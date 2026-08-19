# Resumo:
# - Le o CSV bruto Taxi Zone Lookup e salva como Delta Bronze.
# - Mantem as colunas originais da referencia oficial da NYC TLC.

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from v2.config.paths import taxi_zone_lookup_bronze_dir, taxi_zone_lookup_raw_dir
from v2.config.sources import TAXI_ZONE_LOOKUP_FILENAME
from v2.config.spark import create_spark
from v2.platform.delta import write_delta_table


def run_bronze_taxi_zone_lookup(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    write_delta_table(df, output_path, mode=mode)

    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Taxi Zone Lookup Bronze Delta")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = (
        args.input
        if args.input
        else str(taxi_zone_lookup_raw_dir() / TAXI_ZONE_LOOKUP_FILENAME)
    )
    output_path = args.output if args.output else str(taxi_zone_lookup_bronze_dir())

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("Format: csv -> delta")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not Path(input_path).exists():
        print(f"Input file not found: {input_path}")
        print("Run first: poetry run ingest-taxi-zone-lookup")
        return 1

    spark = create_spark("BronzeTaxiZoneLookup")

    try:
        df_bronze = run_bronze_taxi_zone_lookup(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
        )

        print("Bronze Taxi Zone Lookup saved.")
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
