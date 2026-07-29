from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

from v2.config.paths import noaa_bronze_dir, noaa_raw_dir
from v2.config.spark import create_spark


def run_bronze_noaa_weather(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
) -> DataFrame:
    df = (
        spark.read.option("multiLine", "true")
        .json(json_page_pattern(input_path))
        .withColumn("arquivo_origem", input_file_name())
        .withColumn("data_processamento_bronze", current_timestamp())
    )

    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(output_path)

    return df


def json_page_pattern(input_path: str) -> str:
    return f"{input_path.rstrip('/')}/page_*.json"


def is_local_path(path: str) -> bool:
    return "://" not in path


def main() -> int:
    parser = argparse.ArgumentParser(description="Create NOAA Weather Bronze Delta table")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--datasetid", default="GHCND")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(noaa_raw_dir(args.year, args.datasetid))
    output_path = args.output if args.output else str(noaa_bronze_dir(args.year, args.datasetid))

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("Format: NOAA raw JSON -> delta")
    print(f"Files : {json_page_pattern(input_path)}")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not Path(input_path).exists():
        print(f"Input path not found: {input_path}")
        return 1

    spark = create_spark("BronzeNOAAWeather")

    try:
        df_bronze = run_bronze_noaa_weather(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
        )

        print("Bronze NOAA Weather saved.")
        df_bronze.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_bronze.count()}")

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
