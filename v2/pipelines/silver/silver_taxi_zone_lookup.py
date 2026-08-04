# Resumo:
# - Cria a Silver do Taxi Zone Lookup.
# - Padroniza colunas, roda Data Quality e publica a referencia de localizacao.

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import (
    taxi_zone_lookup_bronze_dir,
    taxi_zone_lookup_quality_metrics_dir,
    taxi_zone_lookup_quarantine_dir,
    taxi_zone_lookup_silver_dir,
)
from v2.config.spark import create_spark
from v2.pipelines.quality.config import TaxiZoneLookupQualityConfig
from v2.pipelines.quality.exceptions import DataQualityCriticalError
from v2.pipelines.quality.models import QualityStatus
from v2.pipelines.quality.storage import write_quality_outputs
from v2.pipelines.quality.validators import validate_taxi_zone_lookup_data

COLUMN_RENAMES = {
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zona",
    "service_zone": "zona_servico",
}


def run_silver_taxi_zone_lookup(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
    quarantine_path: str | None = None,
    metrics_path: str | None = None,
    pipeline_run_id: str | None = None,
    enable_quality: bool = True,
    quality_config: TaxiZoneLookupQualityConfig | None = None,
) -> DataFrame:
    df = spark.read.format("delta").load(input_path)
    df = normalize_columns(df)

    if enable_quality:
        quality_result = validate_taxi_zone_lookup_data(
            df=df,
            config=quality_config or TaxiZoneLookupQualityConfig(),
            pipeline_run_id=pipeline_run_id,
        )
        print_quality_summary(quality_result.metrics.as_row())

        if quarantine_path and metrics_path:
            write_quality_outputs(
                result=quality_result,
                quarantine_path=quarantine_path,
                metrics_path=metrics_path,
                quarantine_mode=mode,
            )

        if quality_result.status == QualityStatus.FAIL:
            raise DataQualityCriticalError(
                "Taxi Zone Lookup Data Quality failed. Silver lookup was not published. "
                f"pipeline_run_id={quality_result.pipeline_run_id}"
            )

        df = quality_result.valid_records
    else:
        df = filter_required_columns(df).dropDuplicates(["location_id"])

    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(
        output_path
    )

    return df


def normalize_columns(df: DataFrame) -> DataFrame:
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df.select(
        F.col("location_id").cast("int").alias("location_id"),
        F.trim(F.col("borough")).alias("borough"),
        F.trim(F.col("zona")).alias("zona"),
        F.trim(F.col("zona_servico")).alias("zona_servico"),
    )


def filter_required_columns(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("location_id").isNotNull()
        & F.col("borough").isNotNull()
        & (F.trim(F.col("borough")) != "")
        & F.col("zona").isNotNull()
        & (F.trim(F.col("zona")) != "")
        & F.col("zona_servico").isNotNull()
        & (F.trim(F.col("zona_servico")) != "")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Taxi Zone Lookup Silver Delta")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--quarantine-output", default=None)
    parser.add_argument("--metrics-output", default=None)
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(taxi_zone_lookup_bronze_dir())
    output_path = args.output if args.output else str(taxi_zone_lookup_silver_dir())
    quarantine_path = (
        args.quarantine_output
        if args.quarantine_output
        else str(taxi_zone_lookup_quarantine_dir())
    )
    metrics_path = (
        args.metrics_output
        if args.metrics_output
        else str(taxi_zone_lookup_quality_metrics_dir())
    )

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print(f"Quarantine: {quarantine_path}")
    print(f"Metrics   : {metrics_path}")
    print("Format: delta -> delta")
    print("Steps : normalize columns, validate quality, save lookup silver")
    print(f"Quality: {'disabled' if args.skip_quality else 'enabled'}")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not (Path(input_path) / "_delta_log").exists():
        print(f"Bronze Delta not found: {input_path}")
        print("Run first: poetry run bronze-taxi-zone-lookup")
        return 1

    spark = create_spark("SilverTaxiZoneLookup")

    try:
        df_silver = run_silver_taxi_zone_lookup(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
            quarantine_path=quarantine_path,
            metrics_path=metrics_path,
            pipeline_run_id=args.pipeline_run_id,
            enable_quality=not args.skip_quality,
        )

        print("Silver Taxi Zone Lookup saved.")
        df_silver.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_silver.count()}")

        return 0
    finally:
        spark.stop()


def is_local_path(path: str) -> bool:
    return "://" not in path


def print_quality_summary(metrics: dict[str, object]) -> None:
    print("Data Quality Taxi Zone Lookup summary:")
    print(f"  pipeline_run_id    : {metrics['pipeline_run_id']}")
    print(f"  status             : {metrics['pipeline_status']}")
    print(f"  total_records      : {metrics['total_records']}")
    print(f"  valid_records      : {metrics['valid_records']}")
    print(f"  invalid_records    : {metrics['invalid_records']}")
    print(f"  quality_percentage : {metrics['quality_percentage']}")
    print(f"  duplicate_count    : {metrics['duplicate_count']}")
    print(f"  null_error_count   : {metrics['null_error_count']}")
    print(f"  range_error_count  : {metrics['range_error_count']}")


if __name__ == "__main__":
    raise SystemExit(main())
