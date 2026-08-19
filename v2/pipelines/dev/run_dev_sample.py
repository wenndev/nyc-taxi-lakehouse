# Resumo:
# - Orquestra um fluxo dev end-to-end com amostra local.
# - Reaproveita as funcoes PySpark oficiais de Bronze, Silver, Quality e Gold.

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from v2.config.paths import (
    noaa_silver_dir,
    nyc_tlc_raw_dir,
    taxi_zone_lookup_silver_dir,
)
from v2.config.spark import create_spark
from v2.pipelines.bronze.bronze_nyc_tlc import run_bronze_nyc_tlc
from v2.pipelines.gold.gold_daily_weather_demand import (
    run_gold_daily_weather_demand,
)
from v2.pipelines.gold.gold_star_schema import run_gold_star_schema
from v2.pipelines.gold.validate_gold_daily_weather_demand import (
    DailyWeatherDemandValidationConfig,
    print_validation_result as print_daily_validation_result,
    run_daily_weather_demand_validation,
)
from v2.pipelines.gold.validate_gold_star_schema import (
    GoldValidationConfig,
    days_in_year,
    print_validation_result as print_gold_validation_result,
    run_gold_star_schema_validation,
)
from v2.pipelines.silver.silver_nyc_tlc import run_silver_nyc_tlc
from v2.pipelines.silver.validate_silver_common import (
    delta_table_exists,
    print_validation_result as print_silver_validation_result,
)
from v2.pipelines.silver.validate_silver_noaa_weather import (
    SilverNOAAValidationConfig,
    run_silver_noaa_weather_validation,
)
from v2.pipelines.silver.validate_silver_nyc_tlc import (
    SilverNYCTLCValidationConfig,
    run_silver_nyc_tlc_validation,
)
from v2.pipelines.silver.validate_silver_taxi_zone_lookup import (
    SilverTaxiZoneLookupValidationConfig,
    run_silver_taxi_zone_lookup_validation,
)
from v2.platform.logging import configure_logging, get_logger, log_event
from v2.platform.run_context import RunContext


@dataclass(frozen=True)
class DevSamplePaths:
    raw_tlc_input: str
    bronze_tlc_output: str
    silver_tlc_output: str
    quarantine_tlc_output: str
    metrics_tlc_output: str
    taxi_zone_lookup_input: str
    noaa_input: str
    gold_star_schema_output: str
    gold_daily_weather_demand_output: str


def run_dev_sample(
    paths: DevSamplePaths,
    year: int = 2025,
    start_date: str = "2025-01-01",
    end_date: str = "2025-02-01",
    sample_rows: int = 100000,
    expected_tlc_days: int = 2,
    mode: str = "overwrite",
    run_context: RunContext | None = None,
) -> None:
    validate_required_inputs(paths)

    context = run_context or RunContext.create(
        pipeline_name="run-v2-dev-sample",
        year=year,
        start_date=start_date,
        end_date=end_date,
        batch_id=f"{year}_{start_date}_{end_date}",
    )
    logger = get_logger(__name__)
    log_event(
        logger,
        "pipeline_start",
        context=context,
        sample_rows=sample_rows,
        expected_tlc_days=expected_tlc_days,
        mode=mode,
    )

    spark = create_spark("RunV2DevSample")

    try:
        print_stage("1/8 Bronze NYC TLC sample")
        run_bronze_nyc_tlc(
            spark=spark,
            input_path=paths.raw_tlc_input,
            output_path=paths.bronze_tlc_output,
            mode=mode,
            limit_rows=sample_rows,
        )

        print_stage("2/8 Silver NYC TLC sample")
        run_silver_nyc_tlc(
            spark=spark,
            input_path=paths.bronze_tlc_output,
            output_path=paths.silver_tlc_output,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            quarantine_path=paths.quarantine_tlc_output,
            metrics_path=paths.metrics_tlc_output,
            pipeline_run_id=context.pipeline_run_id,
            enable_quality=True,
        )

        print_stage("3/8 Validate Silver")
        silver_tlc_result = run_silver_nyc_tlc_validation(
            spark=spark,
            input_path=paths.silver_tlc_output,
            config=SilverNYCTLCValidationConfig(
                year=year,
                min_rows=1,
                expected_days=expected_tlc_days,
            ),
        )
        print_silver_validation_result(
            silver_tlc_result,
            title="Silver NYC TLC dev validation",
        )
        fail_if_not_passed(silver_tlc_result.passed, "Silver NYC TLC dev")

        lookup_result = run_silver_taxi_zone_lookup_validation(
            spark=spark,
            input_path=paths.taxi_zone_lookup_input,
            config=SilverTaxiZoneLookupValidationConfig(expected_locations=265),
        )
        print_silver_validation_result(
            lookup_result,
            title="Silver Taxi Zone Lookup validation",
        )
        fail_if_not_passed(lookup_result.passed, "Silver Taxi Zone Lookup")

        noaa_result = run_silver_noaa_weather_validation(
            spark=spark,
            input_path=paths.noaa_input,
            config=SilverNOAAValidationConfig(
                year=year,
                expected_days=days_in_year(year),
                min_rows=365,
                min_stations=2,
            ),
        )
        print_silver_validation_result(
            noaa_result,
            title="Silver NOAA Weather validation",
        )
        fail_if_not_passed(noaa_result.passed, "Silver NOAA Weather")

        print_stage("4/8 Gold Star Schema dev")
        run_gold_star_schema(
            spark=spark,
            tlc_input_path=paths.silver_tlc_output,
            noaa_input_path=paths.noaa_input,
            taxi_zone_lookup_input_path=paths.taxi_zone_lookup_input,
            output_path=paths.gold_star_schema_output,
            year=year,
            mode=mode,
        )

        print_stage("5/8 Validate Gold Star Schema dev")
        gold_result = run_gold_star_schema_validation(
            spark=spark,
            input_path=paths.gold_star_schema_output,
            config=GoldValidationConfig(
                expected_days=days_in_year(year),
                expected_locations=265,
                min_fact_rows=1,
                require_complete_weather=True,
            ),
        )
        print_gold_validation_result(gold_result)
        fail_if_not_passed(gold_result.passed, "Gold Star Schema dev")

        print_stage("6/8 Gold Daily Weather Demand dev")
        run_gold_daily_weather_demand(
            spark=spark,
            tlc_input_path=paths.silver_tlc_output,
            noaa_input_path=paths.noaa_input,
            output_path=paths.gold_daily_weather_demand_output,
            year=year,
            mode=mode,
        )

        print_stage("7/8 Validate Gold Daily Weather Demand dev")
        daily_result = run_daily_weather_demand_validation(
            spark=spark,
            input_path=paths.gold_daily_weather_demand_output,
            config=DailyWeatherDemandValidationConfig(
                expected_days=days_in_year(year),
                min_days_with_demand=1,
                min_total_trips=1,
                require_complete_weather=True,
            ),
            year=year,
        )
        print_daily_validation_result(daily_result)
        fail_if_not_passed(daily_result.passed, "Gold Daily Weather Demand dev")

        print_stage("8/8 Dev sample end-to-end PASS")
        print("Fluxo dev validado de ponta a ponta.")
        log_event(logger, "pipeline_success", context=context)
    except Exception as exc:
        log_event(
            logger,
            "pipeline_failure",
            context=context,
            level=logging.ERROR,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise
    finally:
        spark.stop()


def validate_required_inputs(paths: DevSamplePaths) -> None:
    if not Path(paths.raw_tlc_input).exists():
        raise FileNotFoundError(
            f"Raw TLC sample input not found: {paths.raw_tlc_input}"
        )

    for label, path in (
        ("Silver Taxi Zone Lookup", paths.taxi_zone_lookup_input),
        ("Silver NOAA Weather", paths.noaa_input),
    ):
        if not delta_table_exists(path):
            raise FileNotFoundError(f"{label} Delta table not found: {path}")


def fail_if_not_passed(passed: bool, label: str) -> None:
    if not passed:
        raise RuntimeError(f"{label} validation failed.")


def print_stage(message: str) -> None:
    print("")
    print("=" * 80)
    print(message)
    print("=" * 80)


def default_paths(year: int, month: int) -> DevSamplePaths:
    month_label = f"{month:02d}"
    sample_id = f"{year}_{month_label}"
    dev_root = Path("v2/data/delta/dev")

    return DevSamplePaths(
        raw_tlc_input=str(
            nyc_tlc_raw_dir(year) / f"yellow_tripdata_{year}-{month_label}.parquet"
        ),
        bronze_tlc_output=str(
            dev_root / "bronze" / "nyc_tlc" / "yellow_sample" / sample_id
        ),
        silver_tlc_output=str(
            dev_root / "silver" / "nyc_tlc" / "yellow_sample" / sample_id
        ),
        quarantine_tlc_output=str(
            dev_root / "quarantine" / "nyc_tlc" / "yellow_sample" / sample_id
        ),
        metrics_tlc_output=str(
            dev_root / "monitoring" / "quality" / "nyc_tlc" / "yellow_sample" / sample_id
        ),
        taxi_zone_lookup_input=str(taxi_zone_lookup_silver_dir()),
        noaa_input=str(noaa_silver_dir(year)),
        gold_star_schema_output=str(dev_root / "gold" / "star_schema" / sample_id),
        gold_daily_weather_demand_output=str(
            dev_root / "gold" / "daily_weather_demand" / sample_id
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local dev end-to-end sample for NYC Taxi Lakehouse V2"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--sample-rows", type=int, default=100000)
    parser.add_argument("--expected-tlc-days", type=int, default=2)
    parser.add_argument("--raw-tlc-input", default=None)
    parser.add_argument("--bronze-output", default=None)
    parser.add_argument("--silver-output", default=None)
    parser.add_argument("--quarantine-output", default=None)
    parser.add_argument("--metrics-output", default=None)
    parser.add_argument("--taxi-zone-lookup-input", default=None)
    parser.add_argument("--noaa-input", default=None)
    parser.add_argument("--gold-star-schema-output", default=None)
    parser.add_argument("--gold-daily-output", default=None)
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def apply_overrides(paths: DevSamplePaths, args: argparse.Namespace) -> DevSamplePaths:
    return DevSamplePaths(
        raw_tlc_input=args.raw_tlc_input or paths.raw_tlc_input,
        bronze_tlc_output=args.bronze_output or paths.bronze_tlc_output,
        silver_tlc_output=args.silver_output or paths.silver_tlc_output,
        quarantine_tlc_output=args.quarantine_output or paths.quarantine_tlc_output,
        metrics_tlc_output=args.metrics_output or paths.metrics_tlc_output,
        taxi_zone_lookup_input=args.taxi_zone_lookup_input or paths.taxi_zone_lookup_input,
        noaa_input=args.noaa_input or paths.noaa_input,
        gold_star_schema_output=(
            args.gold_star_schema_output or paths.gold_star_schema_output
        ),
        gold_daily_weather_demand_output=(
            args.gold_daily_output or paths.gold_daily_weather_demand_output
        ),
    )


def print_plan(
    paths: DevSamplePaths,
    args: argparse.Namespace,
    context: RunContext,
) -> None:
    start_date, end_date = resolve_date_range(args)
    print("Run V2 dev sample")
    print(f"Year             : {args.year}")
    print(f"Month            : {args.month:02d}")
    print(f"Start date       : {start_date}")
    print(f"End date         : {end_date}")
    print(f"Pipeline run id  : {context.pipeline_run_id}")
    print(f"Sample rows      : {args.sample_rows}")
    print(f"Expected TLC days: {args.expected_tlc_days}")
    print(f"Mode             : {args.mode}")
    print("")
    print(f"Raw TLC input    : {paths.raw_tlc_input}")
    print(f"Bronze output    : {paths.bronze_tlc_output}")
    print(f"Silver output    : {paths.silver_tlc_output}")
    print(f"Quarantine output: {paths.quarantine_tlc_output}")
    print(f"Metrics output   : {paths.metrics_tlc_output}")
    print(f"Lookup input     : {paths.taxi_zone_lookup_input}")
    print(f"NOAA input       : {paths.noaa_input}")
    print(f"Gold star output : {paths.gold_star_schema_output}")
    print(f"Gold daily output: {paths.gold_daily_weather_demand_output}")


def resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    start_date = args.start_date or date(args.year, args.month, 1).isoformat()
    if args.end_date:
        return start_date, args.end_date

    next_month_year = args.year + 1 if args.month == 12 else args.year
    next_month = 1 if args.month == 12 else args.month + 1
    end_date = date(next_month_year, next_month, 1).isoformat()
    return start_date, end_date


def main() -> int:
    configure_logging()
    args = parse_args()
    paths = apply_overrides(default_paths(args.year, args.month), args)
    start_date, end_date = resolve_date_range(args)
    context = RunContext.create(
        pipeline_name="run-v2-dev-sample",
        year=args.year,
        start_date=start_date,
        end_date=end_date,
        batch_id=f"{args.year}_{start_date}_{end_date}",
        pipeline_run_id=args.pipeline_run_id,
    )
    print_plan(paths, args, context)

    if args.dry_run:
        return 0

    try:
        run_dev_sample(
            paths=paths,
            year=args.year,
            start_date=start_date,
            end_date=end_date,
            sample_rows=args.sample_rows,
            expected_tlc_days=args.expected_tlc_days,
            mode=args.mode,
            run_context=context,
        )
    except Exception as exc:
        print(f"Dev sample failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
