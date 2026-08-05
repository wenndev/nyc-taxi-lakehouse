# Resumo:
# - Valida a Silver do Taxi Zone Lookup depois da publicacao.
# - Confere 265 zonas, chaves unicas e textos obrigatorios preenchidos.

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import taxi_zone_lookup_silver_dir
from v2.config.spark import create_spark
from v2.pipelines.silver.validate_silver_common import (
    SilverValidationCheck,
    SilverValidationResult,
    check_equals,
    count_duplicate_keys,
    count_nulls,
    count_rows,
    delta_table_exists,
    has_columns,
    print_validation_result,
)


@dataclass(frozen=True)
class SilverTaxiZoneLookupValidationConfig:
    expected_locations: int = 265


def run_silver_taxi_zone_lookup_validation(
    spark: SparkSession,
    input_path: str,
    config: SilverTaxiZoneLookupValidationConfig,
) -> SilverValidationResult:
    df = spark.read.format("delta").load(input_path)
    return validate_silver_taxi_zone_lookup_table(df=df, config=config)


def validate_silver_taxi_zone_lookup_table(
    df: DataFrame,
    config: SilverTaxiZoneLookupValidationConfig,
) -> SilverValidationResult:
    checks: list[SilverValidationCheck] = []
    checks.extend(validate_required_columns(df))

    if has_columns(df, ("location_id",)):
        checks.extend(
            [
                check_equals(
                    name="lookup_rows",
                    actual=df.count(),
                    expected=config.expected_locations,
                    message="Taxi Zone Lookup deve conter as zonas oficiais.",
                ),
                check_equals(
                    name="lookup_duplicate_location_id",
                    actual=count_duplicate_keys(df, ("location_id",)),
                    expected=0,
                    message="location_id deve ser unico.",
                ),
                check_equals(
                    name="lookup_invalid_location_id",
                    actual=count_rows(
                        df,
                        (F.col("location_id") < 1) | (F.col("location_id") > 265),
                    ),
                    expected=0,
                    message="location_id deve estar entre 1 e 265.",
                ),
                check_equals(
                    name="lookup_null_location_id",
                    actual=count_nulls(df, "location_id"),
                    expected=0,
                    message="location_id nao deve ser nulo.",
                ),
            ]
        )

    checks.extend(validate_text_columns(df))

    return SilverValidationResult(checks=tuple(checks))


def validate_required_columns(df: DataFrame) -> list[SilverValidationCheck]:
    required_columns = ("location_id", "borough", "zona", "zona_servico")
    missing_columns = tuple(
        column_name for column_name in required_columns if column_name not in df.columns
    )

    return [
        check_equals(
            name="lookup_required_columns",
            actual=list(missing_columns),
            expected=[],
            message="Taxi Zone Lookup Silver deve conter as colunas obrigatorias.",
        )
    ]


def validate_text_columns(df: DataFrame) -> list[SilverValidationCheck]:
    checks = []

    for column_name in ("borough", "zona", "zona_servico"):
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"lookup_invalid_{column_name}",
                    actual=count_rows(
                        df,
                        F.col(column_name).isNull()
                        | (F.trim(F.col(column_name)) == ""),
                    ),
                    expected=0,
                    message=f"{column_name} nao deve ser nula ou vazia.",
                )
            )

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Taxi Zone Lookup Silver Delta table"
    )
    parser.add_argument("--input", default=None)
    parser.add_argument("--expected-locations", type=int, default=265)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(taxi_zone_lookup_silver_dir())
    config = SilverTaxiZoneLookupValidationConfig(
        expected_locations=args.expected_locations
    )

    print(f"Input             : {input_path}")
    print(f"Expected locations: {config.expected_locations}")

    if args.dry_run:
        return 0

    if not delta_table_exists(input_path):
        print(f"Silver Taxi Zone Lookup Delta table not found: {input_path}")
        print("Run first: poetry run silver-taxi-zone-lookup")
        return 1

    spark = create_spark("ValidateSilverTaxiZoneLookup")

    try:
        result = run_silver_taxi_zone_lookup_validation(
            spark=spark,
            input_path=input_path,
            config=config,
        )
        print_validation_result(result, title="Silver Taxi Zone Lookup validation")
        return 0 if result.passed else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
