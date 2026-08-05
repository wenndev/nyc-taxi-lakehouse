# Resumo:
# - Valida a Silver NOAA depois da publicacao.
# - Confere cobertura anual, grao estacao/dia, estacoes e metricas climaticas.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import noaa_silver_dir
from v2.config.spark import create_spark
from v2.config.sources import NOAA_GHCND_NYC_STORAGE_ID
from v2.pipelines.silver.validate_silver_common import (
    SilverValidationCheck,
    SilverValidationResult,
    check_at_least,
    check_equals,
    collect_date_stats,
    count_distinct,
    count_duplicate_keys,
    count_nulls,
    count_rows,
    days_in_year,
    delta_table_exists,
    has_columns,
    print_validation_result,
)


@dataclass(frozen=True)
class SilverNOAAValidationConfig:
    year: int = 2025
    expected_days: int = 365
    min_rows: int = 365
    min_stations: int = 2


def run_silver_noaa_weather_validation(
    spark: SparkSession,
    input_path: str,
    config: SilverNOAAValidationConfig,
) -> SilverValidationResult:
    df = spark.read.format("delta").load(input_path)
    return validate_silver_noaa_weather_table(df=df, config=config)


def validate_silver_noaa_weather_table(
    df: DataFrame,
    config: SilverNOAAValidationConfig,
) -> SilverValidationResult:
    checks: list[SilverValidationCheck] = []
    checks.extend(validate_required_columns(df))

    if has_columns(df, ("data_clima",)):
        start_date = date(config.year, 1, 1)
        end_date = date(config.year + 1, 1, 1)
        stats = collect_date_stats(df, "data_clima")
        validate_full_year = config.expected_days == days_in_year(config.year)

        checks.extend(
            [
                check_at_least(
                    name="noaa_rows",
                    actual=stats["total_rows"],
                    minimum=config.min_rows,
                    message="Silver NOAA deve ter linhas suficientes.",
                ),
                check_equals(
                    name="noaa_distinct_days",
                    actual=count_distinct(df, "data_clima"),
                    expected=config.expected_days,
                    message="Silver NOAA deve cobrir todos os dias esperados.",
                ),
                check_equals(
                    name="noaa_min_date",
                    actual=stats["min_date"],
                    expected=start_date,
                    message="Silver NOAA deve iniciar no primeiro dia do ano.",
                ),
                check_equals(
                    name="noaa_out_of_year_rows",
                    actual=count_rows(
                        df,
                        (F.col("data_clima") < F.lit(start_date))
                        | (F.col("data_clima") >= F.lit(end_date)),
                    ),
                    expected=0,
                    message="Silver NOAA deve conter apenas datas do ano esperado.",
                ),
            ]
        )
        if validate_full_year:
            checks.append(
                check_equals(
                    name="noaa_max_date",
                    actual=stats["max_date"],
                    expected=date(config.year, 12, 31),
                    message="Silver NOAA deve terminar no ultimo dia do ano.",
                )
            )

    if has_columns(df, ("id_estacao",)):
        checks.append(
            check_at_least(
                name="noaa_distinct_stations",
                actual=count_distinct(df, "id_estacao"),
                minimum=config.min_stations,
                message="Silver NOAA deve conter o minimo esperado de estacoes.",
            )
        )

    if has_columns(df, ("data_clima", "id_estacao")):
        checks.append(
            check_equals(
                name="noaa_duplicate_station_day",
                actual=count_duplicate_keys(df, ("data_clima", "id_estacao")),
                expected=0,
                message="Silver NOAA deve ter 1 linha por estacao/dia.",
            )
        )

    checks.extend(validate_not_null_columns(df))
    checks.extend(validate_weather_ranges(df))

    return SilverValidationResult(checks=tuple(checks))


def validate_required_columns(df: DataFrame) -> list[SilverValidationCheck]:
    required_columns = (
        "data_clima",
        "id_estacao",
        "precipitacao_mm",
        "temp_max_c",
        "temp_min_c",
        "temp_media_c",
        "amplitude_termica_c",
        "neve_mm",
        "neve_acumulada_mm",
        "qtd_tipos_dado",
        "ano",
        "mes",
        "dia_mes",
        "teve_chuva",
        "teve_neve",
        "categoria_chuva",
        "categoria_temperatura",
        "registro_clima_incompleto",
    )
    missing_columns = tuple(
        column_name for column_name in required_columns if column_name not in df.columns
    )

    return [
        check_equals(
            name="noaa_required_columns",
            actual=list(missing_columns),
            expected=[],
            message="Silver NOAA deve conter as colunas obrigatorias.",
        )
    ]


def validate_not_null_columns(df: DataFrame) -> list[SilverValidationCheck]:
    columns = (
        "data_clima",
        "id_estacao",
        "qtd_tipos_dado",
        "ano",
        "mes",
        "dia_mes",
        "registro_clima_incompleto",
    )
    checks = []

    for column_name in columns:
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"noaa_null_{column_name}",
                    actual=count_nulls(df, column_name),
                    expected=0,
                    message=f"{column_name} nao deve ser nula na Silver NOAA.",
                )
            )

    return checks


def validate_weather_ranges(df: DataFrame) -> list[SilverValidationCheck]:
    rules = {
        "qtd_tipos_dado": (F.col("qtd_tipos_dado") < 1)
        | (F.col("qtd_tipos_dado") > 5),
        "precipitacao_mm": F.col("precipitacao_mm") < 0,
        "neve_mm": F.col("neve_mm") < 0,
        "neve_acumulada_mm": F.col("neve_acumulada_mm") < 0,
        "temp_max_c": (F.col("temp_max_c") < -90) | (F.col("temp_max_c") > 60),
        "temp_min_c": (F.col("temp_min_c") < -90) | (F.col("temp_min_c") > 60),
    }
    checks = []

    for column_name, condition in rules.items():
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"noaa_invalid_{column_name}",
                    actual=count_rows(df, condition),
                    expected=0,
                    message=f"{column_name} deve respeitar faixa plausivel.",
                )
            )

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NOAA Weather Silver Delta")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--datasetid", default=NOAA_GHCND_NYC_STORAGE_ID)
    parser.add_argument("--input", default=None)
    parser.add_argument("--expected-days", type=int, default=None)
    parser.add_argument("--min-rows", type=int, default=365)
    parser.add_argument("--min-stations", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_path = (
        args.input
        if args.input
        else str(noaa_silver_dir(args.year, args.datasetid))
    )
    config = SilverNOAAValidationConfig(
        year=args.year,
        expected_days=args.expected_days or days_in_year(args.year),
        min_rows=args.min_rows,
        min_stations=args.min_stations,
    )

    print(f"Input         : {input_path}")
    print(f"Year          : {config.year}")
    print(f"Expected days : {config.expected_days}")
    print(f"Min rows      : {config.min_rows}")
    print(f"Min stations  : {config.min_stations}")

    if args.dry_run:
        return 0

    if not delta_table_exists(input_path):
        print(f"Silver NOAA Delta table not found: {input_path}")
        print("Run first: poetry run silver-noaa-weather")
        return 1

    spark = create_spark("ValidateSilverNOAAWeather")

    try:
        result = run_silver_noaa_weather_validation(
            spark=spark,
            input_path=input_path,
            config=config,
        )
        print_validation_result(result, title="Silver NOAA Weather validation")
        return 0 if result.passed else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
