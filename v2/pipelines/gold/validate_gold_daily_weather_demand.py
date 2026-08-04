# Resumo:
# - Valida a Gold diaria usada em EDA e ML.
# - Garante calendario completo, clima alinhado e alvo de demanda consistente.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import daily_weather_demand_gold_dir
from v2.config.spark import create_spark
from v2.pipelines.gold.validate_gold_star_schema import (
    GoldValidationCheck,
    GoldValidationResult,
    check_at_least,
    check_equals,
    count_duplicate_keys,
    count_nulls,
    count_true_values,
    days_in_year,
    delta_table_exists,
    has_columns,
)


@dataclass(frozen=True)
class DailyWeatherDemandValidationConfig:
    expected_days: int
    min_days_with_demand: int = 1
    min_total_trips: int = 1
    require_complete_weather: bool = True


def run_daily_weather_demand_validation(
    spark: SparkSession,
    input_path: str,
    config: DailyWeatherDemandValidationConfig,
    year: int = 2025,
) -> GoldValidationResult:
    df = spark.read.format("delta").load(input_path)
    return validate_daily_weather_demand_table(df=df, config=config, year=year)


def validate_daily_weather_demand_table(
    df: DataFrame,
    config: DailyWeatherDemandValidationConfig,
    year: int = 2025,
) -> GoldValidationResult:
    checks: list[GoldValidationCheck] = []
    checks.extend(validate_required_columns(df))

    if has_columns(df, ("data",)):
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        data_stats = collect_data_stats(df)

        checks.extend(
            [
                check_equals(
                    name="daily_rows",
                    actual=data_stats["total_rows"],
                    expected=config.expected_days,
                    message="daily_weather_demand deve ter 1 linha por dia.",
                ),
                check_equals(
                    name="daily_duplicate_dates",
                    actual=count_duplicate_keys(df, "data"),
                    expected=0,
                    message="A coluna data deve ser unica.",
                ),
                check_equals(
                    name="daily_null_dates",
                    actual=count_nulls(df, "data"),
                    expected=0,
                    message="A coluna data nao deve ser nula.",
                ),
                check_equals(
                    name="daily_min_date",
                    actual=data_stats["min_date"],
                    expected=start_date,
                    message="A menor data deve ser o inicio do ano.",
                ),
                check_equals(
                    name="daily_max_date",
                    actual=data_stats["max_date"],
                    expected=end_date,
                    message="A maior data deve ser o fim do ano.",
                ),
            ]
        )

    if has_columns(df, ("qtd_corridas",)):
        demand_stats = collect_demand_stats(df)

        checks.extend(
            [
                check_equals(
                    name="daily_null_qtd_corridas",
                    actual=count_nulls(df, "qtd_corridas"),
                    expected=0,
                    message="qtd_corridas nao deve ser nula.",
                ),
                check_equals(
                    name="daily_negative_qtd_corridas",
                    actual=demand_stats["negative_demand_days"],
                    expected=0,
                    message="qtd_corridas nao deve ser negativa.",
                ),
                check_at_least(
                    name="daily_days_with_demand",
                    actual=demand_stats["days_with_demand"],
                    minimum=config.min_days_with_demand,
                    message="A base deve ter dias com demanda de taxi.",
                ),
                check_at_least(
                    name="daily_total_trips",
                    actual=demand_stats["total_trips"],
                    minimum=config.min_total_trips,
                    message="A soma de qtd_corridas deve ser maior que zero.",
                ),
            ]
        )

    if config.require_complete_weather:
        checks.extend(validate_weather_completeness(df))

    return GoldValidationResult(checks=tuple(checks))


def validate_required_columns(df: DataFrame) -> list[GoldValidationCheck]:
    required_columns = (
        "data",
        "ano",
        "mes",
        "dia_mes",
        "dia_semana_num",
        "fim_de_semana",
        "qtd_corridas",
        "valor_total_corridas",
        "qtd_registros_suspeitos",
        "fonte_clima",
        "escopo_clima",
        "qtd_estacoes",
        "cobertura_estacoes_pct",
        "precipitacao_media_mm",
        "precipitacao_max_mm",
        "temp_media_c",
        "temp_max_media_c",
        "temp_min_media_c",
        "teve_chuva",
        "teve_neve",
        "categoria_chuva",
        "categoria_temperatura",
        "sem_corridas",
        "sem_clima",
        "registro_alinhamento_incompleto",
    )
    missing_columns = tuple(
        column_name for column_name in required_columns if column_name not in df.columns
    )

    return [
        check_equals(
            name="daily_required_columns",
            actual=list(missing_columns),
            expected=[],
            message="daily_weather_demand deve conter as colunas obrigatorias.",
        )
    ]


def validate_weather_completeness(df: DataFrame) -> list[GoldValidationCheck]:
    checks: list[GoldValidationCheck] = []

    if "sem_clima" in df.columns:
        checks.append(
            check_equals(
                name="daily_days_without_weather",
                actual=count_true_values(df, "sem_clima"),
                expected=0,
                message="Nenhum dia deve ficar sem clima associado.",
            )
        )

    if "registro_alinhamento_incompleto" in df.columns:
        checks.append(
            check_equals(
                name="daily_incomplete_alignment",
                actual=count_true_values(df, "registro_alinhamento_incompleto"),
                expected=0,
                message="Nao deve haver falha de alinhamento entre clima e demanda.",
            )
        )

    weather_columns = (
        "fonte_clima",
        "escopo_clima",
        "qtd_estacoes",
        "cobertura_estacoes_pct",
        "precipitacao_media_mm",
        "precipitacao_max_mm",
        "temp_media_c",
        "temp_max_media_c",
        "temp_min_media_c",
        "teve_chuva",
        "teve_neve",
        "categoria_chuva",
        "categoria_temperatura",
    )

    for column_name in weather_columns:
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"daily_null_{column_name}",
                    actual=count_nulls(df, column_name),
                    expected=0,
                    message=f"{column_name} nao deve ser nula na base diaria.",
                )
            )

    if "qtd_estacoes" in df.columns:
        checks.append(
            check_equals(
                name="daily_days_without_stations",
                actual=count_rows(df, F.col("qtd_estacoes") <= 0),
                expected=0,
                message="Todo dia deve ter pelo menos uma estacao NOAA consolidada.",
            )
        )

    return checks


def collect_data_stats(df: DataFrame) -> dict[str, object]:
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.min("data").alias("min_date"),
        F.max("data").alias("max_date"),
    ).collect()[0]

    return {
        "total_rows": row["total_rows"] or 0,
        "min_date": row["min_date"],
        "max_date": row["max_date"],
    }


def collect_demand_stats(df: DataFrame) -> dict[str, int]:
    row = df.agg(
        F.sum((F.col("qtd_corridas") > 0).cast("long")).alias("days_with_demand"),
        F.sum((F.col("qtd_corridas") < 0).cast("long")).alias("negative_demand_days"),
        F.sum(F.coalesce(F.col("qtd_corridas"), F.lit(0))).cast("long").alias(
            "total_trips"
        ),
    ).collect()[0]

    return {
        "days_with_demand": row["days_with_demand"] or 0,
        "negative_demand_days": row["negative_demand_days"] or 0,
        "total_trips": row["total_trips"] or 0,
    }


def count_rows(df: DataFrame, condition) -> int:
    return df.filter(condition).count()


def print_validation_result(result: GoldValidationResult) -> None:
    print(f"Gold Daily Weather Demand validation: {result.status}")
    for check in result.checks:
        print(
            f"[{check.status}] {check.name}: "
            f"actual={check.actual} expected={check.expected} - {check.message}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Gold daily_weather_demand Delta table"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--input", default=None)
    parser.add_argument("--expected-days", type=int, default=None)
    parser.add_argument("--min-days-with-demand", type=int, default=1)
    parser.add_argument("--min-total-trips", type=int, default=1)
    parser.add_argument("--allow-incomplete-weather", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_path = (
        args.input if args.input else str(daily_weather_demand_gold_dir(args.year))
    )
    expected_days = (
        args.expected_days if args.expected_days else days_in_year(args.year)
    )
    config = DailyWeatherDemandValidationConfig(
        expected_days=expected_days,
        min_days_with_demand=args.min_days_with_demand,
        min_total_trips=args.min_total_trips,
        require_complete_weather=not args.allow_incomplete_weather,
    )

    print(f"Input               : {input_path}")
    print(f"Expected days       : {config.expected_days}")
    print(f"Min days with demand: {config.min_days_with_demand}")
    print(f"Min total trips     : {config.min_total_trips}")

    if args.dry_run:
        return 0

    if not delta_table_exists(input_path):
        print(f"Gold daily_weather_demand Delta table not found: {input_path}")
        print("Run first: poetry run gold-daily-weather-demand")
        return 1

    spark = create_spark("ValidateGoldDailyWeatherDemand")

    try:
        result = run_daily_weather_demand_validation(
            spark=spark,
            input_path=input_path,
            config=config,
            year=args.year,
        )
        print_validation_result(result)
        return 0 if result.passed else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
