# Resumo:
# - Valida a Silver das corridas NYC TLC depois da publicacao.
# - Confere colunas, datas, chaves de localizacao, valores e duplicatas.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import nyc_tlc_silver_dir
from v2.config.spark import create_spark
from v2.pipelines.silver.silver_nyc_tlc import DEDUPLICATION_COLUMNS
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
    delta_table_exists,
    has_columns,
    print_validation_result,
)


@dataclass(frozen=True)
class SilverNYCTLCValidationConfig:
    year: int = 2025
    min_rows: int = 1
    expected_days: int | None = None


def run_silver_nyc_tlc_validation(
    spark: SparkSession,
    input_path: str,
    config: SilverNYCTLCValidationConfig,
) -> SilverValidationResult:
    df = spark.read.format("delta").load(input_path)
    return validate_silver_nyc_tlc_table(df=df, config=config)


def validate_silver_nyc_tlc_table(
    df: DataFrame,
    config: SilverNYCTLCValidationConfig,
) -> SilverValidationResult:
    checks: list[SilverValidationCheck] = []
    checks.extend(validate_required_columns(df))

    if has_columns(df, ("data_viagem",)):
        start_date = date(config.year, 1, 1)
        end_date = date(config.year + 1, 1, 1)
        stats = collect_date_stats(df, "data_viagem")

        checks.extend(
            [
                check_at_least(
                    name="tlc_rows",
                    actual=stats["total_rows"],
                    minimum=config.min_rows,
                    message="Silver TLC deve ter corridas validas.",
                ),
                check_equals(
                    name="tlc_out_of_year_rows",
                    actual=count_rows(
                        df,
                        (F.col("data_viagem") < F.lit(start_date))
                        | (F.col("data_viagem") >= F.lit(end_date)),
                    ),
                    expected=0,
                    message="Silver TLC deve conter apenas viagens do ano esperado.",
                ),
                check_equals(
                    name="tlc_null_data_viagem",
                    actual=count_nulls(df, "data_viagem"),
                    expected=0,
                    message="data_viagem nao deve ser nula.",
                ),
            ]
        )

        if config.expected_days is not None:
            checks.append(
                check_equals(
                    name="tlc_distinct_days",
                    actual=count_distinct(df, "data_viagem"),
                    expected=config.expected_days,
                    message="Silver TLC deve cobrir a quantidade esperada de dias.",
                )
            )

    if has_columns(df, ("id_local_partida", "id_local_chegada")):
        checks.extend(
            [
                check_equals(
                    name="tlc_invalid_pickup_location",
                    actual=count_rows(
                        df,
                        (F.col("id_local_partida") < 1)
                        | (F.col("id_local_partida") > 265),
                    ),
                    expected=0,
                    message="id_local_partida deve estar entre 1 e 265.",
                ),
                check_equals(
                    name="tlc_invalid_dropoff_location",
                    actual=count_rows(
                        df,
                        (F.col("id_local_chegada") < 1)
                        | (F.col("id_local_chegada") > 265),
                    ),
                    expected=0,
                    message="id_local_chegada deve estar entre 1 e 265.",
                ),
            ]
        )

    if has_columns(df, tuple(DEDUPLICATION_COLUMNS)):
        checks.append(
            check_equals(
                name="tlc_business_duplicates",
                actual=count_duplicate_keys(df, tuple(DEDUPLICATION_COLUMNS)),
                expected=0,
                message="Silver TLC nao deve ter duplicatas de negocio.",
            )
        )

    checks.extend(validate_not_null_columns(df))
    checks.extend(validate_non_negative_metrics(df))

    return SilverValidationResult(checks=tuple(checks))


def validate_required_columns(df: DataFrame) -> list[SilverValidationCheck]:
    required_columns = (
        "id_vendedor",
        "data_hora_partida",
        "data_hora_chegada",
        "data_viagem",
        "ano",
        "mes",
        "dia_mes",
        "hora_partida",
        "dia_semana_num",
        "dia_semana_nome",
        "fim_de_semana",
        "id_local_partida",
        "id_local_chegada",
        "tipo_pagamento",
        "tipo_pagamento_desc",
        "valor_total",
        "distancia_milhas",
        "distancia_km",
        "duracao_minutos",
        "periodo_dia",
        "horario_pico",
        "registro_suspeito",
    )
    missing_columns = tuple(
        column_name for column_name in required_columns if column_name not in df.columns
    )

    return [
        check_equals(
            name="tlc_required_columns",
            actual=list(missing_columns),
            expected=[],
            message="Silver TLC deve conter as colunas obrigatorias.",
        )
    ]


def validate_not_null_columns(df: DataFrame) -> list[SilverValidationCheck]:
    columns = (
        "id_vendedor",
        "data_hora_partida",
        "data_hora_chegada",
        "id_local_partida",
        "id_local_chegada",
        "valor_total",
        "distancia_km",
        "duracao_minutos",
        "periodo_dia",
        "horario_pico",
        "registro_suspeito",
    )
    checks = []

    for column_name in columns:
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"tlc_null_{column_name}",
                    actual=count_nulls(df, column_name),
                    expected=0,
                    message=f"{column_name} nao deve ser nula na Silver TLC.",
                )
            )

    return checks


def validate_non_negative_metrics(df: DataFrame) -> list[SilverValidationCheck]:
    rules = {
        "valor_total": F.col("valor_total") <= 0,
        "distancia_km": F.col("distancia_km") < 0,
        "duracao_minutos": F.col("duracao_minutos") < 0,
    }
    checks = []

    for column_name, condition in rules.items():
        if column_name in df.columns:
            checks.append(
                check_equals(
                    name=f"tlc_invalid_{column_name}",
                    actual=count_rows(df, condition),
                    expected=0,
                    message=f"{column_name} deve respeitar faixa valida.",
                )
            )

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NYC TLC Silver Delta table")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--input", default=None)
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--expected-days", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(nyc_tlc_silver_dir(args.year))
    config = SilverNYCTLCValidationConfig(
        year=args.year,
        min_rows=args.min_rows,
        expected_days=args.expected_days,
    )

    print(f"Input        : {input_path}")
    print(f"Year         : {config.year}")
    print(f"Min rows     : {config.min_rows}")
    print(f"Expected days: {config.expected_days or 'not enforced'}")

    if args.dry_run:
        return 0

    if not delta_table_exists(input_path):
        print(f"Silver NYC TLC Delta table not found: {input_path}")
        print("Run first: poetry run silver-nyc-tlc")
        return 1

    spark = create_spark("ValidateSilverNYCTLC")

    try:
        result = run_silver_nyc_tlc_validation(
            spark=spark,
            input_path=input_path,
            config=config,
        )
        print_validation_result(result, title="Silver NYC TLC validation")
        return 0 if result.passed else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
