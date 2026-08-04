# Resumo:
# - Valida a Gold Star Schema depois da execucao do pipeline.
# - Checa contagens, duplicidade de chaves, FKs nulas e chaves orfas.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import star_schema_gold_dir
from v2.config.spark import create_spark
from v2.pipelines.gold.gold_star_schema import table_path


@dataclass(frozen=True)
class GoldValidationCheck:
    name: str
    passed: bool
    actual: object
    expected: object
    message: str

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass(frozen=True)
class GoldValidationResult:
    checks: tuple[GoldValidationCheck, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass(frozen=True)
class GoldValidationConfig:
    expected_days: int
    expected_locations: int
    min_fact_rows: int = 1
    require_complete_weather: bool = True


@dataclass(frozen=True)
class GoldStarSchemaDataFrames:
    dim_data: DataFrame
    dim_clima: DataFrame
    dim_localizacao: DataFrame
    fact_trips: DataFrame


def run_gold_star_schema_validation(
    spark: SparkSession,
    input_path: str,
    config: GoldValidationConfig,
) -> GoldValidationResult:
    tables = read_gold_tables(spark=spark, input_path=input_path)
    return validate_gold_star_schema_tables(tables=tables, config=config)


def read_gold_tables(spark: SparkSession, input_path: str) -> GoldStarSchemaDataFrames:
    return GoldStarSchemaDataFrames(
        dim_data=spark.read.format("delta").load(table_path(input_path, "dim_data")),
        dim_clima=spark.read.format("delta").load(table_path(input_path, "dim_clima")),
        dim_localizacao=spark.read.format("delta").load(
            table_path(input_path, "dim_localizacao")
        ),
        fact_trips=spark.read.format("delta").load(
            table_path(input_path, "fact_trips")
        ),
    )


def validate_gold_star_schema_tables(
    tables: GoldStarSchemaDataFrames,
    config: GoldValidationConfig,
) -> GoldValidationResult:
    checks: list[GoldValidationCheck] = []

    checks.extend(validate_required_columns(tables))

    if has_columns(tables.dim_data, ("data_id", "data")):
        dim_data_count = tables.dim_data.count()
        checks.append(
            check_equals(
                name="dim_data_rows",
                actual=dim_data_count,
                expected=config.expected_days,
                message="dim_data deve ter 1 linha por dia do ano.",
            )
        )
        checks.append(
            check_equals(
                name="dim_data_duplicate_keys",
                actual=count_duplicate_keys(tables.dim_data, "data_id"),
                expected=0,
                message="dim_data.data_id deve ser unico.",
            )
        )

    if has_columns(tables.dim_clima, ("clima_id", "data")):
        dim_clima_count = tables.dim_clima.count()
        checks.append(
            check_equals(
                name="dim_clima_rows",
                actual=dim_clima_count,
                expected=config.expected_days,
                message="dim_clima deve ter 1 linha de clima consolidado por dia.",
            )
        )
        checks.append(
            check_equals(
                name="dim_clima_duplicate_keys",
                actual=count_duplicate_keys(tables.dim_clima, "clima_id"),
                expected=0,
                message="dim_clima.clima_id deve ser unico.",
            )
        )

        if (
            config.require_complete_weather
            and "registro_clima_incompleto" in tables.dim_clima.columns
        ):
            checks.append(
                check_equals(
                    name="dim_clima_incomplete_rows",
                    actual=count_true_values(
                        tables.dim_clima,
                        "registro_clima_incompleto",
                    ),
                    expected=0,
                    message="dim_clima nao deve ter dias com clima incompleto.",
                )
            )

    if has_columns(tables.dim_localizacao, ("localizacao_id", "location_id")):
        dim_localizacao_count = tables.dim_localizacao.count()
        checks.append(
            check_equals(
                name="dim_localizacao_rows",
                actual=dim_localizacao_count,
                expected=config.expected_locations,
                message="dim_localizacao deve conter as zonas oficiais da NYC TLC.",
            )
        )
        checks.append(
            check_equals(
                name="dim_localizacao_duplicate_keys",
                actual=count_duplicate_keys(tables.dim_localizacao, "localizacao_id"),
                expected=0,
                message="dim_localizacao.localizacao_id deve ser unico.",
            )
        )

        if "localizacao_sem_lookup" in tables.dim_localizacao.columns:
            checks.append(
                check_equals(
                    name="dim_localizacao_without_lookup",
                    actual=count_true_values(
                        tables.dim_localizacao,
                        "localizacao_sem_lookup",
                    ),
                    expected=0,
                    message=(
                        "Toda localizacao usada deve estar enriquecida pelo lookup."
                    ),
                )
            )

    fact_required_columns = (
        "data_id",
        "clima_id",
        "localizacao_partida_id",
        "localizacao_chegada_id",
    )
    if has_columns(tables.fact_trips, fact_required_columns):
        fact_count = tables.fact_trips.count()
        checks.append(
            check_at_least(
                name="fact_trips_rows",
                actual=fact_count,
                minimum=config.min_fact_rows,
                message="fact_trips deve conter corridas validas.",
            )
        )

        checks.extend(
            [
                check_equals(
                    name="fact_trips_null_data_id",
                    actual=count_nulls(tables.fact_trips, "data_id"),
                    expected=0,
                    message="fact_trips.data_id nao deve ser nulo.",
                ),
                check_equals(
                    name="fact_trips_null_clima_id",
                    actual=count_nulls(tables.fact_trips, "clima_id"),
                    expected=0,
                    message="fact_trips.clima_id nao deve ser nulo.",
                ),
                check_equals(
                    name="fact_trips_null_localizacao_partida_id",
                    actual=count_nulls(tables.fact_trips, "localizacao_partida_id"),
                    expected=0,
                    message="fact_trips.localizacao_partida_id nao deve ser nulo.",
                ),
                check_equals(
                    name="fact_trips_null_localizacao_chegada_id",
                    actual=count_nulls(tables.fact_trips, "localizacao_chegada_id"),
                    expected=0,
                    message="fact_trips.localizacao_chegada_id nao deve ser nulo.",
                ),
            ]
        )

        if has_columns(tables.dim_data, ("data_id",)):
            checks.append(
                check_equals(
                    name="fact_trips_orphan_data_id",
                    actual=count_orphan_keys(
                        fact_df=tables.fact_trips,
                        fk_column="data_id",
                        dim_df=tables.dim_data,
                        dim_key_column="data_id",
                    ),
                    expected=0,
                    message="fact_trips.data_id deve existir em dim_data.",
                )
            )

        if has_columns(tables.dim_clima, ("clima_id",)):
            checks.append(
                check_equals(
                    name="fact_trips_orphan_clima_id",
                    actual=count_orphan_keys(
                        fact_df=tables.fact_trips,
                        fk_column="clima_id",
                        dim_df=tables.dim_clima,
                        dim_key_column="clima_id",
                    ),
                    expected=0,
                    message="fact_trips.clima_id deve existir em dim_clima.",
                )
            )

        if has_columns(tables.dim_localizacao, ("localizacao_id",)):
            checks.append(
                check_equals(
                    name="fact_trips_orphan_localizacao_partida_id",
                    actual=count_orphan_keys(
                        fact_df=tables.fact_trips,
                        fk_column="localizacao_partida_id",
                        dim_df=tables.dim_localizacao,
                        dim_key_column="localizacao_id",
                    ),
                    expected=0,
                    message=(
                        "fact_trips.localizacao_partida_id deve existir em "
                        "dim_localizacao."
                    ),
                )
            )
            checks.append(
                check_equals(
                    name="fact_trips_orphan_localizacao_chegada_id",
                    actual=count_orphan_keys(
                        fact_df=tables.fact_trips,
                        fk_column="localizacao_chegada_id",
                        dim_df=tables.dim_localizacao,
                        dim_key_column="localizacao_id",
                    ),
                    expected=0,
                    message=(
                        "fact_trips.localizacao_chegada_id deve existir em "
                        "dim_localizacao."
                    ),
                )
            )

    return GoldValidationResult(checks=tuple(checks))


def validate_required_columns(
    tables: GoldStarSchemaDataFrames,
) -> list[GoldValidationCheck]:
    required_columns_by_table = {
        "dim_data": ("data_id", "data"),
        "dim_clima": ("clima_id", "data", "registro_clima_incompleto"),
        "dim_localizacao": (
            "localizacao_id",
            "location_id",
            "localizacao_sem_lookup",
        ),
        "fact_trips": (
            "data_id",
            "clima_id",
            "localizacao_partida_id",
            "localizacao_chegada_id",
        ),
    }
    dataframes_by_table = {
        "dim_data": tables.dim_data,
        "dim_clima": tables.dim_clima,
        "dim_localizacao": tables.dim_localizacao,
        "fact_trips": tables.fact_trips,
    }

    checks = []
    for table_name, required_columns in required_columns_by_table.items():
        missing_columns = tuple(
            column_name
            for column_name in required_columns
            if column_name not in dataframes_by_table[table_name].columns
        )
        checks.append(
            check_equals(
                name=f"{table_name}_required_columns",
                actual=list(missing_columns),
                expected=[],
                message=f"{table_name} deve conter as colunas obrigatorias.",
            )
        )

    return checks


def count_duplicate_keys(df: DataFrame, key_column: str) -> int:
    total_rows = df.count()
    distinct_keys = df.select(key_column).distinct().count()
    return total_rows - distinct_keys


def count_true_values(df: DataFrame, column_name: str) -> int:
    row = df.agg(
        F.sum(F.coalesce(F.col(column_name), F.lit(False)).cast("long")).alias("total")
    ).collect()[0]
    return row["total"] or 0


def count_nulls(df: DataFrame, column_name: str) -> int:
    row = df.agg(
        F.sum(F.col(column_name).isNull().cast("long")).alias("total")
    ).collect()[0]
    return row["total"] or 0


def count_orphan_keys(
    fact_df: DataFrame,
    fk_column: str,
    dim_df: DataFrame,
    dim_key_column: str,
) -> int:
    fact_keys = (
        fact_df.select(F.col(fk_column).alias(dim_key_column))
        .filter(F.col(dim_key_column).isNotNull())
        .distinct()
    )
    dim_keys = dim_df.select(dim_key_column).distinct()
    return fact_keys.join(dim_keys, on=dim_key_column, how="left_anti").count()


def has_columns(df: DataFrame, columns: tuple[str, ...]) -> bool:
    available_columns = set(df.columns)
    return all(column_name in available_columns for column_name in columns)


def check_equals(
    name: str,
    actual: object,
    expected: object,
    message: str,
) -> GoldValidationCheck:
    return GoldValidationCheck(
        name=name,
        passed=actual == expected,
        actual=actual,
        expected=expected,
        message=message,
    )


def check_at_least(
    name: str,
    actual: int,
    minimum: int,
    message: str,
) -> GoldValidationCheck:
    return GoldValidationCheck(
        name=name,
        passed=actual >= minimum,
        actual=actual,
        expected=f">= {minimum}",
        message=message,
    )


def days_in_year(year: int) -> int:
    return (date(year + 1, 1, 1) - date(year, 1, 1)).days


def is_local_path(path: str) -> bool:
    return "://" not in path


def delta_table_exists(path: str) -> bool:
    if not is_local_path(path):
        return True

    return (Path(path) / "_delta_log").exists()


def gold_tables_exist(input_path: str) -> bool:
    table_names = ("dim_data", "dim_clima", "dim_localizacao", "fact_trips")
    return all(
        delta_table_exists(table_path(input_path, table_name))
        for table_name in table_names
    )


def print_validation_result(result: GoldValidationResult) -> None:
    print(f"Gold Star Schema validation: {result.status}")
    for check in result.checks:
        print(
            f"[{check.status}] {check.name}: "
            f"actual={check.actual} expected={check.expected} - {check.message}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Gold Star Schema Delta tables"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--input", default=None)
    parser.add_argument("--expected-days", type=int, default=None)
    parser.add_argument("--expected-locations", type=int, default=265)
    parser.add_argument("--min-fact-rows", type=int, default=1)
    parser.add_argument("--allow-incomplete-weather", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(star_schema_gold_dir(args.year))
    expected_days = (
        args.expected_days if args.expected_days else days_in_year(args.year)
    )
    config = GoldValidationConfig(
        expected_days=expected_days,
        expected_locations=args.expected_locations,
        min_fact_rows=args.min_fact_rows,
        require_complete_weather=not args.allow_incomplete_weather,
    )

    print(f"Input             : {input_path}")
    print(f"Expected days     : {config.expected_days}")
    print(f"Expected locations: {config.expected_locations}")
    print(f"Min fact rows     : {config.min_fact_rows}")

    if args.dry_run:
        return 0

    if not gold_tables_exist(input_path):
        print(f"Gold Star Schema Delta tables not found under: {input_path}")
        print("Run first: poetry run gold-star-schema")
        return 1

    spark = create_spark("ValidateGoldStarSchema")

    try:
        result = run_gold_star_schema_validation(
            spark=spark,
            input_path=input_path,
            config=config,
        )
        print_validation_result(result)
        return 0 if result.passed else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
