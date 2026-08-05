# Resumo:
# - Reune funcoes comuns dos validadores pos-Silver.
# - Padroniza checks, resultado final, contagens e existencia de tabela Delta.

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class SilverValidationCheck:
    name: str
    passed: bool
    actual: object
    expected: object
    message: str

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass(frozen=True)
class SilverValidationResult:
    checks: tuple[SilverValidationCheck, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def check_equals(
    name: str,
    actual: object,
    expected: object,
    message: str,
) -> SilverValidationCheck:
    return SilverValidationCheck(
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
) -> SilverValidationCheck:
    return SilverValidationCheck(
        name=name,
        passed=actual >= minimum,
        actual=actual,
        expected=f">= {minimum}",
        message=message,
    )


def has_columns(df: DataFrame, columns: tuple[str, ...]) -> bool:
    available_columns = set(df.columns)
    return all(column_name in available_columns for column_name in columns)


def count_nulls(df: DataFrame, column_name: str) -> int:
    row = df.agg(
        F.sum(F.col(column_name).isNull().cast("long")).alias("total")
    ).collect()[0]
    return row["total"] or 0


def count_rows(df: DataFrame, condition) -> int:
    return df.filter(condition).count()


def count_duplicate_keys(df: DataFrame, key_columns: tuple[str, ...]) -> int:
    total_rows = df.count()
    distinct_rows = df.select(*key_columns).distinct().count()
    return total_rows - distinct_rows


def count_distinct(df: DataFrame, column_name: str) -> int:
    row = df.agg(F.countDistinct(column_name).alias("total")).collect()[0]
    return row["total"] or 0


def collect_date_stats(df: DataFrame, column_name: str) -> dict[str, object]:
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.min(column_name).alias("min_date"),
        F.max(column_name).alias("max_date"),
    ).collect()[0]

    return {
        "total_rows": row["total_rows"] or 0,
        "min_date": row["min_date"],
        "max_date": row["max_date"],
    }


def days_in_year(year: int) -> int:
    return (date(year + 1, 1, 1) - date(year, 1, 1)).days


def is_local_path(path: str) -> bool:
    return "://" not in path


def delta_table_exists(path: str) -> bool:
    if not is_local_path(path):
        return True

    return (Path(path) / "_delta_log").exists()


def print_validation_result(
    result: SilverValidationResult,
    title: str,
) -> None:
    print(f"{title}: {result.status}")
    for check in result.checks:
        print(
            f"[{check.status}] {check.name}: "
            f"actual={check.actual} expected={check.expected} - {check.message}"
        )
