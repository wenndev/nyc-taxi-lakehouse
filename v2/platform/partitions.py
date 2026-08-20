# Resumo:
# - Centraliza a regra de particionamento temporal ano/mes.
# - Monta replaceWhere e filtros mensais para reprocessamento incremental.

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

YEAR_MONTH_PARTITIONS = ("ano", "mes")


@dataclass(frozen=True)
class YearMonthPartition:
    year: int
    month: int

    def __post_init__(self) -> None:
        validate_year_month(self.year, self.month)

    @property
    def replace_where(self) -> str:
        return f"ano = {self.year} AND mes = {self.month}"

    def date_range(self) -> tuple[str, str]:
        start_date = date(self.year, self.month, 1)
        if self.month == 12:
            end_date = date(self.year + 1, 1, 1)
        else:
            end_date = date(self.year, self.month + 1, 1)

        return start_date.isoformat(), end_date.isoformat()


def resolve_year_month_partition(
    base_year: int,
    replace_year: int | None = None,
    replace_month: int | None = None,
) -> YearMonthPartition | None:
    if replace_year is None and replace_month is None:
        return None

    if replace_month is None:
        raise ValueError("replace_month is required when replace_year is informed.")

    year = replace_year if replace_year is not None else base_year
    validate_year_month(year, replace_month)
    return YearMonthPartition(year=year, month=replace_month)


def validate_year_month(year: int, month: int) -> None:
    if year < 1900:
        raise ValueError("year must be greater than or equal to 1900.")

    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12.")


def validate_replace_partition_write_mode(
    mode: str,
    partition: YearMonthPartition | None,
) -> None:
    if partition is not None and mode != "overwrite":
        raise ValueError("replace_month requires mode='overwrite'.")


def filter_year_month_partition(
    df: DataFrame,
    partition: YearMonthPartition | None,
) -> DataFrame:
    if partition is None:
        return df

    return df.filter(
        (F.col("ano") == partition.year) & (F.col("mes") == partition.month)
    )


def filter_month_date_range(
    df: DataFrame,
    date_column: str,
    partition: YearMonthPartition | None,
) -> DataFrame:
    if partition is None:
        return df

    start_date, end_date = partition.date_range()
    return df.filter(
        (F.col(date_column) >= F.lit(start_date))
        & (F.col(date_column) < F.lit(end_date))
    )
