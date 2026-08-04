# Resumo:
# - Define os modelos de retorno da qualidade.
# - Padroniza valid_records, invalid_records, metricas, status e pipeline_run_id.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pyspark.sql import DataFrame


class QualityStatus(StrEnum):
    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"


@dataclass(frozen=True)
class SchemaValidationResult:
    missing_columns: tuple[str, ...] = ()
    unexpected_columns: tuple[str, ...] = ()
    incompatible_types: tuple[str, ...] = ()

    @property
    def has_errors(self) -> bool:
        return bool(
            self.missing_columns or self.unexpected_columns or self.incompatible_types
        )

    @property
    def critical_rule_codes(self) -> tuple[str, ...]:
        rules: list[str] = []

        if self.missing_columns:
            rules.append("MISSING_REQUIRED_COLUMN")

        if self.incompatible_types:
            rules.append("INCOMPATIBLE_TYPE")

        if self.unexpected_columns:
            rules.append("UNEXPECTED_COLUMN")

        return tuple(rules)


@dataclass(frozen=True)
class QualityMetrics:
    pipeline_run_id: str
    dataset_name: str
    execution_timestamp: datetime
    total_records: int
    valid_records: int
    invalid_records: int
    quality_percentage: float
    duplicate_count: int
    null_error_count: int
    schema_error_count: int
    range_error_count: int
    future_date_count: int
    pipeline_status: QualityStatus
    missing_columns: tuple[str, ...] = ()
    unexpected_columns: tuple[str, ...] = ()
    incompatible_types: tuple[str, ...] = ()

    def as_row(self) -> dict[str, object]:
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "dataset_name": self.dataset_name,
            "execution_timestamp": self.execution_timestamp,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "quality_percentage": self.quality_percentage,
            "duplicate_count": self.duplicate_count,
            "null_error_count": self.null_error_count,
            "schema_error_count": self.schema_error_count,
            "range_error_count": self.range_error_count,
            "future_date_count": self.future_date_count,
            "pipeline_status": self.pipeline_status.value,
            "missing_columns": list(self.missing_columns),
            "unexpected_columns": list(self.unexpected_columns),
            "incompatible_types": list(self.incompatible_types),
        }


@dataclass(frozen=True)
class DataQualityResult:
    valid_records: DataFrame
    invalid_records: DataFrame
    metrics: QualityMetrics
    status: QualityStatus
    pipeline_run_id: str
    dataset_name: str
    schema_validation: SchemaValidationResult = field(
        default_factory=SchemaValidationResult
    )


def utc_now() -> datetime:
    return datetime.now(UTC)
