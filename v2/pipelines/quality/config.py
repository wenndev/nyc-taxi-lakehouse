from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class QualityThresholds:
    pass_min_percentage: float = 99.0
    warning_min_percentage: float = 95.0


@dataclass(frozen=True)
class NOAAValueLimits:
    temperature_min_c: float = -90.0
    temperature_max_c: float = 60.0
    precipitation_min_mm: float = 0.0
    precipitation_max_mm: float = 2000.0
    snow_min_mm: float = 0.0
    snow_max_mm: float = 3000.0
    snow_depth_min_mm: float = 0.0
    snow_depth_max_mm: float = 10000.0


@dataclass(frozen=True)
class NOAAQualityConfig:
    dataset_name: str = "noaa_weather"
    expected_columns: tuple[str, ...] = (
        "data_clima",
        "id_estacao",
        "tipo_dado",
        "valor",
        "atributos",
        "arquivo_origem",
        "data_processamento_bronze",
    )
    required_columns: tuple[str, ...] = (
        "data_clima",
        "id_estacao",
        "tipo_dado",
        "valor",
    )
    duplicate_key_columns: tuple[str, ...] = (
        "data_clima",
        "id_estacao",
        "tipo_dado",
    )
    allowed_datatypes: tuple[str, ...] = ("PRCP", "TMAX", "TMIN", "SNOW", "SNWD")
    expected_schema: dict[str, str] = field(
        default_factory=lambda: {
            "data_clima": "date",
            "id_estacao": "string",
            "tipo_dado": "string",
            "valor": "double",
            "atributos": "string",
            "arquivo_origem": "string",
            "data_processamento_bronze": "timestamp",
        }
    )
    thresholds: QualityThresholds = field(default_factory=QualityThresholds)
    limits: NOAAValueLimits = field(default_factory=NOAAValueLimits)
    fail_on_empty: bool = True
    fail_on_schema_error: bool = True
    fail_on_unexpected_columns: bool = False
    critical_rule_codes: tuple[str, ...] = (
        "FUTURE_DATE",
        "INVALID_DATATYPE",
    )

