# Resumo:
# - Define as configuracoes de Data Quality para NOAA, TLC e Taxi Zone Lookup.
# - Aqui ficam colunas esperadas, limites plausiveis e thresholds PASS/WARNING/FAIL.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


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


@dataclass(frozen=True)
class TLCValueLimits:
    min_passenger_count: int = 0
    max_passenger_count: int = 8
    min_location_id: int = 1
    max_location_id: int = 265
    min_distance_miles: float = 0.0
    max_distance_miles: float = 200.0
    min_fare_amount: float = 0.0
    min_total_amount: float = 0.01
    max_total_amount: float = 1000.0
    max_duration_minutes: float = 1440.0


@dataclass(frozen=True)
class TLCQualityConfig:
    dataset_name: str = "nyc_tlc_yellow_trips"
    expected_columns: tuple[str, ...] = (
        "id_vendedor",
        "data_hora_partida",
        "data_hora_chegada",
        "qtd_passageiros",
        "distancia_milhas",
        "id_tarifa",
        "flag_armazenado_e_enviado",
        "id_local_partida",
        "id_local_chegada",
        "tipo_pagamento",
        "valor_tarifa",
        "taxa_extra",
        "taxa_mta_fixa",
        "gorjeta",
        "valor_pedagios",
        "sobretaxa_melhoria",
        "valor_total",
        "sobretaxa_transito",
        "taxa_aeroporto",
        "taxa_congestionamento_cbd",
    )
    required_columns: tuple[str, ...] = (
        "id_vendedor",
        "data_hora_partida",
        "data_hora_chegada",
        "id_local_partida",
        "id_local_chegada",
        "valor_total",
    )
    duplicate_key_columns: tuple[str, ...] = (
        "id_vendedor",
        "data_hora_partida",
        "id_local_partida",
        "id_local_chegada",
        "valor_total",
    )
    expected_schema: dict[str, str] = field(
        default_factory=lambda: {
            "id_vendedor": "integer",
            "data_hora_partida": "timestamp",
            "data_hora_chegada": "timestamp",
            "qtd_passageiros": "integer",
            "distancia_milhas": "double",
            "id_tarifa": "integer",
            "flag_armazenado_e_enviado": "string",
            "id_local_partida": "integer",
            "id_local_chegada": "integer",
            "tipo_pagamento": "integer",
            "valor_tarifa": "double",
            "taxa_extra": "double",
            "taxa_mta_fixa": "double",
            "gorjeta": "double",
            "valor_pedagios": "double",
            "sobretaxa_melhoria": "double",
            "valor_total": "double",
            "sobretaxa_transito": "double",
            "taxa_aeroporto": "double",
            "taxa_congestionamento_cbd": "double",
        }
    )
    thresholds: QualityThresholds = field(default_factory=QualityThresholds)
    limits: TLCValueLimits = field(default_factory=TLCValueLimits)
    pickup_start_date: date | None = None
    pickup_end_before_date: date | None = None
    fail_on_empty: bool = True
    fail_on_schema_error: bool = True
    fail_on_unexpected_columns: bool = False
    critical_rule_codes: tuple[str, ...] = ()

    @classmethod
    def for_year(cls, year: int) -> TLCQualityConfig:
        return cls(
            pickup_start_date=date(year, 1, 1),
            pickup_end_before_date=date(year + 1, 1, 1),
        )


@dataclass(frozen=True)
class TaxiZoneLookupValueLimits:
    min_location_id: int = 1
    max_location_id: int = 265


@dataclass(frozen=True)
class TaxiZoneLookupQualityConfig:
    dataset_name: str = "nyc_tlc_taxi_zone_lookup"
    expected_columns: tuple[str, ...] = (
        "location_id",
        "borough",
        "zona",
        "zona_servico",
    )
    required_columns: tuple[str, ...] = (
        "location_id",
        "borough",
        "zona",
        "zona_servico",
    )
    duplicate_key_columns: tuple[str, ...] = ("location_id",)
    expected_schema: dict[str, str] = field(
        default_factory=lambda: {
            "location_id": "integer",
            "borough": "string",
            "zona": "string",
            "zona_servico": "string",
        }
    )
    thresholds: QualityThresholds = field(
        default_factory=lambda: QualityThresholds(
            pass_min_percentage=100.0,
            warning_min_percentage=100.0,
        )
    )
    limits: TaxiZoneLookupValueLimits = field(default_factory=TaxiZoneLookupValueLimits)
    fail_on_empty: bool = True
    fail_on_schema_error: bool = True
    fail_on_unexpected_columns: bool = False
    critical_rule_codes: tuple[str, ...] = (
        "NULL_REQUIRED_FIELD",
        "INVALID_LOCATION_ID",
        "DUPLICATE_KEY",
    )
