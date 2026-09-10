# Resumo:
# - Centraliza os caminhos padrao da V2.
# - Evita espalhar strings de raw, bronze, silver, gold, quarantine e monitoring.

from __future__ import annotations

from v2.config.settings import StoragePath, join_storage_path, load_settings
from v2.config.sources import NOAA_GHCND_NYC_STORAGE_ID

SETTINGS = load_settings()
V2_ROOT = SETTINGS.v2_root
RAW_ROOT = SETTINGS.raw_root
DELTA_ROOT = SETTINGS.delta_root
GOLD_ROOT = SETTINGS.gold_root
QUARANTINE_ROOT = SETTINGS.quarantine_root
MONITORING_ROOT = SETTINGS.monitoring_root


def nyc_tlc_raw_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(RAW_ROOT, "nyc_tlc", "yellow", year)


def nyc_tlc_bronze_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(DELTA_ROOT, "bronze", "nyc_tlc", "yellow", year)


def nyc_tlc_silver_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(DELTA_ROOT, "silver", "nyc_tlc", "yellow", year)


def nyc_tlc_quarantine_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(QUARANTINE_ROOT, "nyc_tlc", "yellow", year)


def nyc_tlc_quality_metrics_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(MONITORING_ROOT, "quality", "nyc_tlc", "yellow", year)


def taxi_zone_lookup_raw_dir() -> StoragePath:
    return join_storage_path(RAW_ROOT, "nyc_tlc", "taxi_zone_lookup")


def taxi_zone_lookup_bronze_dir() -> StoragePath:
    return join_storage_path(DELTA_ROOT, "bronze", "nyc_tlc", "taxi_zone_lookup")


def taxi_zone_lookup_silver_dir() -> StoragePath:
    return join_storage_path(DELTA_ROOT, "silver", "nyc_tlc", "taxi_zone_lookup")


def taxi_zone_lookup_quarantine_dir() -> StoragePath:
    return join_storage_path(QUARANTINE_ROOT, "nyc_tlc", "taxi_zone_lookup")


def taxi_zone_lookup_quality_metrics_dir() -> StoragePath:
    return join_storage_path(MONITORING_ROOT, "quality", "nyc_tlc", "taxi_zone_lookup")


def noaa_raw_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> StoragePath:
    return join_storage_path(RAW_ROOT, "noaa", datasetid.lower(), year)


def noaa_bronze_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> StoragePath:
    return join_storage_path(DELTA_ROOT, "bronze", "noaa", datasetid.lower(), year)


def noaa_silver_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> StoragePath:
    return join_storage_path(DELTA_ROOT, "silver", "noaa", datasetid.lower(), year)


def noaa_quarantine_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> StoragePath:
    return join_storage_path(QUARANTINE_ROOT, "noaa", datasetid.lower(), year)


def noaa_quality_metrics_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> StoragePath:
    return join_storage_path(MONITORING_ROOT, "quality", "noaa", datasetid.lower(), year)


def daily_weather_demand_gold_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(GOLD_ROOT, "daily_weather_demand", year)


def star_schema_gold_dir(year: int = 2025) -> StoragePath:
    return join_storage_path(GOLD_ROOT, "star_schema", year)
