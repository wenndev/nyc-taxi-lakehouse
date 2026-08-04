# Resumo:
# - Centraliza os caminhos padrao da V2.
# - Evita espalhar strings de raw, bronze, silver, gold, quarantine e monitoring.

from __future__ import annotations

from pathlib import Path

from v2.config.sources import NOAA_GHCND_NYC_STORAGE_ID

V2_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = V2_ROOT / "data" / "raw"
DELTA_ROOT = V2_ROOT / "data" / "delta"
GOLD_ROOT = DELTA_ROOT / "gold"
QUARANTINE_ROOT = DELTA_ROOT / "quarantine"
MONITORING_ROOT = DELTA_ROOT / "monitoring"


def nyc_tlc_raw_dir(year: int = 2025) -> Path:
    return RAW_ROOT / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_bronze_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "bronze" / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_silver_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "silver" / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_quarantine_dir(year: int = 2025) -> Path:
    return QUARANTINE_ROOT / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_quality_metrics_dir(year: int = 2025) -> Path:
    return MONITORING_ROOT / "quality" / "nyc_tlc" / "yellow" / str(year)


def noaa_raw_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return RAW_ROOT / "noaa" / datasetid.lower() / str(year)


def noaa_bronze_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return DELTA_ROOT / "bronze" / "noaa" / datasetid.lower() / str(year)


def noaa_silver_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return DELTA_ROOT / "silver" / "noaa" / datasetid.lower() / str(year)


def noaa_quarantine_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> Path:
    return QUARANTINE_ROOT / "noaa" / datasetid.lower() / str(year)


def noaa_quality_metrics_dir(
    year: int = 2025,
    datasetid: str = NOAA_GHCND_NYC_STORAGE_ID,
) -> Path:
    return MONITORING_ROOT / "quality" / "noaa" / datasetid.lower() / str(year)


def daily_weather_demand_gold_dir(year: int = 2025) -> Path:
    return GOLD_ROOT / "daily_weather_demand" / str(year)


def star_schema_gold_dir(year: int = 2025) -> Path:
    return GOLD_ROOT / "star_schema" / str(year)
