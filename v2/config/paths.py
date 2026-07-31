from __future__ import annotations

from pathlib import Path

from v2.config.sources import NOAA_GHCND_NYC_STORAGE_ID

V2_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = V2_ROOT / "data" / "raw"
DELTA_ROOT = V2_ROOT / "data" / "delta"
GOLD_ROOT = DELTA_ROOT / "gold"


def nyc_tlc_raw_dir(year: int = 2025) -> Path:
    return RAW_ROOT / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_bronze_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "bronze" / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_silver_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "silver" / "nyc_tlc" / "yellow" / str(year)


def noaa_raw_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return RAW_ROOT / "noaa" / datasetid.lower() / str(year)


def noaa_bronze_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return DELTA_ROOT / "bronze" / "noaa" / datasetid.lower() / str(year)


def noaa_silver_dir(year: int = 2025, datasetid: str = NOAA_GHCND_NYC_STORAGE_ID) -> Path:
    return DELTA_ROOT / "silver" / "noaa" / datasetid.lower() / str(year)


def daily_weather_demand_gold_dir(year: int = 2025) -> Path:
    return GOLD_ROOT / "daily_weather_demand" / str(year)


def star_schema_gold_dir(year: int = 2025) -> Path:
    return GOLD_ROOT / "star_schema" / str(year)
