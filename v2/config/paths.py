from __future__ import annotations

from pathlib import Path


V2_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = V2_ROOT / "data" / "raw"
DELTA_ROOT = V2_ROOT / "data" / "delta"


def nyc_tlc_raw_dir(year: int = 2025) -> Path:
    return RAW_ROOT / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_bronze_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "bronze" / "nyc_tlc" / "yellow" / str(year)


def nyc_tlc_silver_dir(year: int = 2025) -> Path:
    return DELTA_ROOT / "silver" / "nyc_tlc" / "yellow" / str(year)
