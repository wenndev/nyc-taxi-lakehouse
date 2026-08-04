# Resumo:
# - Centraliza URLs, nomes de arquivos e identificadores das fontes.
# - Ajuda ingestion, Bronze e Silver a usarem os mesmos nomes de TLC e NOAA.

from __future__ import annotations


NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_TLC_YELLOW_FILE_TEMPLATE = "yellow_tripdata_{year}-{month:02d}.parquet"

NOAA_CDO_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
NOAA_CDO_DATA_URL = f"{NOAA_CDO_BASE_URL}/data"

NOAA_GHCND_DATASET_ID = "GHCND"
NOAA_GHCND_NYC_STORAGE_ID = "GHCND_NYC"
NOAA_NYC_LOCATION_ID = "CITY:US360019"


def nyc_tlc_yellow_url(year: int, month: int) -> str:
    filename = NYC_TLC_YELLOW_FILE_TEMPLATE.format(year=year, month=month)
    return f"{NYC_TLC_BASE_URL}/{filename}"


def nyc_tlc_yellow_filename(year: int, month: int) -> str:
    return NYC_TLC_YELLOW_FILE_TEMPLATE.format(year=year, month=month)
