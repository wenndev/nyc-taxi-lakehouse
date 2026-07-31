# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest NOAA Weather
# MAGIC
# MAGIC Wrapper Databricks para o ADF chamar a ingestao NOAA com paginacao.

# COMMAND ----------

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def add_repo_root_to_path() -> None:
    current_dir = Path.cwd()

    for candidate in [current_dir, *current_dir.parents]:
        if (candidate / "v2").exists():
            sys.path.insert(0, str(candidate))
            return


add_repo_root_to_path()

# COMMAND ----------


def get_dbutils() -> Any:
    resolved_dbutils = globals().get("dbutils")

    if resolved_dbutils is None:
        raise RuntimeError("This notebook must run on Databricks with dbutils.")

    return resolved_dbutils


dbutils = get_dbutils()

# COMMAND ----------

from v2.pipelines.ingestion.download_noaa_weather import (  # noqa: E402
    DEFAULT_DATATYPES,
    build_base_params,
    default_to_nyc_location,
    build_url,
    download_pages,
    resolve_date_range,
    resolve_output_dir,
    resolve_storage_datasetid,
    validate_args,
)
from v2.config.sources import (  # noqa: E402
    NOAA_GHCND_NYC_STORAGE_ID,
    NOAA_NYC_LOCATION_ID,
)

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("datasetid", "GHCND")
dbutils.widgets.text("datatypeids", ",".join(DEFAULT_DATATYPES))
dbutils.widgets.text("stationid", "")
dbutils.widgets.text("locationid", NOAA_NYC_LOCATION_ID)
dbutils.widgets.text("units", "metric")
dbutils.widgets.text("limit", "1000")
dbutils.widgets.text("initial_offset", "1")
dbutils.widgets.text("output", "")
dbutils.widgets.text("storage_datasetid", NOAA_GHCND_NYC_STORAGE_ID)
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("secret_key", "noaa-token")
dbutils.widgets.text("overwrite", "false")
dbutils.widgets.text("sleep_seconds", "0.25")
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.text("dry_run", "false")

# COMMAND ----------


def widget(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def optional_widget(name: str) -> str | None:
    value = widget(name)
    return value or None


def csv_widget(name: str) -> list[str] | None:
    value = optional_widget(name)
    if not value:
        return None

    return [item.strip() for item in value.split(",") if item.strip()]


def bool_widget(name: str) -> bool:
    return widget(name).lower() in {"1", "true", "yes", "y", "sim"}


def get_noaa_token(secret_scope: str | None, secret_key: str | None) -> str | None:
    if secret_scope and secret_key:
        return dbutils.secrets.get(scope=secret_scope, key=secret_key)

    return os.environ.get("NOAA_TOKEN")


class Args:
    start_date: str | None
    end_date: str | None
    limit: int
    initial_offset: int
    stationid: list[str] | None
    locationid: list[str] | None
    allow_global: bool


args = Args()
args.start_date = optional_widget("start_date")
args.end_date = optional_widget("end_date")
args.limit = int(widget("limit"))
args.initial_offset = int(widget("initial_offset"))
args.stationid = csv_widget("stationid")
args.locationid = csv_widget("locationid")
args.allow_global = False
default_to_nyc_location(args)

year = int(widget("year"))
datasetid = widget("datasetid")
datatypes = csv_widget("datatypeids") or DEFAULT_DATATYPES
units = widget("units")
output = optional_widget("output")
storage_datasetid = resolve_storage_datasetid(
    api_datasetid=datasetid,
    stationids=args.stationid,
    locationids=args.locationid,
    storage_datasetid=optional_widget("storage_datasetid"),
)
overwrite = bool_widget("overwrite")
dry_run = bool_widget("dry_run")
sleep_seconds = float(widget("sleep_seconds"))
max_retries = int(widget("max_retries"))
secret_scope = optional_widget("secret_scope")
secret_key = optional_widget("secret_key")

start_date, end_date = resolve_date_range(year, args.start_date, args.end_date)
validate_args(args, datatypes)

output_dir = resolve_output_dir(
    output,
    Path(f"/tmp/noaa/{storage_datasetid.lower()}/{year}"),
)
base_params = build_base_params(
    datasetid=datasetid,
    datatypes=datatypes,
    stationids=args.stationid,
    locationids=args.locationid,
    start_date=start_date,
    end_date=end_date,
    units=units,
)

first_page_url = build_url(base_params, limit=args.limit, offset=args.initial_offset)

print(f"Output : {output_dir}")
print(f"Dataset: {datasetid}")
print(f"Storage: {storage_datasetid}")
print(f"Dates  : {start_date} -> {end_date}")
print(f"Types  : {', '.join(datatypes)}")
print(f"First page: {first_page_url}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "output": str(output_dir),
                "storage_datasetid": storage_datasetid,
                "first_page": first_page_url,
            }
        )
    )

token = get_noaa_token(secret_scope, secret_key)
if not token:
    raise ValueError("NOAA token not found. Configure Databricks secret or NOAA_TOKEN.")

output_dir.mkdir(parents=True, exist_ok=True)
exit_code = download_pages(
    token=token,
    base_params=base_params,
    output_dir=output_dir,
    limit=args.limit,
    initial_offset=args.initial_offset,
    storage_datasetid=storage_datasetid,
    overwrite=overwrite,
    sleep_seconds=sleep_seconds,
    max_retries=max_retries,
)

if exit_code != 0:
    raise RuntimeError(f"NOAA ingestion failed with exit code {exit_code}")

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "output": str(output_dir),
            "year": year,
            "datasetid": datasetid,
            "storage_datasetid": storage_datasetid,
            "stationid": args.stationid,
            "locationid": args.locationid,
        }
    )
)
