# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze NOAA Weather
# MAGIC
# MAGIC Wrapper Databricks para criar a Bronze Delta da NOAA.

# COMMAND ----------

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


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


def get_spark() -> SparkSession:
    resolved_spark = globals().get("spark")

    if resolved_spark is None:
        raise RuntimeError("This notebook must run on Databricks with spark.")

    return resolved_spark


dbutils = get_dbutils()
spark = get_spark()

# COMMAND ----------

from v2.pipelines.bronze.bronze_noaa_weather import (  # noqa: E402
    json_page_pattern,
    run_bronze_noaa_weather,
)

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("datasetid", "GHCND")
dbutils.widgets.text("input", "")
dbutils.widgets.text("output", "")
dbutils.widgets.text("mode", "overwrite")
dbutils.widgets.text("skip_count", "true")
dbutils.widgets.text("dry_run", "false")

# COMMAND ----------


def widget(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def optional_widget(name: str) -> str | None:
    value = widget(name)
    return value or None


def bool_widget(name: str) -> bool:
    return widget(name).lower() in {"1", "true", "yes", "y", "sim"}


year = int(widget("year"))
datasetid = widget("datasetid")
input_path = optional_widget("input")
output_path = optional_widget("output")
mode = widget("mode")
skip_count = bool_widget("skip_count")
dry_run = bool_widget("dry_run")

if not input_path:
    raise ValueError("Parameter 'input' is required.")

if not output_path:
    raise ValueError("Parameter 'output' is required.")

print(f"Input : {input_path}")
print(f"Output: {output_path}")
print(f"Year  : {year}")
print(f"Dataset: {datasetid}")
print("Format: NOAA raw JSON -> delta")
print(f"Files : {json_page_pattern(input_path)}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "input": input_path,
                "output": output_path,
                "year": year,
                "datasetid": datasetid,
            }
        )
    )

df_bronze = run_bronze_noaa_weather(
    spark=spark,
    input_path=input_path,
    output_path=output_path,
    mode=mode,
)

print("Bronze NOAA Weather saved.")
df_bronze.printSchema()

rows = None
if not skip_count:
    rows = df_bronze.count()
    print(f"Rows: {rows}")

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "input": input_path,
            "output": output_path,
            "year": year,
            "datasetid": datasetid,
            "rows": rows,
        }
    )
)
