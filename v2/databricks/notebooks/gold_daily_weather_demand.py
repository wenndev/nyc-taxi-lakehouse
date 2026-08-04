# Databricks notebook source
# Resumo:
# - Wrapper Databricks para criar a Gold diaria clima x demanda.
# - Recebe caminhos por widgets e chama run_gold_daily_weather_demand.

# MAGIC %md
# MAGIC # Gold Daily Weather Demand
# MAGIC
# MAGIC Wrapper Databricks para criar a Gold diaria de demanda e clima.

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

from v2.pipelines.gold.gold_daily_weather_demand import (  # noqa: E402
    run_gold_daily_weather_demand,
)

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("tlc_input", "")
dbutils.widgets.text("noaa_input", "")
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
tlc_input_path = optional_widget("tlc_input")
noaa_input_path = optional_widget("noaa_input")
output_path = optional_widget("output")
mode = widget("mode")
skip_count = bool_widget("skip_count")
dry_run = bool_widget("dry_run")

if not tlc_input_path:
    raise ValueError("Parameter 'tlc_input' is required.")

if not noaa_input_path:
    raise ValueError("Parameter 'noaa_input' is required.")

if not output_path:
    raise ValueError("Parameter 'output' is required.")

print(f"TLC input : {tlc_input_path}")
print(f"NOAA input: {noaa_input_path}")
print(f"Output    : {output_path}")
print(f"Year      : {year}")
print("Format    : silver delta + calendar -> gold delta")
print("Grain     : 1 row per day")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "tlc_input": tlc_input_path,
                "noaa_input": noaa_input_path,
                "output": output_path,
                "year": year,
            }
        )
    )

df_gold = run_gold_daily_weather_demand(
    spark=spark,
    tlc_input_path=tlc_input_path,
    noaa_input_path=noaa_input_path,
    output_path=output_path,
    year=year,
    mode=mode,
)

print("Gold daily weather demand saved.")
df_gold.printSchema()

rows = None
if not skip_count:
    rows = df_gold.count()
    print(f"Rows: {rows}")

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "tlc_input": tlc_input_path,
            "noaa_input": noaa_input_path,
            "output": output_path,
            "year": year,
            "rows": rows,
        }
    )
)
