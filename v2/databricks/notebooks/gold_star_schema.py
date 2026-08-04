# Databricks notebook source
# Resumo:
# - Wrapper Databricks para criar o Star Schema da Gold.
# - Recebe caminhos por widgets e chama run_gold_star_schema.

# MAGIC %md
# MAGIC # Gold Star Schema
# MAGIC
# MAGIC Wrapper Databricks para criar a Gold dimensional da V2.

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

from v2.pipelines.gold.gold_star_schema import run_gold_star_schema  # noqa: E402

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
print("Format    : silver delta -> gold star schema delta")
print("Tables    : dim_data, dim_clima, dim_localizacao, fact_trips")

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

tables = run_gold_star_schema(
    spark=spark,
    tlc_input_path=tlc_input_path,
    noaa_input_path=noaa_input_path,
    output_path=output_path,
    year=year,
    mode=mode,
)

print("Gold Star Schema saved.")

rows = None
if not skip_count:
    rows = {
        "dim_data": tables.dim_data.count(),
        "dim_clima": tables.dim_clima.count(),
        "dim_localizacao": tables.dim_localizacao.count(),
        "fact_trips": tables.fact_trips.count(),
    }
    print(f"dim_data        : {rows['dim_data']}")
    print(f"dim_clima       : {rows['dim_clima']}")
    print(f"dim_localizacao : {rows['dim_localizacao']}")
    print(f"fact_trips      : {rows['fact_trips']}")

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
