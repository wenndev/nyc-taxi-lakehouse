# Databricks notebook source
# Resumo:
# - Wrapper Databricks para criar Silver Delta do Taxi Zone Lookup.
# - Recebe caminhos e Data Quality por widgets e chama run_silver_taxi_zone_lookup.

# MAGIC %md
# MAGIC # Silver Taxi Zone Lookup
# MAGIC
# MAGIC Wrapper Databricks para criar a Silver Delta do Taxi Zone Lookup.

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

from v2.pipelines.silver.silver_taxi_zone_lookup import (  # noqa: E402
    run_silver_taxi_zone_lookup,
)

# COMMAND ----------

dbutils.widgets.text("input", "")
dbutils.widgets.text("output", "")
dbutils.widgets.text("quarantine_output", "")
dbutils.widgets.text("metrics_output", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("skip_quality", "false")
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


input_path = optional_widget("input")
output_path = optional_widget("output")
quarantine_path = optional_widget("quarantine_output")
metrics_path = optional_widget("metrics_output")
pipeline_run_id = optional_widget("pipeline_run_id")
skip_quality = bool_widget("skip_quality")
mode = widget("mode")
skip_count = bool_widget("skip_count")
dry_run = bool_widget("dry_run")

if not input_path:
    raise ValueError("Parameter 'input' is required.")

if not output_path:
    raise ValueError("Parameter 'output' is required.")

if not skip_quality and not quarantine_path:
    raise ValueError("Parameter 'quarantine_output' is required when quality is enabled.")

if not skip_quality and not metrics_path:
    raise ValueError("Parameter 'metrics_output' is required when quality is enabled.")

print(f"Input : {input_path}")
print(f"Output: {output_path}")
print(f"Quarantine: {quarantine_path}")
print(f"Metrics   : {metrics_path}")
print("Format: delta -> delta")
print("Steps : normalize columns, validate quality, save lookup silver")
print(f"Quality: {'disabled' if skip_quality else 'enabled'}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "input": input_path,
                "output": output_path,
                "quarantine_output": quarantine_path,
                "metrics_output": metrics_path,
                "skip_quality": skip_quality,
            }
        )
    )

df_silver = run_silver_taxi_zone_lookup(
    spark=spark,
    input_path=input_path,
    output_path=output_path,
    mode=mode,
    quarantine_path=quarantine_path,
    metrics_path=metrics_path,
    pipeline_run_id=pipeline_run_id,
    enable_quality=not skip_quality,
)

print("Silver Taxi Zone Lookup saved.")
df_silver.printSchema()

rows = None
if not skip_count:
    rows = df_silver.count()
    print(f"Rows: {rows}")

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "input": input_path,
            "output": output_path,
            "quarantine_output": quarantine_path,
            "metrics_output": metrics_path,
            "skip_quality": skip_quality,
            "rows": rows,
        }
    )
)
