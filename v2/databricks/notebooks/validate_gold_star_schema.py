# Databricks notebook source
# Resumo:
# - Wrapper Databricks para validar a Gold Star Schema.
# - Deve ser executado depois da Gold para falhar o job se houver inconsistencia.

# MAGIC %md
# MAGIC # Validate Gold Star Schema
# MAGIC
# MAGIC Wrapper Databricks para validar a Gold dimensional da V2.

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

from v2.pipelines.gold.validate_gold_star_schema import (  # noqa: E402
    GoldValidationConfig,
    days_in_year,
    print_validation_result,
    run_gold_star_schema_validation,
)

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("input", "")
dbutils.widgets.text("expected_days", "")
dbutils.widgets.text("expected_locations", "265")
dbutils.widgets.text("min_fact_rows", "1")
dbutils.widgets.text("allow_incomplete_weather", "false")
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
input_path = optional_widget("input")
expected_days = (
    int(widget("expected_days"))
    if optional_widget("expected_days")
    else days_in_year(year)
)
expected_locations = int(widget("expected_locations"))
min_fact_rows = int(widget("min_fact_rows"))
allow_incomplete_weather = bool_widget("allow_incomplete_weather")
dry_run = bool_widget("dry_run")

if not input_path:
    raise ValueError("Parameter 'input' is required.")

config = GoldValidationConfig(
    expected_days=expected_days,
    expected_locations=expected_locations,
    min_fact_rows=min_fact_rows,
    require_complete_weather=not allow_incomplete_weather,
)

print(f"Input             : {input_path}")
print(f"Year              : {year}")
print(f"Expected days     : {config.expected_days}")
print(f"Expected locations: {config.expected_locations}")
print(f"Min fact rows     : {config.min_fact_rows}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "input": input_path,
                "year": year,
                "expected_days": config.expected_days,
                "expected_locations": config.expected_locations,
            }
        )
    )

result = run_gold_star_schema_validation(
    spark=spark,
    input_path=input_path,
    config=config,
)
print_validation_result(result)

payload = {
    "status": result.status,
    "input": input_path,
    "year": year,
    "checks": [
        {
            "name": check.name,
            "status": check.status,
            "actual": check.actual,
            "expected": check.expected,
            "message": check.message,
        }
        for check in result.checks
    ],
}

if not result.passed:
    raise RuntimeError(json.dumps(payload))

dbutils.notebook.exit(json.dumps(payload))
