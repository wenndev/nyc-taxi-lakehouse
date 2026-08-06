# Databricks notebook source
# Resumo:
# - Wrapper Databricks para validar a Silver do Taxi Zone Lookup.
# - Deve ser executado depois da Silver Lookup para garantir a dim_localizacao.

# MAGIC %md
# MAGIC # Validate Silver Taxi Zone Lookup
# MAGIC
# MAGIC Wrapper Databricks para validar a referencia de localizacao da NYC TLC.

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

from v2.pipelines.silver.validate_silver_common import (  # noqa: E402
    print_validation_result,
)
from v2.pipelines.silver.validate_silver_taxi_zone_lookup import (  # noqa: E402
    SilverTaxiZoneLookupValidationConfig,
    run_silver_taxi_zone_lookup_validation,
)

# COMMAND ----------

dbutils.widgets.text("input", "")
dbutils.widgets.text("expected_locations", "265")
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
expected_locations = int(widget("expected_locations"))
dry_run = bool_widget("dry_run")

if not input_path:
    raise ValueError("Parameter 'input' is required.")

config = SilverTaxiZoneLookupValidationConfig(
    expected_locations=expected_locations,
)

print(f"Input             : {input_path}")
print(f"Expected locations: {config.expected_locations}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "input": input_path,
                "expected_locations": expected_locations,
            },
            default=str,
        )
    )

result = run_silver_taxi_zone_lookup_validation(
    spark=spark,
    input_path=input_path,
    config=config,
)
print_validation_result(result, title="Silver Taxi Zone Lookup validation")

payload = {
    "status": result.status,
    "input": input_path,
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
    raise RuntimeError(json.dumps(payload, default=str))

dbutils.notebook.exit(json.dumps(payload, default=str))
