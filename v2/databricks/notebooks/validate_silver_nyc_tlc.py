# Databricks notebook source
# Resumo:
# - Wrapper Databricks para validar a Silver da NYC TLC.
# - Deve ser executado depois da Silver TLC para falhar o job se houver erro.

# MAGIC %md
# MAGIC # Validate Silver NYC TLC
# MAGIC
# MAGIC Wrapper Databricks para validar a Silver da NYC TLC antes da Gold.

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

from v2.pipelines.silver.validate_silver_nyc_tlc import (  # noqa: E402
    SilverNYCTLCValidationConfig,
    run_silver_nyc_tlc_validation,
)
from v2.pipelines.silver.validate_silver_common import (  # noqa: E402
    print_validation_result,
)

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("input", "")
dbutils.widgets.text("min_rows", "1")
dbutils.widgets.text("expected_days", "")
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
min_rows = int(widget("min_rows"))
expected_days = (
    int(widget("expected_days")) if optional_widget("expected_days") else None
)
dry_run = bool_widget("dry_run")

if not input_path:
    raise ValueError("Parameter 'input' is required.")

config = SilverNYCTLCValidationConfig(
    year=year,
    min_rows=min_rows,
    expected_days=expected_days,
)

print(f"Input        : {input_path}")
print(f"Year         : {config.year}")
print(f"Min rows     : {config.min_rows}")
print(f"Expected days: {config.expected_days or 'not enforced'}")

if dry_run:
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "dry_run",
                "input": input_path,
                "year": year,
                "min_rows": min_rows,
                "expected_days": expected_days,
            },
            default=str,
        )
    )

result = run_silver_nyc_tlc_validation(
    spark=spark,
    input_path=input_path,
    config=config,
)
print_validation_result(result, title="Silver NYC TLC validation")

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
    raise RuntimeError(json.dumps(payload, default=str))

dbutils.notebook.exit(json.dumps(payload, default=str))
