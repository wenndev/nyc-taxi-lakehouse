# Databricks notebook source
# Resumo:
# - Wrapper Databricks para baixar o Taxi Zone Lookup da NYC TLC.
# - Recebe parametros por widgets e chama a ingestion local reaproveitavel.

# MAGIC %md
# MAGIC # Ingest Taxi Zone Lookup
# MAGIC
# MAGIC Wrapper Databricks para o ADF chamar a ingestao do Taxi Zone Lookup.

# COMMAND ----------

from __future__ import annotations

import json
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

from v2.pipelines.ingestion.download_taxi_zone_lookup import (  # noqa: E402
    download_taxi_zone_lookup,
    resolve_output_dir,
)

# COMMAND ----------

dbutils.widgets.text("output", "")
dbutils.widgets.text("overwrite", "false")
dbutils.widgets.text("dry_run", "false")

# COMMAND ----------


def widget(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def optional_widget(name: str) -> str | None:
    value = widget(name)
    return value or None


def bool_widget(name: str) -> bool:
    return widget(name).lower() in {"1", "true", "yes", "y", "sim"}


output = optional_widget("output")
overwrite = bool_widget("overwrite")
dry_run = bool_widget("dry_run")

if not output:
    raise ValueError("Parameter 'output' is required.")

output_dir = resolve_output_dir(output, default_output=Path(output))

exit_code = download_taxi_zone_lookup(
    output_dir=output_dir,
    overwrite=overwrite,
    dry_run=dry_run,
)

if exit_code != 0:
    raise RuntimeError(f"Taxi Zone Lookup ingestion failed with exit code {exit_code}")

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "dry_run" if dry_run else "success",
            "output": str(output_dir),
            "overwrite": overwrite,
            "dry_run": dry_run,
        }
    )
)
