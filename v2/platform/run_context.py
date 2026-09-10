# Resumo:
# - Define o contexto de uma execucao de pipeline.
# - Centraliza pipeline_run_id, janela de dados, ambiente e batch_id.

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from v2.config.settings import RuntimeEnvironment, load_settings


@dataclass(frozen=True)
class RunContext:
    pipeline_name: str
    pipeline_run_id: str
    environment: RuntimeEnvironment
    started_at: datetime
    year: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    batch_id: str | None = None

    @classmethod
    def create(
        cls,
        pipeline_name: str,
        year: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        batch_id: str | None = None,
        pipeline_run_id: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> RunContext:
        values = os.environ if env is None else env
        settings = load_settings(values)

        return cls(
            pipeline_name=pipeline_name,
            pipeline_run_id=pipeline_run_id
            or discover_pipeline_run_id(values)
            or build_pipeline_run_id(pipeline_name),
            environment=settings.environment,
            started_at=datetime.now(UTC),
            year=year,
            start_date=start_date,
            end_date=end_date,
            batch_id=batch_id,
        )

    def as_log_context(self) -> dict[str, object]:
        return {
            "pipeline_name": self.pipeline_name,
            "pipeline_run_id": self.pipeline_run_id,
            "environment": self.environment.value,
            "started_at": self.started_at.isoformat(),
            "year": self.year,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "batch_id": self.batch_id,
        }


def discover_pipeline_run_id(values: Mapping[str, str]) -> str | None:
    for name in (
        "PIPELINE_RUN_ID",
        "STEP_FUNCTIONS_EXECUTION_ID",
        "AWS_STEP_FUNCTIONS_EXECUTION_ID",
        "AWS_GLUE_JOB_RUN_ID",
        "GLUE_JOB_RUN_ID",
        "JOB_RUN_ID",
        "ADF_PIPELINE_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_RUN_ID",
    ):
        value = values.get(name)
        if value and value.strip():
            return value.strip()

    return None


def build_pipeline_run_id(pipeline_name: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    suffix = uuid4().hex[:8]
    return f"{pipeline_name}-{timestamp}-{suffix}"
