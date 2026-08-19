# Resumo:
# - Configura logs estruturados em JSON para scripts locais e jobs Databricks.
# - Mantem observabilidade simples sem adicionar dependencia externa.

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from enum import Enum
from typing import Any

from v2.platform.run_context import RunContext


def configure_logging(level: str | None = None) -> None:
    resolved_level = (level or os.getenv("NYC_TAXI_LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=resolved_level,
        format="%(message)s",
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    event: str,
    context: RunContext | None = None,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    payload: dict[str, Any] = {"event": event}

    if context is not None:
        payload.update(context.as_log_context())

    payload.update(fields)
    logger.log(level, json.dumps(payload, ensure_ascii=True, default=to_json_value))


def to_json_value(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()

    if isinstance(value, Enum):
        return value.value

    return str(value)
