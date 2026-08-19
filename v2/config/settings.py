# Resumo:
# - Centraliza configuracoes de ambiente, storage local e perfil Spark.
# - Prepara a V2 para trocar local/cloud por configuracao, sem mudar a regra de negocio.

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class RuntimeEnvironment(StrEnum):
    LOCAL = "local"
    DATABRICKS = "databricks"
    AZURE = "azure"


class StorageMode(StrEnum):
    LOCAL = "local"
    DATABRICKS_VOLUME = "databricks_volume"
    ADLS = "adls"


@dataclass(frozen=True)
class SparkSettings:
    master: str | None
    driver_memory: str
    driver_max_result_size: str
    shuffle_partitions: str
    max_partition_bytes: str
    delta_snapshot_partitions: str
    session_timezone: str
    debug_max_to_string_fields: str


@dataclass(frozen=True)
class ProjectSettings:
    environment: RuntimeEnvironment
    storage_mode: StorageMode
    v2_root: Path
    raw_root: Path
    delta_root: Path
    spark: SparkSettings

    @property
    def gold_root(self) -> Path:
        return self.delta_root / "gold"

    @property
    def quarantine_root(self) -> Path:
        return self.delta_root / "quarantine"

    @property
    def monitoring_root(self) -> Path:
        return self.delta_root / "monitoring"


def load_settings(env: Mapping[str, str] | None = None) -> ProjectSettings:
    values = os.environ if env is None else env
    runtime_environment = parse_runtime_environment(
        get_config_value(values, "NYC_TAXI_ENV", RuntimeEnvironment.LOCAL.value)
    )
    storage_mode = parse_storage_mode(
        get_config_value(values, "NYC_TAXI_STORAGE_MODE", StorageMode.LOCAL.value)
    )

    v2_root = config_path(
        get_config_value(values, "NYC_TAXI_V2_ROOT", str(default_v2_root()))
    )
    raw_root = config_path(
        get_config_value(values, "NYC_TAXI_RAW_ROOT", str(v2_root / "data" / "raw"))
    )
    delta_root = config_path(
        get_config_value(values, "NYC_TAXI_DELTA_ROOT", str(v2_root / "data" / "delta"))
    )

    return ProjectSettings(
        environment=runtime_environment,
        storage_mode=storage_mode,
        v2_root=v2_root,
        raw_root=raw_root,
        delta_root=delta_root,
        spark=SparkSettings(
            master=get_optional_config_value(
                values,
                "NYC_TAXI_SPARK_MASTER",
                default_spark_master(runtime_environment),
            ),
            driver_memory=get_config_value(values, "NYC_TAXI_SPARK_DRIVER_MEMORY", "1g"),
            driver_max_result_size=get_config_value(
                values,
                "NYC_TAXI_SPARK_DRIVER_MAX_RESULT_SIZE",
                "512m",
            ),
            shuffle_partitions=get_config_value(
                values,
                "NYC_TAXI_SPARK_SHUFFLE_PARTITIONS",
                "16",
            ),
            max_partition_bytes=get_config_value(
                values,
                "NYC_TAXI_SPARK_MAX_PARTITION_BYTES",
                "32m",
            ),
            delta_snapshot_partitions=get_config_value(
                values,
                "NYC_TAXI_DELTA_SNAPSHOT_PARTITIONS",
                "4",
            ),
            session_timezone=get_config_value(
                values,
                "NYC_TAXI_SPARK_SESSION_TIMEZONE",
                "UTC",
            ),
            debug_max_to_string_fields=get_config_value(
                values,
                "NYC_TAXI_SPARK_DEBUG_MAX_TO_STRING_FIELDS",
                "200",
            ),
        ),
    )


def default_v2_root() -> Path:
    return Path(__file__).resolve().parents[1]


def config_path(value: str) -> Path:
    return Path(value).expanduser()


def get_config_value(
    values: Mapping[str, str],
    name: str,
    default: str,
) -> str:
    value = values.get(name)
    if value is None or value.strip() == "":
        return default

    return value.strip()


def get_optional_config_value(
    values: Mapping[str, str],
    name: str,
    default: str | None,
) -> str | None:
    value = values.get(name)
    if value is None:
        return default

    value = value.strip()
    return value or None


def default_spark_master(environment: RuntimeEnvironment) -> str | None:
    if environment == RuntimeEnvironment.LOCAL:
        return "local[1]"

    return None


def parse_runtime_environment(value: str) -> RuntimeEnvironment:
    normalized = value.strip().lower()
    try:
        return RuntimeEnvironment(normalized)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in RuntimeEnvironment)
        raise ValueError(f"Invalid NYC_TAXI_ENV={value!r}. Expected one of: {allowed}") from exc


def parse_storage_mode(value: str) -> StorageMode:
    normalized = value.strip().lower()
    try:
        return StorageMode(normalized)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in StorageMode)
        raise ValueError(
            f"Invalid NYC_TAXI_STORAGE_MODE={value!r}. Expected one of: {allowed}"
        ) from exc
