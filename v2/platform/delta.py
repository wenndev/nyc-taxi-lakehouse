# Resumo:
# - Centraliza leitura de existencia e escrita Delta Lake.
# - Prepara idempotencia futura com overwrite controlado, append, partitionBy e replaceWhere.

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from pyspark.sql import DataFrame


class DeltaWriteMode(StrEnum):
    OVERWRITE = "overwrite"
    APPEND = "append"


@dataclass(frozen=True)
class DeltaWriteConfig:
    mode: DeltaWriteMode = DeltaWriteMode.OVERWRITE
    overwrite_schema: bool = True
    merge_schema: bool = False
    partition_by: tuple[str, ...] = ()
    replace_where: str | None = None


def write_delta_table(
    df: DataFrame,
    path: str,
    mode: str | DeltaWriteMode = DeltaWriteMode.OVERWRITE,
    overwrite_schema: bool = True,
    merge_schema: bool = False,
    partition_by: str | Sequence[str] | None = None,
    replace_where: str | None = None,
) -> None:
    """
    Escreve um DataFrame em Delta usando uma configuracao padronizada.

    `replace_where` fica preparado para reprocessamento idempotente por recorte,
    mas so pode ser usado junto com `mode="overwrite"`.
    """
    config = DeltaWriteConfig(
        mode=parse_delta_write_mode(mode),
        overwrite_schema=overwrite_schema,
        merge_schema=merge_schema,
        partition_by=normalize_partition_columns(partition_by),
        replace_where=replace_where,
    )
    validate_delta_write_config(config)

    writer = df.write.format("delta").mode(config.mode.value)

    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)

    if config.replace_where:
        writer = writer.option("replaceWhere", config.replace_where)

    if (
        config.mode == DeltaWriteMode.OVERWRITE
        and config.overwrite_schema
        and not config.replace_where
    ):
        writer = writer.option("overwriteSchema", "true")

    if config.merge_schema:
        writer = writer.option("mergeSchema", "true")

    writer.save(path)


def parse_delta_write_mode(mode: str | DeltaWriteMode) -> DeltaWriteMode:
    if isinstance(mode, DeltaWriteMode):
        return mode

    normalized = mode.strip().lower()
    try:
        return DeltaWriteMode(normalized)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in DeltaWriteMode)
        raise ValueError(f"Invalid Delta write mode={mode!r}. Expected: {allowed}") from exc


def normalize_partition_columns(
    partition_by: str | Sequence[str] | None,
) -> tuple[str, ...]:
    if partition_by is None:
        return ()

    if isinstance(partition_by, str):
        return (partition_by,)

    return tuple(column for column in partition_by if column)


def validate_delta_write_config(config: DeltaWriteConfig) -> None:
    if config.replace_where and config.mode != DeltaWriteMode.OVERWRITE:
        raise ValueError("replace_where can only be used with mode='overwrite'")


def is_local_path(path: str) -> bool:
    return "://" not in path


def delta_table_exists(path: str) -> bool:
    if not is_local_path(path):
        return True

    return (Path(path) / "_delta_log").exists()
