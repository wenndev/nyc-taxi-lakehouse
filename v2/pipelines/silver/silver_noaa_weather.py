from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import (
    noaa_bronze_dir,
    noaa_quality_metrics_dir,
    noaa_quarantine_dir,
    noaa_silver_dir,
)
from v2.config.spark import create_spark
from v2.config.sources import NOAA_GHCND_NYC_STORAGE_ID
from v2.pipelines.quality.config import NOAAQualityConfig
from v2.pipelines.quality.exceptions import DataQualityCriticalError
from v2.pipelines.quality.models import QualityStatus
from v2.pipelines.quality.storage import write_quality_outputs
from v2.pipelines.quality.validators import validate_noaa_data


def run_silver_noaa_weather(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
    quarantine_path: str | None = None,
    metrics_path: str | None = None,
    pipeline_run_id: str | None = None,
    enable_quality: bool = True,
    quality_config: NOAAQualityConfig | None = None,
) -> DataFrame:
    df = spark.read.format("delta").load(input_path)
    df = normalize_results(df)

    if enable_quality:
        quality_result = validate_noaa_data(
            df=df,
            config=quality_config or NOAAQualityConfig(),
            pipeline_run_id=pipeline_run_id,
        )
        print_quality_summary(quality_result.metrics.as_row())

        if quarantine_path and metrics_path:
            write_quality_outputs(
                result=quality_result,
                quarantine_path=quarantine_path,
                metrics_path=metrics_path,
                quarantine_mode=mode,
            )

        if quality_result.status == QualityStatus.FAIL:
            raise DataQualityCriticalError(
                "NOAA Data Quality failed. Silver NOAA was not published. "
                f"pipeline_run_id={quality_result.pipeline_run_id}"
            )

        df = quality_result.valid_records
    else:
        df = filter_required_columns(df)

    df = build_daily_weather(df)
    df = add_derived_columns(df)

    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(output_path)

    return df


def normalize_results(df: DataFrame) -> DataFrame:
    return df.select(
        F.explode("results").alias("resultado"),
        F.col("arquivo_origem"),
        F.col("data_processamento_bronze"),
    ).select(
        F.to_date(F.col("resultado.date")).alias("data_clima"),
        F.col("resultado.station").alias("id_estacao"),
        F.col("resultado.datatype").alias("tipo_dado"),
        F.col("resultado.value").cast("double").alias("valor"),
        F.col("resultado.attributes").alias("atributos"),
        F.col("arquivo_origem"),
        F.col("data_processamento_bronze"),
    )


def filter_required_columns(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("data_clima").isNotNull()
        & F.col("id_estacao").isNotNull()
        & F.col("tipo_dado").isNotNull()
        & F.col("valor").isNotNull()
    ).dropDuplicates(["data_clima", "id_estacao", "tipo_dado"])


def build_daily_weather(df: DataFrame) -> DataFrame:
    return df.groupBy("data_clima", "id_estacao").agg(
        F.max(F.when(F.col("tipo_dado") == "PRCP", F.col("valor"))).alias(
            "precipitacao_mm"
        ),
        F.max(F.when(F.col("tipo_dado") == "TMAX", F.col("valor"))).alias("temp_max_c"),
        F.max(F.when(F.col("tipo_dado") == "TMIN", F.col("valor"))).alias("temp_min_c"),
        F.max(F.when(F.col("tipo_dado") == "SNOW", F.col("valor"))).alias("neve_mm"),
        F.max(F.when(F.col("tipo_dado") == "SNWD", F.col("valor"))).alias(
            "neve_acumulada_mm"
        ),
        F.size(F.collect_set("tipo_dado")).alias("qtd_tipos_dado"),
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ano", F.year("data_clima"))
        .withColumn("mes", F.month("data_clima"))
        .withColumn("dia_mes", F.dayofmonth("data_clima"))
        .withColumn(
            "temp_media_c",
            F.when(
                F.col("temp_max_c").isNotNull() & F.col("temp_min_c").isNotNull(),
                F.round((F.col("temp_max_c") + F.col("temp_min_c")) / 2, 2),
            ),
        )
        .withColumn(
            "amplitude_termica_c",
            F.when(
                F.col("temp_max_c").isNotNull() & F.col("temp_min_c").isNotNull(),
                F.round(F.col("temp_max_c") - F.col("temp_min_c"), 2),
            ),
        )
        .withColumn("teve_chuva", F.col("precipitacao_mm") > 0)
        .withColumn(
            "teve_neve",
            (F.col("neve_mm") > 0) | (F.col("neve_acumulada_mm") > 0),
        )
        .withColumn("categoria_chuva", classify_rain(F.col("precipitacao_mm")))
        .withColumn("categoria_temperatura", classify_temperature(F.col("temp_media_c")))
        .withColumn(
            "registro_clima_incompleto",
            F.col("precipitacao_mm").isNull()
            | F.col("temp_max_c").isNull()
            | F.col("temp_min_c").isNull()
            | F.col("neve_mm").isNull()
            | F.col("neve_acumulada_mm").isNull(),
        )
    )


def classify_rain(precipitacao_mm):
    return (
        F.when(precipitacao_mm.isNull(), "desconhecida")
        .when(precipitacao_mm == 0, "sem_chuva")
        .when(precipitacao_mm <= 5, "chuva_leve")
        .when(precipitacao_mm <= 20, "chuva_moderada")
        .otherwise("chuva_forte")
    )


def classify_temperature(temp_media_c):
    return (
        F.when(temp_media_c.isNull(), "desconhecida")
        .when(temp_media_c < 0, "muito_frio")
        .when(temp_media_c < 10, "frio")
        .when(temp_media_c < 20, "ameno")
        .when(temp_media_c < 28, "quente")
        .otherwise("muito_quente")
    )


def is_local_path(path: str) -> bool:
    return "://" not in path


def print_quality_summary(metrics: dict[str, object]) -> None:
    print("Data Quality NOAA summary:")
    print(f"  pipeline_run_id    : {metrics['pipeline_run_id']}")
    print(f"  status             : {metrics['pipeline_status']}")
    print(f"  total_records      : {metrics['total_records']}")
    print(f"  valid_records      : {metrics['valid_records']}")
    print(f"  invalid_records    : {metrics['invalid_records']}")
    print(f"  quality_percentage : {metrics['quality_percentage']}")
    print(f"  duplicate_count    : {metrics['duplicate_count']}")
    print(f"  null_error_count   : {metrics['null_error_count']}")
    print(f"  range_error_count  : {metrics['range_error_count']}")
    print(f"  future_date_count  : {metrics['future_date_count']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Create NOAA Weather Silver Delta table")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--datasetid", default=NOAA_GHCND_NYC_STORAGE_ID)
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--quarantine-output", default=None)
    parser.add_argument("--metrics-output", default=None)
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = args.input if args.input else str(noaa_bronze_dir(args.year, args.datasetid))
    output_path = args.output if args.output else str(noaa_silver_dir(args.year, args.datasetid))
    quarantine_path = (
        args.quarantine_output
        if args.quarantine_output
        else str(noaa_quarantine_dir(args.year, args.datasetid))
    )
    metrics_path = (
        args.metrics_output
        if args.metrics_output
        else str(noaa_quality_metrics_dir(args.year, args.datasetid))
    )

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print(f"Quarantine: {quarantine_path}")
    print(f"Metrics   : {metrics_path}")
    print("Format: delta -> delta")
    print(
        "Steps : explode NOAA results, filter required columns, pivot daily weather, "
        "add derived columns"
    )
    print(f"Quality: {'disabled' if args.skip_quality else 'enabled'}")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not (Path(input_path) / "_delta_log").exists():
        print(f"Bronze Delta not found: {input_path}")
        print("Run first: poetry run bronze-noaa-weather")
        return 1

    spark = create_spark("SilverNOAAWeather")

    try:
        df_silver = run_silver_noaa_weather(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
            quarantine_path=quarantine_path,
            metrics_path=metrics_path,
            pipeline_run_id=args.pipeline_run_id,
            enable_quality=not args.skip_quality,
        )

        print("Silver NOAA Weather saved.")
        df_silver.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_silver.count()}")

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
