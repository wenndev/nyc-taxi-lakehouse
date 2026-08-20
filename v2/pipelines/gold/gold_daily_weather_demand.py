# Resumo:
# - Cria a Gold diaria para analise clima x demanda.
# - Entrega 1 linha por dia com metricas de taxi e clima consolidado.

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import (
    daily_weather_demand_gold_dir,
    noaa_silver_dir,
    nyc_tlc_silver_dir,
)
from v2.config.spark import create_spark
from v2.pipelines.gold.weather_consolidation import build_consolidated_daily_weather
from v2.platform.delta import write_delta_table
from v2.platform.partitions import (
    YEAR_MONTH_PARTITIONS,
    YearMonthPartition,
    filter_year_month_partition,
    resolve_year_month_partition,
    validate_replace_partition_write_mode,
)


def run_gold_daily_weather_demand(
    spark: SparkSession,
    tlc_input_path: str,
    noaa_input_path: str,
    output_path: str,
    year: int = 2025,
    mode: str = "overwrite",
    replace_partition: YearMonthPartition | None = None,
) -> DataFrame:
    validate_replace_partition_write_mode(mode, replace_partition)
    calendar = build_calendar(spark, year)
    demand = build_daily_taxi_demand(spark.read.format("delta").load(tlc_input_path), year)
    weather = build_consolidated_daily_weather(
        spark.read.format("delta").load(noaa_input_path),
        year=year,
    )

    df = (
        calendar.join(demand, on="data", how="left")
        .join(weather.withColumnRenamed("data_clima", "data"), on="data", how="left")
        .transform(fill_demand_nulls)
        .withColumn("sem_corridas", F.col("qtd_corridas") == 0)
        .withColumn("sem_clima", F.col("escopo_clima").isNull())
        .withColumn(
            "registro_alinhamento_incompleto",
            F.col("sem_clima")
            | F.coalesce(F.col("registro_clima_incompleto"), F.lit(False)),
        )
        .transform(lambda data_frame: filter_year_month_partition(data_frame, replace_partition))
        .orderBy("data")
    )

    write_delta_table(
        df,
        output_path,
        mode=mode,
        partition_by=YEAR_MONTH_PARTITIONS,
        replace_where=replace_partition.replace_where if replace_partition else None,
    )

    return df


def build_calendar(spark: SparkSession, year: int) -> DataFrame:
    start_date = date(year, 1, 1).isoformat()
    end_date = date(year, 12, 31).isoformat()

    return (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(start_date)),
                    F.to_date(F.lit(end_date)),
                    F.expr("interval 1 day"),
                )
            ).alias("data")
        )
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumn("dia_mes", F.dayofmonth("data"))
        .withColumn("dia_semana_num", F.dayofweek("data"))
        .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    )


def build_daily_taxi_demand(df: DataFrame, year: int) -> DataFrame:
    return (
        df.filter(F.col("ano") == year)
        .groupBy(F.col("data_viagem").alias("data"))
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.round(F.sum("valor_total"), 2).alias("valor_total_corridas"),
            F.round(F.avg("valor_total"), 2).alias("valor_medio_corrida"),
            F.round(F.avg("distancia_km"), 2).alias("distancia_media_km"),
            F.round(F.avg("duracao_minutos"), 2).cast("double").alias(
                "duracao_media_minutos"
            ),
            F.round(F.avg("qtd_passageiros"), 2).alias("media_passageiros"),
            F.sum(F.col("registro_suspeito").cast("int")).alias("qtd_registros_suspeitos"),
        )
    )


def fill_demand_nulls(df: DataFrame) -> DataFrame:
    return df.fillna(
        {
            "qtd_corridas": 0,
            "valor_total_corridas": 0.0,
            "qtd_registros_suspeitos": 0,
        }
    )


def is_local_path(path: str) -> bool:
    return "://" not in path


def delta_exists(path: str) -> bool:
    if not is_local_path(path):
        return True

    from pathlib import Path

    return (Path(path) / "_delta_log").exists()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create Gold daily table aligned by calendar, taxi demand and weather"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--tlc-input", default=None)
    parser.add_argument("--noaa-input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--replace-year", type=int, default=None)
    parser.add_argument("--replace-month", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()
    replace_partition = resolve_year_month_partition(
        base_year=args.year,
        replace_year=args.replace_year,
        replace_month=args.replace_month,
    )
    validate_replace_partition_write_mode(args.mode, replace_partition)

    tlc_input_path = args.tlc_input if args.tlc_input else str(nyc_tlc_silver_dir(args.year))
    noaa_input_path = args.noaa_input if args.noaa_input else str(noaa_silver_dir(args.year))
    output_path = args.output if args.output else str(daily_weather_demand_gold_dir(args.year))

    print(f"TLC input : {tlc_input_path}")
    print(f"NOAA input: {noaa_input_path}")
    print(f"Output    : {output_path}")
    print("Format    : silver delta + calendar -> gold delta")
    print("Grain     : 1 row per day")
    if replace_partition:
        print(f"ReplaceWhere: {replace_partition.replace_where}")

    if args.dry_run:
        return 0

    if not delta_exists(tlc_input_path):
        print(f"TLC Silver Delta not found: {tlc_input_path}")
        print("Run first: poetry run silver-nyc-tlc")
        return 1

    if not delta_exists(noaa_input_path):
        print(f"NOAA Silver Delta not found: {noaa_input_path}")
        print("Run first: poetry run silver-noaa-weather")
        return 1

    spark = create_spark("GoldDailyWeatherDemand")

    try:
        df_gold = run_gold_daily_weather_demand(
            spark=spark,
            tlc_input_path=tlc_input_path,
            noaa_input_path=noaa_input_path,
            output_path=output_path,
            year=args.year,
            mode=args.mode,
            replace_partition=replace_partition,
        )

        print("Gold daily weather demand saved.")
        df_gold.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_gold.count()}")

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
