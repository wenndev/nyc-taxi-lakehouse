from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from v2.config.paths import noaa_silver_dir, nyc_tlc_silver_dir, star_schema_gold_dir
from v2.config.spark import create_spark
from v2.pipelines.gold.weather_consolidation import build_consolidated_daily_weather


@dataclass
class GoldStarSchemaTables:
    dim_data: DataFrame
    dim_clima: DataFrame
    dim_localizacao: DataFrame
    fact_trips: DataFrame


def run_gold_star_schema(
    spark: SparkSession,
    tlc_input_path: str,
    noaa_input_path: str,
    output_path: str,
    year: int = 2025,
    mode: str = "overwrite",
) -> GoldStarSchemaTables:
    df_tlc = spark.read.format("delta").load(tlc_input_path)
    df_noaa = spark.read.format("delta").load(noaa_input_path)

    dim_data = build_dim_data(spark, year)
    dim_clima = build_dim_clima(df_noaa, year)
    dim_localizacao = build_dim_localizacao(df_tlc)
    fact_trips = build_fact_trips(
        df_tlc=df_tlc,
        dim_data=dim_data,
        dim_clima=dim_clima,
        dim_localizacao=dim_localizacao,
        year=year,
    )

    tables = GoldStarSchemaTables(
        dim_data=dim_data,
        dim_clima=dim_clima,
        dim_localizacao=dim_localizacao,
        fact_trips=fact_trips,
    )
    write_gold_tables(tables, output_path=output_path, mode=mode)

    return tables


def build_dim_data(spark: SparkSession, year: int) -> DataFrame:
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
        .withColumn("data_id", F.date_format("data", "yyyyMMdd").cast("int"))
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumn("dia_mes", F.dayofmonth("data"))
        .withColumn("dia_semana_num", F.dayofweek("data"))
        .withColumn("dia_semana_nome", translate_day_of_week(F.col("dia_semana_num")))
        .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
        .select(
            "data_id",
            "data",
            "ano",
            "mes",
            "dia_mes",
            "dia_semana_num",
            "dia_semana_nome",
            "fim_de_semana",
        )
    )


def build_dim_clima(df_noaa: DataFrame, year: int) -> DataFrame:
    return (
        build_consolidated_daily_weather(df_noaa, year=year)
        .withColumnRenamed("data_clima", "data")
        .withColumn("clima_id", F.date_format("data", "yyyyMMdd").cast("int"))
        .select(
            "clima_id",
            "data",
            "fonte_clima",
            "escopo_clima",
            "qtd_estacoes",
            "qtd_estacoes_completas",
            "cobertura_estacoes_pct",
            "qtd_estacoes_com_precipitacao",
            "qtd_estacoes_com_temperatura",
            "precipitacao_media_mm",
            "precipitacao_max_mm",
            "temp_max_media_c",
            "temp_min_media_c",
            "temp_media_c",
            "amplitude_termica_c",
            "neve_media_mm",
            "neve_max_mm",
            "neve_acumulada_media_mm",
            "neve_acumulada_max_mm",
            "teve_chuva",
            "teve_neve",
            "categoria_chuva",
            "categoria_temperatura",
            "registro_clima_incompleto",
        )
    )


def build_dim_localizacao(df_tlc: DataFrame) -> DataFrame:
    return (
        df_tlc.select(F.col("id_local_partida").alias("location_id"))
        .union(df_tlc.select(F.col("id_local_chegada").alias("location_id")))
        .filter(F.col("location_id").isNotNull())
        .distinct()
        .withColumn("localizacao_id", F.col("location_id").cast("int"))
        .select("localizacao_id", "location_id")
    )


def build_fact_trips(
    df_tlc: DataFrame,
    dim_data: DataFrame,
    dim_clima: DataFrame,
    dim_localizacao: DataFrame,
    year: int,
) -> DataFrame:
    dim_partida = dim_localizacao.select(
        F.col("location_id").alias("id_local_partida"),
        F.col("localizacao_id").alias("localizacao_partida_id"),
    )
    dim_chegada = dim_localizacao.select(
        F.col("location_id").alias("id_local_chegada"),
        F.col("localizacao_id").alias("localizacao_chegada_id"),
    )

    return (
        df_tlc.filter(F.col("ano") == year)
        .withColumn("data", F.col("data_viagem"))
        .join(dim_data.select("data_id", "data"), on="data", how="left")
        .join(dim_clima.select("clima_id", "data"), on="data", how="left")
        .join(dim_partida, on="id_local_partida", how="left")
        .join(dim_chegada, on="id_local_chegada", how="left")
        .select(
            "data_id",
            "localizacao_partida_id",
            "localizacao_chegada_id",
            "clima_id",
            "data_hora_partida",
            "data_hora_chegada",
            "duracao_minutos",
            "qtd_passageiros",
            "tipo_pagamento",
            "tipo_pagamento_desc",
            "distancia_milhas",
            "distancia_km",
            "valor_total",
            "gorjeta",
            "id_tarifa",
            "tipo_tarifa_desc",
            "periodo_dia",
            "horario_pico",
            "fim_de_semana",
            "registro_suspeito",
        )
    )


def translate_day_of_week(dia_semana_num):
    return (
        F.when(dia_semana_num == 1, "domingo")
        .when(dia_semana_num == 2, "segunda")
        .when(dia_semana_num == 3, "terca")
        .when(dia_semana_num == 4, "quarta")
        .when(dia_semana_num == 5, "quinta")
        .when(dia_semana_num == 6, "sexta")
        .when(dia_semana_num == 7, "sabado")
    )


def write_gold_tables(
    tables: GoldStarSchemaTables,
    output_path: str,
    mode: str,
) -> None:
    for table_name, df in [
        ("dim_data", tables.dim_data),
        ("dim_clima", tables.dim_clima),
        ("dim_localizacao", tables.dim_localizacao),
        ("fact_trips", tables.fact_trips),
    ]:
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(
            table_path(output_path, table_name)
        )


def table_path(output_path: str, table_name: str) -> str:
    return f"{output_path.rstrip('/')}/{table_name}"


def is_local_path(path: str) -> bool:
    return "://" not in path


def delta_exists(path: str) -> bool:
    if not is_local_path(path):
        return True

    from pathlib import Path

    return (Path(path) / "_delta_log").exists()


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Gold Star Schema Delta tables")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--tlc-input", default=None)
    parser.add_argument("--noaa-input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    tlc_input_path = args.tlc_input if args.tlc_input else str(nyc_tlc_silver_dir(args.year))
    noaa_input_path = args.noaa_input if args.noaa_input else str(noaa_silver_dir(args.year))
    output_path = args.output if args.output else str(star_schema_gold_dir(args.year))

    print(f"TLC input : {tlc_input_path}")
    print(f"NOAA input: {noaa_input_path}")
    print(f"Output    : {output_path}")
    print("Format    : silver delta -> gold star schema delta")
    print("Tables    : dim_data, dim_clima, dim_localizacao, fact_trips")

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

    spark = create_spark("GoldStarSchema")

    try:
        tables = run_gold_star_schema(
            spark=spark,
            tlc_input_path=tlc_input_path,
            noaa_input_path=noaa_input_path,
            output_path=output_path,
            year=args.year,
            mode=args.mode,
        )

        print("Gold Star Schema saved.")

        if not args.skip_count:
            print(f"dim_data        : {tables.dim_data.count()}")
            print(f"dim_clima       : {tables.dim_clima.count()}")
            print(f"dim_localizacao : {tables.dim_localizacao.count()}")
            print(f"fact_trips      : {tables.fact_trips.count()}")

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
