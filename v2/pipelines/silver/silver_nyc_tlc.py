from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    expr,
    hour,
    month,
    round as spark_round,
    to_date,
    when,
    year,
)

from v2.config.paths import nyc_tlc_bronze_dir, nyc_tlc_silver_dir
from v2.config.spark import create_spark


COLUMN_RENAMES = {
    "VendorID": "id_vendedor",
    "tpep_pickup_datetime": "data_hora_partida",
    "tpep_dropoff_datetime": "data_hora_chegada",
    "passenger_count": "qtd_passageiros",
    "trip_distance": "distancia_milhas",
    "RatecodeID": "id_tarifa",
    "store_and_fwd_flag": "flag_armazenado_e_enviado",
    "PULocationID": "id_local_partida",
    "DOLocationID": "id_local_chegada",
    "payment_type": "tipo_pagamento",
    "fare_amount": "valor_tarifa",
    "extra": "taxa_extra",
    "mta_tax": "taxa_mta_fixa",
    "tip_amount": "gorjeta",
    "tolls_amount": "valor_pedagios",
    "improvement_surcharge": "sobretaxa_melhoria",
    "total_amount": "valor_total",
    "congestion_surcharge": "sobretaxa_transito",
    "Airport_fee": "taxa_aeroporto",
    "cbd_congestion_fee": "taxa_congestionamento_cbd",
}

NUMERIC_COLUMNS_TO_FILL = [
    "qtd_passageiros",
    "distancia_milhas",
    "valor_tarifa",
    "gorjeta",
    "valor_pedagios",
    "taxa_extra",
    "taxa_mta_fixa",
    "sobretaxa_melhoria",
    "sobretaxa_transito",
    "taxa_aeroporto",
    "taxa_congestionamento_cbd",
]

DEDUPLICATION_COLUMNS = [
    "id_vendedor",
    "data_hora_partida",
    "id_local_partida",
    "id_local_chegada",
    "valor_total",
]


def run_silver_nyc_tlc(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
    start_date: str | None = None,
    end_date: str | None = None,
    limit_rows: int | None = None,
) -> DataFrame:
    df = spark.read.format("delta").load(input_path)
    df = rename_columns(df)
    df = filter_date_range(df, start_date=start_date, end_date=end_date)
    if limit_rows:
        df = df.limit(limit_rows)
    df = filter_critical_columns(df)
    df = fill_numeric_nulls(df)
    df = filter_invalid_values(df)
    df = fill_categorical_nulls(df)
    df = add_derived_columns(df)
    df = add_semantic_columns(df)
    df = drop_business_duplicates(df)

    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(output_path)

    return df


def filter_date_range(
    df: DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
) -> DataFrame:
    if start_date:
        df = df.filter(col("data_hora_partida") >= start_date)

    if end_date:
        df = df.filter(col("data_hora_partida") < end_date)

    return df


def rename_columns(df: DataFrame) -> DataFrame:
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def filter_critical_columns(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("id_vendedor").isNotNull())
        & (col("id_vendedor") > 0)
        & (col("data_hora_partida").isNotNull())
        & (col("data_hora_chegada").isNotNull())
        & (col("data_hora_chegada") >= col("data_hora_partida"))
        & (col("id_local_partida").isNotNull())
        & (col("id_local_partida") > 0)
        & (col("id_local_chegada").isNotNull())
        & (col("id_local_chegada") > 0)
        & (col("valor_total").isNotNull())
        & (col("valor_total") > 0)
    )


def filter_invalid_values(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("distancia_milhas") >= 0)
        & (col("valor_total") >= 0)
        & (col("valor_tarifa") >= 0)
        & (col("data_hora_chegada") >= col("data_hora_partida"))
    )


def fill_numeric_nulls(df: DataFrame) -> DataFrame:
    for column_name in NUMERIC_COLUMNS_TO_FILL:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                when(col(column_name).isNull(), 0).otherwise(col(column_name)),
            )
    return df


def fill_categorical_nulls(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "flag_armazenado_e_enviado",
        when(col("flag_armazenado_e_enviado").isNull(), "DESCONHECIDO").otherwise(
            col("flag_armazenado_e_enviado")
        ),
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    hora_partida = hour(col("data_hora_partida"))
    dia_semana_num = dayofweek(col("data_hora_partida"))
    duracao_minutos = spark_round(
        expr("timestampdiff(SECOND, data_hora_partida, data_hora_chegada) / 60.0"),
        2,
    )
    distancia_km = spark_round(col("distancia_milhas") * 1.60934, 2)

    return (
        df.withColumn("data_viagem", to_date(col("data_hora_partida")))
        .withColumn("ano", year(col("data_hora_partida")))
        .withColumn("mes", month(col("data_hora_partida")))
        .withColumn("mes_nome", translate_month(month(col("data_hora_partida"))))
        .withColumn("dia_mes", dayofmonth(col("data_hora_partida")))
        .withColumn("hora_partida", hora_partida)
        .withColumn("dia_semana_num", dia_semana_num)
        .withColumn("dia_semana_nome", translate_day_of_week(dia_semana_num))
        .withColumn("fim_de_semana", dia_semana_num.isin(1, 7))
        .withColumn("periodo_dia", classify_day_period(hora_partida))
        .withColumn("horario_pico", is_peak_hour(hora_partida, dia_semana_num))
        .withColumn("duracao_minutos", duracao_minutos)
        .withColumn("distancia_km", distancia_km)
        .withColumn(
            "valor_por_km",
            when(distancia_km > 0, spark_round(col("valor_total") / distancia_km, 2)),
        )
        .withColumn(
            "velocidade_media_kmh",
            when(
                duracao_minutos > 0,
                spark_round(distancia_km / (duracao_minutos / 60), 2),
            ),
        )
    )


def add_semantic_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tipo_pagamento_desc", describe_payment_type(col("tipo_pagamento")))
        .withColumn("tipo_tarifa_desc", describe_rate_code(col("id_tarifa")))
        .withColumn("categoria_distancia", classify_distance(col("distancia_milhas")))
        .withColumn("categoria_duracao", classify_duration(col("duracao_minutos")))
        .withColumn("categoria_valor_total", classify_total_amount(col("valor_total")))
        .withColumn("viagem_com_passageiro", col("qtd_passageiros") > 0)
        .withColumn("viagem_sem_passageiro", col("qtd_passageiros") == 0)
        .withColumn("valor_tarifa_zero", col("valor_tarifa") == 0)
        .withColumn(
            "qtd_passageiros_suspeita",
            (col("qtd_passageiros") < 0) | (col("qtd_passageiros") > 6),
        )
        .withColumn("viagem_distancia_zero", col("distancia_milhas") == 0)
        .withColumn("viagem_distancia_alta", col("distancia_milhas") > 100)
        .withColumn("viagem_duracao_zero", col("duracao_minutos") <= 0)
        .withColumn("viagem_duracao_alta", col("duracao_minutos") > 180)
        .withColumn("viagem_valor_alto", col("valor_total") > 500)
        .withColumn("velocidade_media_alta", col("velocidade_media_kmh") > 120)
        .withColumn(
            "registro_suspeito",
            col("qtd_passageiros_suspeita")
            | col("viagem_sem_passageiro")
            | col("viagem_distancia_zero")
            | col("viagem_distancia_alta")
            | col("viagem_duracao_zero")
            | col("viagem_duracao_alta")
            | col("valor_tarifa_zero")
            | col("viagem_valor_alto")
            | col("velocidade_media_alta"),
        )
    )


def describe_payment_type(tipo_pagamento):
    return (
        when(tipo_pagamento == 1, "cartao_credito")
        .when(tipo_pagamento == 2, "dinheiro")
        .when(tipo_pagamento == 3, "sem_cobranca")
        .when(tipo_pagamento == 4, "disputa")
        .when(tipo_pagamento == 5, "desconhecido")
        .when(tipo_pagamento == 6, "viagem_cancelada")
        .otherwise("desconhecido")
    )


def describe_rate_code(id_tarifa):
    return (
        when(id_tarifa == 1, "tarifa_padrao")
        .when(id_tarifa == 2, "jfk")
        .when(id_tarifa == 3, "newark")
        .when(id_tarifa == 4, "nassau_westchester")
        .when(id_tarifa == 5, "tarifa_negociada")
        .when(id_tarifa == 6, "viagem_compartilhada")
        .when(id_tarifa == 99, "desconhecida")
        .otherwise("desconhecida")
    )


def classify_distance(distancia_milhas):
    return (
        when(distancia_milhas == 0, "zero")
        .when(distancia_milhas <= 1, "curta")
        .when(distancia_milhas <= 5, "media")
        .when(distancia_milhas <= 20, "longa")
        .otherwise("muito_longa")
    )


def classify_duration(duracao_minutos):
    return (
        when(duracao_minutos <= 0, "zero")
        .when(duracao_minutos <= 10, "curta")
        .when(duracao_minutos <= 30, "media")
        .when(duracao_minutos <= 60, "longa")
        .otherwise("muito_longa")
    )


def classify_total_amount(valor_total):
    return (
        when(valor_total <= 10, "baixo")
        .when(valor_total <= 50, "medio")
        .when(valor_total <= 150, "alto")
        .otherwise("muito_alto")
    )


def translate_month(mes):
    return (
        when(mes == 1, "janeiro")
        .when(mes == 2, "fevereiro")
        .when(mes == 3, "marco")
        .when(mes == 4, "abril")
        .when(mes == 5, "maio")
        .when(mes == 6, "junho")
        .when(mes == 7, "julho")
        .when(mes == 8, "agosto")
        .when(mes == 9, "setembro")
        .when(mes == 10, "outubro")
        .when(mes == 11, "novembro")
        .when(mes == 12, "dezembro")
    )


def translate_day_of_week(dia_semana_num):
    return (
        when(dia_semana_num == 1, "domingo")
        .when(dia_semana_num == 2, "segunda")
        .when(dia_semana_num == 3, "terca")
        .when(dia_semana_num == 4, "quarta")
        .when(dia_semana_num == 5, "quinta")
        .when(dia_semana_num == 6, "sexta")
        .when(dia_semana_num == 7, "sabado")
    )


def classify_day_period(hora_partida):
    return (
        when((hora_partida >= 0) & (hora_partida <= 5), "madrugada")
        .when((hora_partida >= 6) & (hora_partida <= 11), "manha")
        .when((hora_partida >= 12) & (hora_partida <= 17), "tarde")
        .otherwise("noite")
    )


def is_peak_hour(hora_partida, dia_semana_num):
    dia_util = dia_semana_num.between(2, 6)
    pico_manha = (hora_partida >= 7) & (hora_partida <= 9)
    pico_tarde = (hora_partida >= 16) & (hora_partida <= 19)
    return dia_util & (pico_manha | pico_tarde)


def drop_business_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(DEDUPLICATION_COLUMNS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create NYC TLC Silver Delta table")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input) if args.input else nyc_tlc_bronze_dir(args.year)
    output_path = Path(args.output) if args.output else nyc_tlc_silver_dir(args.year)

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("Format: delta -> delta")
    print(
        "Steps : rename columns, filter critical columns, fill nulls, "
        "filter invalid values, add derived columns, add semantic columns, "
        "drop duplicates"
    )
    if args.start_date or args.end_date:
        print(f"Date filter: {args.start_date or 'beginning'} -> {args.end_date or 'end'}")
    if args.limit:
        print(f"Limit : {args.limit} rows")

    if args.dry_run:
        return 0

    if not (input_path / "_delta_log").exists():
        print(f"Bronze Delta not found: {input_path}")
        print("Run first: poetry run bronze-nyc-tlc")
        return 1

    spark = create_spark("SilverNYCTLC")

    try:
        df_silver = run_silver_nyc_tlc(
            spark=spark,
            input_path=str(input_path),
            output_path=str(output_path),
            mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
            limit_rows=args.limit,
        )

        print("Silver NYC TLC saved.")
        df_silver.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_silver.count()}")

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
