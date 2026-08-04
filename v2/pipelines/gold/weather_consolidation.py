# Resumo:
# - Consolida varias estacoes NOAA em um clima unico por data.
# - Evita duplicar corridas na fact ao juntar taxi com clima.

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_consolidated_daily_weather(
    df_noaa: DataFrame,
    year: int | None = None,
) -> DataFrame:
    df = df_noaa

    if year is not None:
        df = df.filter(F.year("data_clima") == year)

    complete_record = ~F.col("registro_clima_incompleto")
    has_precipitation = F.col("precipitacao_mm").isNotNull()
    has_temperature = F.col("temp_max_c").isNotNull() & F.col("temp_min_c").isNotNull()

    df = (
        df.groupBy("data_clima")
        .agg(
            F.countDistinct("id_estacao").alias("qtd_estacoes"),
            F.countDistinct(F.when(complete_record, F.col("id_estacao"))).alias(
                "qtd_estacoes_completas"
            ),
            F.countDistinct(F.when(has_precipitation, F.col("id_estacao"))).alias(
                "qtd_estacoes_com_precipitacao"
            ),
            F.countDistinct(F.when(has_temperature, F.col("id_estacao"))).alias(
                "qtd_estacoes_com_temperatura"
            ),
            F.round(F.avg("precipitacao_mm"), 2).alias("precipitacao_media_mm"),
            F.round(F.max("precipitacao_mm"), 2).alias("precipitacao_max_mm"),
            F.round(F.avg("temp_max_c"), 2).alias("temp_max_media_c"),
            F.round(F.avg("temp_min_c"), 2).alias("temp_min_media_c"),
            F.round(F.avg("temp_media_c"), 2).alias("temp_media_c"),
            F.round(F.avg("neve_mm"), 2).alias("neve_media_mm"),
            F.round(F.max("neve_mm"), 2).alias("neve_max_mm"),
            F.round(F.avg("neve_acumulada_mm"), 2).alias(
                "neve_acumulada_media_mm"
            ),
            F.round(F.max("neve_acumulada_mm"), 2).alias("neve_acumulada_max_mm"),
        )
        .withColumn(
            "cobertura_estacoes_pct",
            F.when(
                F.col("qtd_estacoes") > 0,
                F.round(
                    F.col("qtd_estacoes_completas") / F.col("qtd_estacoes") * 100,
                    2,
                ),
            ),
        )
        .withColumn("fonte_clima", F.lit("NOAA_GHCND"))
        .withColumn("escopo_clima", F.lit("NYC_consolidado"))
        .withColumn(
            "amplitude_termica_c",
            F.when(
                F.col("temp_max_media_c").isNotNull()
                & F.col("temp_min_media_c").isNotNull(),
                F.round(F.col("temp_max_media_c") - F.col("temp_min_media_c"), 2),
            ),
        )
        .withColumn("teve_chuva", F.col("precipitacao_max_mm") > 0)
        .withColumn(
            "teve_neve",
            (F.col("neve_max_mm") > 0) | (F.col("neve_acumulada_max_mm") > 0),
        )
        .withColumn("categoria_chuva", classify_rain(F.col("precipitacao_media_mm")))
        .withColumn(
            "categoria_temperatura",
            classify_temperature(F.col("temp_media_c")),
        )
        .withColumn(
            "registro_clima_incompleto",
            F.col("precipitacao_media_mm").isNull()
            | F.col("temp_max_media_c").isNull()
            | F.col("temp_min_media_c").isNull(),
        )
    )

    return df.select(
        "data_clima",
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


def classify_rain(precipitacao_media_mm):
    return (
        F.when(precipitacao_media_mm.isNull(), "desconhecida")
        .when(precipitacao_media_mm == 0, "sem_chuva")
        .when(precipitacao_media_mm <= 5, "chuva_leve")
        .when(precipitacao_media_mm <= 20, "chuva_moderada")
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
