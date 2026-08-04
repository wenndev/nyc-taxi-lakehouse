# Resumo:
# - Testa a validacao da Gold diaria de clima x demanda.
# - Garante PASS para base diaria completa e FAIL quando falta clima.

from __future__ import annotations

import unittest
from datetime import date, timedelta

from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.gold.validate_gold_daily_weather_demand import (
    DailyWeatherDemandValidationConfig,
    validate_daily_weather_demand_table,
)


class GoldDailyWeatherDemandValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestValidateGoldDailyWeatherDemand")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]):
        schema = T.StructType(
            [
                T.StructField("data", T.DateType(), nullable=False),
                T.StructField("ano", T.IntegerType(), nullable=False),
                T.StructField("mes", T.IntegerType(), nullable=False),
                T.StructField("dia_mes", T.IntegerType(), nullable=False),
                T.StructField("dia_semana_num", T.IntegerType(), nullable=False),
                T.StructField("fim_de_semana", T.BooleanType(), nullable=False),
                T.StructField("qtd_corridas", T.LongType(), nullable=False),
                T.StructField("valor_total_corridas", T.DoubleType(), nullable=False),
                T.StructField("qtd_registros_suspeitos", T.LongType(), nullable=False),
                T.StructField("fonte_clima", T.StringType(), nullable=True),
                T.StructField("escopo_clima", T.StringType(), nullable=True),
                T.StructField("qtd_estacoes", T.LongType(), nullable=True),
                T.StructField("cobertura_estacoes_pct", T.DoubleType(), nullable=True),
                T.StructField("precipitacao_media_mm", T.DoubleType(), nullable=True),
                T.StructField("precipitacao_max_mm", T.DoubleType(), nullable=True),
                T.StructField("temp_media_c", T.DoubleType(), nullable=True),
                T.StructField("temp_max_media_c", T.DoubleType(), nullable=True),
                T.StructField("temp_min_media_c", T.DoubleType(), nullable=True),
                T.StructField("teve_chuva", T.BooleanType(), nullable=True),
                T.StructField("teve_neve", T.BooleanType(), nullable=True),
                T.StructField("categoria_chuva", T.StringType(), nullable=True),
                T.StructField("categoria_temperatura", T.StringType(), nullable=True),
                T.StructField("sem_corridas", T.BooleanType(), nullable=False),
                T.StructField("sem_clima", T.BooleanType(), nullable=False),
                T.StructField(
                    "registro_alinhamento_incompleto",
                    T.BooleanType(),
                    nullable=False,
                ),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def valid_rows(self) -> list[tuple]:
        rows = []
        start_date = date(2025, 1, 1)

        for day_offset in range(365):
            current_date = start_date + timedelta(days=day_offset)
            qtd_corridas = 10 if day_offset == 0 else 0
            valor_total = 250.0 if day_offset == 0 else 0.0
            qtd_suspeitos = 1 if day_offset == 0 else 0
            teve_chuva = day_offset == 1
            categoria_chuva = "chuva_leve" if teve_chuva else "sem_chuva"
            sem_corridas = qtd_corridas == 0

            rows.append(
                (
                    current_date,
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    current_date.isoweekday(),
                    current_date.weekday() >= 5,
                    qtd_corridas,
                    valor_total,
                    qtd_suspeitos,
                    "NOAA_GHCND",
                    "NYC_consolidado",
                    90,
                    100.0,
                    2.5 if teve_chuva else 0.0,
                    6.0 if teve_chuva else 0.0,
                    4.5,
                    7.0,
                    2.0,
                    teve_chuva,
                    False,
                    categoria_chuva,
                    "frio",
                    sem_corridas,
                    False,
                    False,
                )
            )

        return rows

    def test_valid_daily_weather_demand_passes(self) -> None:
        result = validate_daily_weather_demand_table(
            df=self.make_df(self.valid_rows()),
            config=DailyWeatherDemandValidationConfig(
                expected_days=365,
                min_days_with_demand=1,
                min_total_trips=1,
            ),
            year=2025,
        )

        self.assertTrue(result.passed)

    def test_missing_weather_fails(self) -> None:
        rows = self.valid_rows()
        row = list(rows[1])
        row[9] = None
        row[23] = True
        row[24] = True
        rows[1] = tuple(row)

        result = validate_daily_weather_demand_table(
            df=self.make_df(rows),
            config=DailyWeatherDemandValidationConfig(
                expected_days=365,
                min_days_with_demand=1,
                min_total_trips=1,
            ),
            year=2025,
        )
        failed_checks = {
            check.name: check for check in result.checks if not check.passed
        }

        self.assertFalse(result.passed)
        self.assertIn("daily_days_without_weather", failed_checks)
        self.assertIn("daily_incomplete_alignment", failed_checks)
        self.assertIn("daily_null_fonte_clima", failed_checks)


if __name__ == "__main__":
    unittest.main()
