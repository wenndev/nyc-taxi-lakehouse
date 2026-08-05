# Resumo:
# - Testa a validacao pos-Silver da NOAA.
# - Garante cobertura diaria, grao estacao/dia e deteccao de duplicatas.

from __future__ import annotations

import unittest
from datetime import date

from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.silver.validate_silver_noaa_weather import (
    SilverNOAAValidationConfig,
    validate_silver_noaa_weather_table,
)


class SilverNOAAWeatherValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestValidateSilverNOAAWeather")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]):
        schema = T.StructType(
            [
                T.StructField("data_clima", T.DateType(), nullable=True),
                T.StructField("id_estacao", T.StringType(), nullable=True),
                T.StructField("precipitacao_mm", T.DoubleType(), nullable=True),
                T.StructField("temp_max_c", T.DoubleType(), nullable=True),
                T.StructField("temp_min_c", T.DoubleType(), nullable=True),
                T.StructField("temp_media_c", T.DoubleType(), nullable=True),
                T.StructField("amplitude_termica_c", T.DoubleType(), nullable=True),
                T.StructField("neve_mm", T.DoubleType(), nullable=True),
                T.StructField("neve_acumulada_mm", T.DoubleType(), nullable=True),
                T.StructField("qtd_tipos_dado", T.IntegerType(), nullable=True),
                T.StructField("ano", T.IntegerType(), nullable=True),
                T.StructField("mes", T.IntegerType(), nullable=True),
                T.StructField("dia_mes", T.IntegerType(), nullable=True),
                T.StructField("teve_chuva", T.BooleanType(), nullable=True),
                T.StructField("teve_neve", T.BooleanType(), nullable=True),
                T.StructField("categoria_chuva", T.StringType(), nullable=True),
                T.StructField("categoria_temperatura", T.StringType(), nullable=True),
                T.StructField(
                    "registro_clima_incompleto",
                    T.BooleanType(),
                    nullable=True,
                ),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def valid_rows(self) -> list[tuple]:
        return [
            (
                date(2025, 1, 1),
                "GHCND:STATION_A",
                0.0,
                7.0,
                2.0,
                4.5,
                5.0,
                0.0,
                0.0,
                5,
                2025,
                1,
                1,
                False,
                False,
                "sem_chuva",
                "frio",
                False,
            ),
            (
                date(2025, 1, 1),
                "GHCND:STATION_B",
                1.0,
                8.0,
                3.0,
                5.5,
                5.0,
                0.0,
                0.0,
                5,
                2025,
                1,
                1,
                True,
                False,
                "chuva_leve",
                "frio",
                False,
            ),
            (
                date(2025, 1, 2),
                "GHCND:STATION_A",
                0.0,
                9.0,
                4.0,
                6.5,
                5.0,
                0.0,
                0.0,
                5,
                2025,
                1,
                2,
                False,
                False,
                "sem_chuva",
                "frio",
                False,
            ),
            (
                date(2025, 1, 2),
                "GHCND:STATION_B",
                0.0,
                10.0,
                5.0,
                7.5,
                5.0,
                0.0,
                0.0,
                5,
                2025,
                1,
                2,
                False,
                False,
                "sem_chuva",
                "frio",
                False,
            ),
        ]

    def test_valid_noaa_silver_passes(self) -> None:
        result = validate_silver_noaa_weather_table(
            df=self.make_df(self.valid_rows()),
            config=SilverNOAAValidationConfig(
                year=2025,
                expected_days=2,
                min_rows=4,
                min_stations=2,
            ),
        )

        self.assertTrue(result.passed)

    def test_duplicate_station_day_fails(self) -> None:
        rows = self.valid_rows()
        rows[3] = rows[2]

        result = validate_silver_noaa_weather_table(
            df=self.make_df(rows),
            config=SilverNOAAValidationConfig(
                year=2025,
                expected_days=2,
                min_rows=4,
                min_stations=2,
            ),
        )
        failed_checks = {
            check.name: check for check in result.checks if not check.passed
        }

        self.assertFalse(result.passed)
        self.assertIn("noaa_duplicate_station_day", failed_checks)


if __name__ == "__main__":
    unittest.main()
