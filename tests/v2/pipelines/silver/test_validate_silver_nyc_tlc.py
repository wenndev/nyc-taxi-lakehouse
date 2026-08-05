# Resumo:
# - Testa a validacao pos-Silver da NYC TLC.
# - Garante PASS para corridas validas e FAIL para duplicidade/valores invalidos.

from __future__ import annotations

import unittest
from datetime import date, datetime

from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.silver.validate_silver_nyc_tlc import (
    SilverNYCTLCValidationConfig,
    validate_silver_nyc_tlc_table,
)


class SilverNYCTLCValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestValidateSilverNYCTLC")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]):
        schema = T.StructType(
            [
                T.StructField("id_vendedor", T.IntegerType(), nullable=True),
                T.StructField("data_hora_partida", T.TimestampType(), nullable=True),
                T.StructField("data_hora_chegada", T.TimestampType(), nullable=True),
                T.StructField("qtd_passageiros", T.IntegerType(), nullable=True),
                T.StructField("distancia_milhas", T.DoubleType(), nullable=True),
                T.StructField("id_tarifa", T.IntegerType(), nullable=True),
                T.StructField("id_local_partida", T.IntegerType(), nullable=True),
                T.StructField("id_local_chegada", T.IntegerType(), nullable=True),
                T.StructField("tipo_pagamento", T.IntegerType(), nullable=True),
                T.StructField("tipo_pagamento_desc", T.StringType(), nullable=True),
                T.StructField("valor_total", T.DoubleType(), nullable=True),
                T.StructField("data_viagem", T.DateType(), nullable=True),
                T.StructField("ano", T.IntegerType(), nullable=True),
                T.StructField("mes", T.IntegerType(), nullable=True),
                T.StructField("dia_mes", T.IntegerType(), nullable=True),
                T.StructField("hora_partida", T.IntegerType(), nullable=True),
                T.StructField("dia_semana_num", T.IntegerType(), nullable=True),
                T.StructField("dia_semana_nome", T.StringType(), nullable=True),
                T.StructField("fim_de_semana", T.BooleanType(), nullable=True),
                T.StructField("periodo_dia", T.StringType(), nullable=True),
                T.StructField("horario_pico", T.BooleanType(), nullable=True),
                T.StructField("duracao_minutos", T.DoubleType(), nullable=True),
                T.StructField("distancia_km", T.DoubleType(), nullable=True),
                T.StructField("registro_suspeito", T.BooleanType(), nullable=True),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def valid_rows(self) -> list[tuple]:
        return [
            (
                2,
                datetime(2025, 1, 1, 8, 0, 0),
                datetime(2025, 1, 1, 8, 20, 0),
                1,
                3.0,
                1,
                236,
                161,
                1,
                "cartao_credito",
                28.5,
                date(2025, 1, 1),
                2025,
                1,
                1,
                8,
                4,
                "quarta",
                False,
                "manha",
                True,
                20.0,
                4.83,
                False,
            ),
            (
                2,
                datetime(2025, 1, 2, 9, 0, 0),
                datetime(2025, 1, 2, 9, 15, 0),
                2,
                2.0,
                1,
                162,
                170,
                2,
                "dinheiro",
                19.0,
                date(2025, 1, 2),
                2025,
                1,
                2,
                9,
                5,
                "quinta",
                False,
                "manha",
                True,
                15.0,
                3.22,
                False,
            ),
        ]

    def test_valid_tlc_silver_passes(self) -> None:
        result = validate_silver_nyc_tlc_table(
            df=self.make_df(self.valid_rows()),
            config=SilverNYCTLCValidationConfig(
                year=2025,
                min_rows=1,
                expected_days=2,
            ),
        )

        self.assertTrue(result.passed)

    def test_duplicate_and_invalid_value_fail(self) -> None:
        rows = self.valid_rows()
        duplicate = rows[0]
        invalid = list(rows[1])
        invalid[10] = 0.0
        rows = [rows[0], duplicate, tuple(invalid)]

        result = validate_silver_nyc_tlc_table(
            df=self.make_df(rows),
            config=SilverNYCTLCValidationConfig(
                year=2025,
                min_rows=1,
                expected_days=2,
            ),
        )
        failed_checks = {
            check.name: check for check in result.checks if not check.passed
        }

        self.assertFalse(result.passed)
        self.assertIn("tlc_business_duplicates", failed_checks)
        self.assertIn("tlc_invalid_valor_total", failed_checks)


if __name__ == "__main__":
    unittest.main()
