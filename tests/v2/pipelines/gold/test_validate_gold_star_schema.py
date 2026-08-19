# Resumo:
# - Testa a validacao automatica da Gold Star Schema.
# - Garante PASS para tabelas consistentes e FAIL para chaves orfas.

from __future__ import annotations

import unittest
from datetime import date, datetime

from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.gold.validate_gold_star_schema import (
    GoldStarSchemaDataFrames,
    GoldValidationConfig,
    validate_gold_star_schema_tables,
)


class GoldStarSchemaValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestValidateGoldStarSchema")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_valid_tables(self) -> GoldStarSchemaDataFrames:
        dim_data = self.spark.createDataFrame(
            [
                (20250101, date(2025, 1, 1)),
                (20250102, date(2025, 1, 2)),
            ],
            schema=T.StructType(
                [
                    T.StructField("data_id", T.IntegerType(), nullable=False),
                    T.StructField("data", T.DateType(), nullable=False),
                ]
            ),
        )
        dim_clima = self.spark.createDataFrame(
            [
                (20250101, date(2025, 1, 1), False),
                (20250102, date(2025, 1, 2), False),
            ],
            schema=T.StructType(
                [
                    T.StructField("clima_id", T.IntegerType(), nullable=False),
                    T.StructField("data", T.DateType(), nullable=False),
                    T.StructField(
                        "registro_clima_incompleto",
                        T.BooleanType(),
                        nullable=False,
                    ),
                ]
            ),
        )
        dim_localizacao = self.spark.createDataFrame(
            [
                (1, 1, False),
                (2, 2, False),
            ],
            schema=T.StructType(
                [
                    T.StructField("localizacao_id", T.IntegerType(), nullable=False),
                    T.StructField("location_id", T.IntegerType(), nullable=False),
                    T.StructField(
                        "localizacao_sem_lookup",
                        T.BooleanType(),
                        nullable=False,
                    ),
                ]
            ),
        )
        fact_trips = self.spark.createDataFrame(
            [
                (
                    20250101,
                    2025,
                    1,
                    1,
                    20250101,
                    1,
                    2,
                    datetime(2025, 1, 1, 8, 0, 0),
                ),
                (
                    20250102,
                    2025,
                    1,
                    2,
                    20250102,
                    2,
                    1,
                    datetime(2025, 1, 2, 9, 0, 0),
                ),
            ],
            schema=T.StructType(
                [
                    T.StructField("data_id", T.IntegerType(), nullable=False),
                    T.StructField("ano", T.IntegerType(), nullable=False),
                    T.StructField("mes", T.IntegerType(), nullable=False),
                    T.StructField("dia_mes", T.IntegerType(), nullable=False),
                    T.StructField("clima_id", T.IntegerType(), nullable=False),
                    T.StructField(
                        "localizacao_partida_id",
                        T.IntegerType(),
                        nullable=False,
                    ),
                    T.StructField(
                        "localizacao_chegada_id",
                        T.IntegerType(),
                        nullable=False,
                    ),
                    T.StructField(
                        "data_hora_partida",
                        T.TimestampType(),
                        nullable=False,
                    ),
                ]
            ),
        )

        return GoldStarSchemaDataFrames(
            dim_data=dim_data,
            dim_clima=dim_clima,
            dim_localizacao=dim_localizacao,
            fact_trips=fact_trips,
        )

    def test_valid_star_schema_passes(self) -> None:
        result = validate_gold_star_schema_tables(
            tables=self.make_valid_tables(),
            config=GoldValidationConfig(
                expected_days=2,
                expected_locations=2,
                min_fact_rows=1,
            ),
        )

        self.assertTrue(result.passed)
        self.assertTrue(all(check.passed for check in result.checks))

    def test_orphan_climate_key_fails(self) -> None:
        tables = self.make_valid_tables()
        fact_trips = tables.fact_trips.replace(20250102, 20250103, subset=["clima_id"])
        result = validate_gold_star_schema_tables(
            tables=GoldStarSchemaDataFrames(
                dim_data=tables.dim_data,
                dim_clima=tables.dim_clima,
                dim_localizacao=tables.dim_localizacao,
                fact_trips=fact_trips,
            ),
            config=GoldValidationConfig(
                expected_days=2,
                expected_locations=2,
                min_fact_rows=1,
            ),
        )

        failed_checks = {
            check.name: check for check in result.checks if not check.passed
        }

        self.assertFalse(result.passed)
        self.assertIn("fact_trips_orphan_clima_id", failed_checks)
        self.assertEqual(failed_checks["fact_trips_orphan_clima_id"].actual, 1)


if __name__ == "__main__":
    unittest.main()
