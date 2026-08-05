# Resumo:
# - Testa a validacao pos-Silver do Taxi Zone Lookup.
# - Garante PASS para lookup correto e FAIL para duplicidade/texto invalido.

from __future__ import annotations

import unittest

from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.silver.validate_silver_taxi_zone_lookup import (
    SilverTaxiZoneLookupValidationConfig,
    validate_silver_taxi_zone_lookup_table,
)


class SilverTaxiZoneLookupValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestValidateSilverTaxiZoneLookup")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]):
        schema = T.StructType(
            [
                T.StructField("location_id", T.IntegerType(), nullable=True),
                T.StructField("borough", T.StringType(), nullable=True),
                T.StructField("zona", T.StringType(), nullable=True),
                T.StructField("zona_servico", T.StringType(), nullable=True),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def test_valid_lookup_passes(self) -> None:
        df = self.make_df(
            [
                (1, "EWR", "Newark Airport", "EWR"),
                (2, "Queens", "Jamaica Bay", "Boro Zone"),
            ]
        )

        result = validate_silver_taxi_zone_lookup_table(
            df=df,
            config=SilverTaxiZoneLookupValidationConfig(expected_locations=2),
        )

        self.assertTrue(result.passed)

    def test_duplicate_and_empty_text_fail(self) -> None:
        df = self.make_df(
            [
                (1, "EWR", "Newark Airport", "EWR"),
                (1, "", "Newark Airport", "EWR"),
            ]
        )

        result = validate_silver_taxi_zone_lookup_table(
            df=df,
            config=SilverTaxiZoneLookupValidationConfig(expected_locations=2),
        )
        failed_checks = {
            check.name: check for check in result.checks if not check.passed
        }

        self.assertFalse(result.passed)
        self.assertIn("lookup_duplicate_location_id", failed_checks)
        self.assertIn("lookup_invalid_borough", failed_checks)


if __name__ == "__main__":
    unittest.main()
