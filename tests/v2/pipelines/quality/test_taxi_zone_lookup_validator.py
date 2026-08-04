# Resumo:
# - Testa o Data Quality do Taxi Zone Lookup com DataFrames Spark pequenos.
# - Garante chaves unicas, campos obrigatorios, range e auditoria.

from __future__ import annotations

import unittest

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.quality.config import TaxiZoneLookupQualityConfig
from v2.pipelines.quality.models import QualityStatus
from v2.pipelines.quality.validators import validate_taxi_zone_lookup_data


LOOKUP_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.IntegerType(), nullable=True),
        T.StructField("borough", T.StringType(), nullable=True),
        T.StructField("zona", T.StringType(), nullable=True),
        T.StructField("zona_servico", T.StringType(), nullable=True),
    ]
)


class TaxiZoneLookupQualityValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestTaxiZoneLookupQualityValidator")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]) -> DataFrame:
        return self.spark.createDataFrame(rows, schema=LOOKUP_SCHEMA)

    def test_valid_lookup_passes(self) -> None:
        df = self.make_df(
            [
                (1, "EWR", "Newark Airport", "EWR"),
                (2, "Queens", "Jamaica Bay", "Boro Zone"),
            ]
        )

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-valid")

        self.assertEqual(result.status, QualityStatus.PASS)
        self.assertEqual(result.metrics.total_records, 2)
        self.assertEqual(result.metrics.valid_records, 2)
        self.assertEqual(result.metrics.invalid_records, 0)

    def test_duplicate_location_id_fails(self) -> None:
        df = self.make_df(
            [
                (1, "EWR", "Newark Airport", "EWR"),
                (1, "EWR", "Newark Airport", "EWR"),
            ]
        )

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-duplicate")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.duplicate_count, 2)
        self.assertEqual(result.valid_records.count(), 0)
        self.assertEqual(result.invalid_records.count(), 2)

    def test_required_text_fields_cannot_be_empty(self) -> None:
        df = self.make_df([(2, " ", "Jamaica Bay", "")])

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-empty-text")
        invalid = result.invalid_records.collect()[0].asDict()

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertIn("NULL_REQUIRED_FIELD", invalid["dq_failed_rules"])
        self.assertEqual(invalid["dq_pipeline_run_id"], "lookup-empty-text")
        self.assertEqual(invalid["dq_dataset_name"], "nyc_tlc_taxi_zone_lookup")

    def test_location_id_out_of_range_fails(self) -> None:
        df = self.make_df([(999, "Queens", "Invalid Zone", "Boro Zone")])

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-range")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.range_error_count, 1)
        self.assertIn(
            "INVALID_LOCATION_ID",
            result.invalid_records.select("dq_failed_rules").collect()[0][0],
        )

    def test_missing_required_column_fails_schema_validation(self) -> None:
        df = self.make_df([(1, "EWR", "Newark Airport", "EWR")]).drop("zona")

        result = validate_taxi_zone_lookup_data(
            df,
            pipeline_run_id="lookup-missing-column",
        )

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertIn("zona", result.metrics.missing_columns)
        self.assertEqual(result.metrics.valid_records, 0)
        self.assertEqual(result.metrics.invalid_records, 1)

    def test_no_records_are_lost_between_valid_and_invalid_outputs(self) -> None:
        df = self.make_df(
            [
                (1, "EWR", "Newark Airport", "EWR"),
                (999, "Queens", "Invalid Zone", "Boro Zone"),
            ]
        )

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-no-loss")

        self.assertEqual(
            result.metrics.total_records,
            result.valid_records.count() + result.invalid_records.count(),
        )

    def test_latitude_longitude_rules_are_not_created_without_columns(self) -> None:
        df = self.make_df([(1, "EWR", "Newark Airport", "EWR")])

        result = validate_taxi_zone_lookup_data(df, pipeline_run_id="lookup-no-lat-lon")
        invalid_codes = (
            result.invalid_records.select(F.explode("dq_failed_rules").alias("code"))
            .select("code")
            .collect()
        )

        self.assertEqual(invalid_codes, [])


if __name__ == "__main__":
    unittest.main()
