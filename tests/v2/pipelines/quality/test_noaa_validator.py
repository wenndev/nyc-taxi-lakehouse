from __future__ import annotations

import unittest
from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.quality.config import NOAAQualityConfig, QualityThresholds
from v2.pipelines.quality.models import QualityStatus
from v2.pipelines.quality.validators import validate_noaa_data


NOAA_SCHEMA = T.StructType(
    [
        T.StructField("data_clima", T.DateType(), nullable=True),
        T.StructField("id_estacao", T.StringType(), nullable=True),
        T.StructField("tipo_dado", T.StringType(), nullable=True),
        T.StructField("valor", T.DoubleType(), nullable=True),
        T.StructField("atributos", T.StringType(), nullable=True),
        T.StructField("arquivo_origem", T.StringType(), nullable=True),
        T.StructField("data_processamento_bronze", T.TimestampType(), nullable=True),
    ]
)


class NOAAQualityValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestNOAAQualityValidator")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]) -> DataFrame:
        return self.spark.createDataFrame(rows, schema=NOAA_SCHEMA)

    def test_valid_dataframe_passes(self) -> None:
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    12.4,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "TMAX",
                    7.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-valid")

        self.assertEqual(result.status, QualityStatus.PASS)
        self.assertEqual(result.metrics.total_records, 2)
        self.assertEqual(result.metrics.valid_records, 2)
        self.assertEqual(result.metrics.invalid_records, 0)
        self.assertEqual(result.valid_records.count(), 2)
        self.assertEqual(result.invalid_records.count(), 0)

    def test_empty_dataframe_fails(self) -> None:
        df = self.spark.createDataFrame([], schema=NOAA_SCHEMA)

        result = validate_noaa_data(df, pipeline_run_id="test-empty")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.total_records, 0)
        self.assertEqual(result.metrics.quality_percentage, 0.0)

    def test_missing_required_column_fails_schema_validation(self) -> None:
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    12.4,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ]
        ).drop("valor")

        result = validate_noaa_data(df, pipeline_run_id="test-missing-column")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertIn("valor", result.metrics.missing_columns)
        self.assertEqual(result.metrics.valid_records, 0)
        self.assertEqual(result.metrics.invalid_records, 1)

    def test_incompatible_type_fails_schema_validation(self) -> None:
        schema = T.StructType(
            [
                T.StructField("data_clima", T.DateType(), nullable=True),
                T.StructField("id_estacao", T.StringType(), nullable=True),
                T.StructField("tipo_dado", T.StringType(), nullable=True),
                T.StructField("valor", T.StringType(), nullable=True),
                T.StructField("atributos", T.StringType(), nullable=True),
                T.StructField("arquivo_origem", T.StringType(), nullable=True),
                T.StructField(
                    "data_processamento_bronze",
                    T.TimestampType(),
                    nullable=True,
                ),
            ]
        )
        df = self.spark.createDataFrame(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    "12.4",
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ],
            schema=schema,
        )

        result = validate_noaa_data(df, pipeline_run_id="test-type")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertIn("valor", result.metrics.incompatible_types)

    def test_invalid_records_keep_original_columns_and_audit_columns(self) -> None:
        df = self.make_df(
            [
                (
                    None,
                    None,
                    "PRCP",
                    None,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-audit")
        invalid = result.invalid_records.collect()[0].asDict()

        self.assertIn("data_clima", invalid)
        self.assertIn("id_estacao", invalid)
        self.assertIn("dq_failed_rules", invalid)
        self.assertIn("NULL_REQUIRED_FIELD", invalid["dq_failed_rules"])
        self.assertIn("INVALID_DATE", invalid["dq_failed_rules"])
        self.assertIn("MISSING_STATION_ID", invalid["dq_failed_rules"])
        self.assertEqual(invalid["dq_pipeline_run_id"], "test-audit")
        self.assertEqual(invalid["dq_dataset_name"], "noaa_weather")

    def test_future_date_invalidates_record_and_fails_status(self) -> None:
        df = self.make_df(
            [
                (
                    date(2099, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    1.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-future")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.future_date_count, 1)
        self.assertIn(
            "FUTURE_DATE",
            result.invalid_records.select("dq_failed_rules").collect()[0][0],
        )

    def test_range_error_invalidates_record(self) -> None:
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "TMAX",
                    100.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-range")

        self.assertEqual(result.metrics.range_error_count, 1)
        self.assertIn(
            "CLIMATE_VALUE_OUT_OF_RANGE",
            result.invalid_records.select("dq_failed_rules").collect()[0][0],
        )

    def test_duplicate_composite_key_invalidates_all_duplicate_records(self) -> None:
        duplicate_row = (
            date(2025, 1, 1),
            "GHCND:USW00094728",
            "PRCP",
            1.0,
            ",,N,",
            "page_1.json",
            datetime(2025, 1, 2, 0, 0, 0),
        )
        df = self.make_df([duplicate_row, duplicate_row])

        result = validate_noaa_data(df, pipeline_run_id="test-duplicates")

        self.assertEqual(result.metrics.duplicate_count, 2)
        self.assertEqual(result.valid_records.count(), 0)
        self.assertEqual(result.invalid_records.count(), 2)

    def test_quality_percentage_and_warning_status(self) -> None:
        config = NOAAQualityConfig(
            thresholds=QualityThresholds(
                pass_min_percentage=99.0,
                warning_min_percentage=50.0,
            )
        )
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    1.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
                (
                    None,
                    None,
                    "PRCP",
                    None,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
            ]
        )

        result = validate_noaa_data(
            df,
            config=config,
            pipeline_run_id="test-warning",
        )

        self.assertEqual(result.status, QualityStatus.WARNING)
        self.assertEqual(result.metrics.quality_percentage, 50.0)

    def test_no_records_are_lost_between_valid_and_invalid_outputs(self) -> None:
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    1.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
                (
                    None,
                    None,
                    "PRCP",
                    None,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                ),
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-no-loss")

        self.assertEqual(
            result.metrics.total_records,
            result.valid_records.count() + result.invalid_records.count(),
        )

    def test_invalid_latitude_longitude_rules_are_not_created_without_columns(self) -> None:
        df = self.make_df(
            [
                (
                    date(2025, 1, 1),
                    "GHCND:USW00094728",
                    "PRCP",
                    1.0,
                    ",,N,",
                    "page_1.json",
                    datetime(2025, 1, 2, 0, 0, 0),
                )
            ]
        )

        result = validate_noaa_data(df, pipeline_run_id="test-no-lat-lon")
        invalid_codes = (
            result.invalid_records.select(F.explode("dq_failed_rules").alias("code"))
            .select("code")
            .collect()
        )

        self.assertEqual(invalid_codes, [])


if __name__ == "__main__":
    unittest.main()
