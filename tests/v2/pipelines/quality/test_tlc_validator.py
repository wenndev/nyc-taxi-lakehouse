# Resumo:
# - Testa o Data Quality da TLC com pequenos DataFrames Spark.
# - Garante regras de corrida invalida, duplicatas, ano, metricas e auditoria.

from __future__ import annotations

import unittest
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.quality.config import QualityThresholds, TLCQualityConfig
from v2.pipelines.quality.models import QualityStatus
from v2.pipelines.quality.validators import validate_tlc_data


TLC_SCHEMA = T.StructType(
    [
        T.StructField("id_vendedor", T.IntegerType(), nullable=True),
        T.StructField("data_hora_partida", T.TimestampType(), nullable=True),
        T.StructField("data_hora_chegada", T.TimestampType(), nullable=True),
        T.StructField("qtd_passageiros", T.IntegerType(), nullable=True),
        T.StructField("distancia_milhas", T.DoubleType(), nullable=True),
        T.StructField("id_tarifa", T.IntegerType(), nullable=True),
        T.StructField("flag_armazenado_e_enviado", T.StringType(), nullable=True),
        T.StructField("id_local_partida", T.IntegerType(), nullable=True),
        T.StructField("id_local_chegada", T.IntegerType(), nullable=True),
        T.StructField("tipo_pagamento", T.IntegerType(), nullable=True),
        T.StructField("valor_tarifa", T.DoubleType(), nullable=True),
        T.StructField("taxa_extra", T.DoubleType(), nullable=True),
        T.StructField("taxa_mta_fixa", T.DoubleType(), nullable=True),
        T.StructField("gorjeta", T.DoubleType(), nullable=True),
        T.StructField("valor_pedagios", T.DoubleType(), nullable=True),
        T.StructField("sobretaxa_melhoria", T.DoubleType(), nullable=True),
        T.StructField("valor_total", T.DoubleType(), nullable=True),
        T.StructField("sobretaxa_transito", T.DoubleType(), nullable=True),
        T.StructField("taxa_aeroporto", T.DoubleType(), nullable=True),
        T.StructField("taxa_congestionamento_cbd", T.DoubleType(), nullable=True),
    ]
)


def valid_trip() -> tuple:
    return (
        2,
        datetime(2025, 1, 1, 8, 10, 0),
        datetime(2025, 1, 1, 8, 28, 0),
        1,
        3.2,
        1,
        "N",
        236,
        161,
        1,
        18.4,
        1.0,
        0.5,
        4.0,
        0.0,
        1.0,
        27.4,
        2.5,
        0.0,
        0.75,
    )


class TLCQualityValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestTLCQualityValidator")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_df(self, rows: list[tuple]) -> DataFrame:
        return self.spark.createDataFrame(rows, schema=TLC_SCHEMA)

    def test_valid_dataframe_passes(self) -> None:
        df = self.make_df([valid_trip()])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-valid",
        )

        self.assertEqual(result.status, QualityStatus.PASS)
        self.assertEqual(result.metrics.total_records, 1)
        self.assertEqual(result.metrics.valid_records, 1)
        self.assertEqual(result.metrics.invalid_records, 0)
        self.assertEqual(result.valid_records.count(), 1)
        self.assertEqual(result.invalid_records.count(), 0)

    def test_invalid_records_keep_original_columns_and_audit_columns(self) -> None:
        invalid_trip = list(valid_trip())
        invalid_trip[0] = 0
        invalid_trip[7] = None
        invalid_trip[16] = -5.0
        df = self.make_df([tuple(invalid_trip)])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-invalid",
        )
        invalid = result.invalid_records.collect()[0].asDict()

        self.assertIn("id_vendedor", invalid)
        self.assertIn("data_hora_partida", invalid)
        self.assertIn("dq_failed_rules", invalid)
        self.assertIn("INVALID_VENDOR_ID", invalid["dq_failed_rules"])
        self.assertIn("NULL_REQUIRED_FIELD", invalid["dq_failed_rules"])
        self.assertIn("TRIP_VALUE_OUT_OF_RANGE", invalid["dq_failed_rules"])
        self.assertEqual(invalid["dq_pipeline_run_id"], "tlc-invalid")
        self.assertEqual(invalid["dq_dataset_name"], "nyc_tlc_yellow_trips")

    def test_future_date_invalidates_record(self) -> None:
        future_trip = list(valid_trip())
        future_trip[1] = datetime(2099, 1, 1, 8, 10, 0)
        future_trip[2] = datetime(2099, 1, 1, 8, 28, 0)
        df = self.make_df([tuple(future_trip)])

        result = validate_tlc_data(df, pipeline_run_id="tlc-future")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.future_date_count, 1)
        self.assertIn(
            "FUTURE_DATE",
            result.invalid_records.select("dq_failed_rules").collect()[0][0],
        )

    def test_pickup_out_of_year_invalidates_record(self) -> None:
        out_of_year_trip = list(valid_trip())
        out_of_year_trip[1] = datetime(2024, 12, 30, 12, 0, 0)
        out_of_year_trip[2] = datetime(2024, 12, 30, 12, 15, 0)
        df = self.make_df([tuple(out_of_year_trip)])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-out-of-year",
        )

        self.assertEqual(result.metrics.range_error_count, 1)
        self.assertIn(
            "PICKUP_OUT_OF_YEAR",
            result.invalid_records.select("dq_failed_rules").collect()[0][0],
        )

    def test_duplicate_business_key_invalidates_all_duplicate_records(self) -> None:
        duplicate_trip = valid_trip()
        df = self.make_df([duplicate_trip, duplicate_trip])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-duplicates",
        )

        self.assertEqual(result.metrics.duplicate_count, 2)
        self.assertEqual(result.valid_records.count(), 0)
        self.assertEqual(result.invalid_records.count(), 2)

    def test_quality_percentage_and_warning_status(self) -> None:
        config = TLCQualityConfig(
            thresholds=QualityThresholds(
                pass_min_percentage=99.0,
                warning_min_percentage=50.0,
            )
        )
        invalid_trip = list(valid_trip())
        invalid_trip[16] = -5.0
        df = self.make_df([valid_trip(), tuple(invalid_trip)])

        result = validate_tlc_data(
            df,
            config=config,
            pipeline_run_id="tlc-warning",
        )

        self.assertEqual(result.status, QualityStatus.WARNING)
        self.assertEqual(result.metrics.quality_percentage, 50.0)

    def test_extra_column_does_not_hide_insufficient_quality(self) -> None:
        invalid_trip = list(valid_trip())
        invalid_trip[16] = -5.0
        df = self.make_df([tuple(invalid_trip)]).withColumn("coluna_extra", F.lit("nova"))

        result = validate_tlc_data(df, pipeline_run_id="tlc-extra-column-invalid")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.pipeline_status, QualityStatus.FAIL)
        self.assertEqual(result.metrics.quality_percentage, 0.0)
        self.assertEqual(result.metrics.unexpected_columns, ("coluna_extra",))
        self.assertEqual(result.valid_records.count(), 0)
        self.assertEqual(result.invalid_records.count(), 1)

    def test_extra_column_with_valid_records_remains_warning(self) -> None:
        df = self.make_df([valid_trip()]).withColumn("coluna_extra", F.lit("nova"))

        result = validate_tlc_data(df, pipeline_run_id="tlc-extra-column-valid")

        self.assertEqual(result.status, QualityStatus.WARNING)
        self.assertEqual(result.metrics.pipeline_status, QualityStatus.WARNING)
        self.assertEqual(result.metrics.quality_percentage, 100.0)
        self.assertEqual(result.metrics.unexpected_columns, ("coluna_extra",))
        self.assertEqual(result.valid_records.count(), 1)
        self.assertEqual(result.invalid_records.count(), 0)

    def test_optional_nulls_do_not_invalidate_record(self) -> None:
        trip = list(valid_trip())
        trip[3] = None
        trip[5] = None
        trip[6] = None
        trip[9] = None
        trip[10] = None
        df = self.make_df([tuple(trip)])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-optional-nulls",
        )

        self.assertEqual(result.status, QualityStatus.PASS)
        self.assertEqual(result.valid_records.count(), 1)
        self.assertEqual(result.invalid_records.count(), 0)

    def test_missing_required_column_fails_schema_validation(self) -> None:
        df = self.make_df([valid_trip()]).drop("valor_total")

        result = validate_tlc_data(df, pipeline_run_id="tlc-missing-column")

        self.assertEqual(result.status, QualityStatus.FAIL)
        self.assertIn("valor_total", result.metrics.missing_columns)
        self.assertEqual(result.metrics.valid_records, 0)
        self.assertEqual(result.metrics.invalid_records, 1)

    def test_no_records_are_lost_between_valid_and_invalid_outputs(self) -> None:
        invalid_trip = list(valid_trip())
        invalid_trip[16] = -5.0
        df = self.make_df([valid_trip(), tuple(invalid_trip)])

        result = validate_tlc_data(df, pipeline_run_id="tlc-no-loss")

        self.assertEqual(
            result.metrics.total_records,
            result.valid_records.count() + result.invalid_records.count(),
        )

    def test_latitude_longitude_rules_are_not_created_without_columns(self) -> None:
        df = self.make_df([valid_trip()])

        result = validate_tlc_data(
            df,
            config=TLCQualityConfig.for_year(2025),
            pipeline_run_id="tlc-no-lat-lon",
        )
        invalid_codes = (
            result.invalid_records.select(F.explode("dq_failed_rules").alias("code"))
            .select("code")
            .collect()
        )

        self.assertEqual(invalid_codes, [])


if __name__ == "__main__":
    unittest.main()
