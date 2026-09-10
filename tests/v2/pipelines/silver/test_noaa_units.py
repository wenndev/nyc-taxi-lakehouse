# Resumo:
# - Testa o contrato de unidades entre RAW, Bronze e Silver NOAA.
# - Usa arquivos temporarios e Spark; nao chama NOAA nem AWS.

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F

from v2.config.spark import create_spark
from v2.config.sources import NOAA_CDO_DATA_URL
from v2.pipelines.bronze.bronze_noaa_weather import (
    build_bronze_noaa_dataframe,
    read_metric_units,
    run_bronze_noaa_weather,
)
from v2.pipelines.ingestion.download_noaa_weather import write_json
from v2.pipelines.quality.exceptions import DataQualityCriticalError
from v2.pipelines.silver.silver_noaa_weather import (
    normalize_results,
    run_silver_noaa_weather,
    validate_metric_units,
)


class NOAAUnitsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestNOAAUnits")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        temporary_dir = TemporaryDirectory()
        self.addCleanup(temporary_dir.cleanup)
        self.root = Path(temporary_dir.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()

    def write_manifest(self, params=None, endpoint=NOAA_CDO_DATA_URL) -> None:
        write_json(
            self.raw / "_manifest.json",
            {"endpoint": endpoint, "params": params},
        )

    def write_metric_batch(self) -> None:
        self.write_manifest([["datasetid", "GHCND"], ["units", "metric"]])
        write_json(
            self.raw / "page_000001_offset_000000001.json",
            {
                "metadata": {"resultset": {"count": 5, "limit": 1000, "offset": 1}},
                "results": [
                    {
                        "date": "2025-01-01T00:00:00",
                        "station": "GHCND:USW00094728",
                        "datatype": datatype,
                        "value": value,
                        "attributes": ",,W,",
                    }
                    for datatype, value in (
                        ("PRCP", 12.3), ("TMAX", 20.5), ("TMIN", -2.5),
                        ("SNOW", 25.4), ("SNWD", 50.8),
                    )
                ],
            },
        )

    def test_metric_manifest_is_accepted_including_legacy_format(self) -> None:
        self.write_manifest([["datasetid", "GHCND"], ["units", "metric"]])
        self.assertEqual(read_metric_units(self.spark, str(self.raw)), "metric")

    def test_unknown_standard_or_ambiguous_units_are_rejected(self) -> None:
        for params in (
            None, [], [["units", "standard"]], [["units", "unknown"]],
            [["units", None]], [["units", "metric"], ["units", "standard"]],
            [["units", "metric"], ["units", "metric"]],
        ):
            with self.subTest(params=params):
                self.write_manifest(params)
                with self.assertRaisesRegex(ValueError, "units=metric"):
                    read_metric_units(self.spark, str(self.raw))

    def test_wrong_endpoint_is_rejected(self) -> None:
        self.write_manifest([["units", "metric"]], endpoint="other")
        with self.assertRaisesRegex(ValueError, "CDO API manifest"):
            read_metric_units(self.spark, str(self.raw))

    def test_missing_manifest_prevents_bronze_publication(self) -> None:
        with patch("v2.pipelines.bronze.bronze_noaa_weather.write_bronze_delta") as writer:
            with self.assertRaises(Py4JJavaError):
                run_bronze_noaa_weather(self.spark, str(self.raw), "unused-output")
            writer.assert_not_called()

    def test_standard_manifest_prevents_bronze_publication(self) -> None:
        self.write_manifest([["units", "standard"]])
        with patch("v2.pipelines.bronze.bronze_noaa_weather.write_bronze_delta") as writer:
            with self.assertRaisesRegex(ValueError, "units=metric"):
                run_bronze_noaa_weather(self.spark, str(self.raw), "unused-output")
            writer.assert_not_called()

    def test_bronze_preserves_metric_values_and_stamps_units(self) -> None:
        self.write_metric_batch()
        bronze = build_bronze_noaa_dataframe(self.spark, str(self.raw))
        self.assertEqual(bronze.select("unidades_noaa").first()[0], "metric")
        values = {
            row.tipo_dado: row.valor for row in normalize_results(bronze).collect()
        }
        self.assertEqual(
            values, {"PRCP": 12.3, "TMAX": 20.5, "TMIN": -2.5, "SNOW": 25.4, "SNWD": 50.8}
        )

    def test_silver_rejects_missing_unit_metadata(self) -> None:
        df = self.spark.range(1)
        with self.assertRaisesRegex(DataQualityCriticalError, "Rebuild Bronze"):
            normalize_results(df)

    def test_silver_rejects_non_string_unit_metadata(self) -> None:
        df = self.spark.createDataFrame([(1,)], "unidades_noaa int")
        with self.assertRaisesRegex(DataQualityCriticalError, "must be a string"):
            validate_metric_units(df)

    def test_silver_rejects_null_standard_unknown_and_mixed_units(self) -> None:
        for units in ([None], ["standard"], [""], ["unknown"], ["metric", "standard"]):
            with self.subTest(units=units):
                df = self.spark.createDataFrame([(u,) for u in units], "unidades_noaa string")
                with self.assertRaisesRegex(DataQualityCriticalError, "every Bronze page"):
                    normalize_results(df)

    def test_skip_quality_does_not_bypass_unit_contract(self) -> None:
        df = self.spark.range(1).withColumn("unidades_noaa", F.lit("standard"))
        reader_spark = Mock()
        reader_spark.read.format.return_value.load.return_value = df
        with patch("v2.pipelines.silver.silver_noaa_weather.write_delta_table") as writer:
            with self.assertRaises(DataQualityCriticalError):
                run_silver_noaa_weather(
                    reader_spark, "unused-input", "unused-output", enable_quality=False
                )
            writer.assert_not_called()

    def test_raw_to_delta_silver_keeps_mm_and_celsius_without_rescaling(self) -> None:
        self.write_metric_batch()
        bronze_path = str(self.root / "bronze")
        silver_path = str(self.root / "silver")
        run_bronze_noaa_weather(self.spark, str(self.raw), bronze_path)
        run_silver_noaa_weather(self.spark, bronze_path, silver_path)
        row = self.spark.read.format("delta").load(silver_path).first()
        self.assertEqual(row.precipitacao_mm, 12.3)
        self.assertEqual(row.temp_max_c, 20.5)
        self.assertEqual(row.temp_min_c, -2.5)
        self.assertEqual(row.neve_mm, 25.4)
        self.assertEqual(row.neve_acumulada_mm, 50.8)


if __name__ == "__main__":
    unittest.main()
