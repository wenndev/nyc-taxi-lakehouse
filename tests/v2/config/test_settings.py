from __future__ import annotations

import unittest
from pathlib import Path

from v2.config.settings import (
    RuntimeEnvironment,
    StorageMode,
    load_settings,
    parse_runtime_environment,
    parse_storage_mode,
)


class SettingsTest(unittest.TestCase):
    def test_default_settings_are_local(self) -> None:
        settings = load_settings({})

        self.assertEqual(settings.environment, RuntimeEnvironment.LOCAL)
        self.assertEqual(settings.storage_mode, StorageMode.LOCAL)
        self.assertEqual(settings.raw_root, settings.v2_root / "data" / "raw")
        self.assertEqual(settings.delta_root, settings.v2_root / "data" / "delta")
        self.assertEqual(settings.spark.master, "local[1]")

    def test_environment_overrides_roots_and_spark(self) -> None:
        settings = load_settings(
            {
                "NYC_TAXI_ENV": "databricks",
                "NYC_TAXI_STORAGE_MODE": "databricks_volume",
                "NYC_TAXI_V2_ROOT": "/repo/nyc-taxi-lakehouse/v2",
                "NYC_TAXI_RAW_ROOT": "/Volumes/catalog/schema/volume/raw",
                "NYC_TAXI_DELTA_ROOT": "/Volumes/catalog/schema/volume/delta",
                "NYC_TAXI_SPARK_SHUFFLE_PARTITIONS": "64",
                "NYC_TAXI_SPARK_MASTER": "",
            }
        )

        self.assertEqual(settings.environment, RuntimeEnvironment.DATABRICKS)
        self.assertEqual(settings.storage_mode, StorageMode.DATABRICKS_VOLUME)
        self.assertEqual(settings.raw_root, Path("/Volumes/catalog/schema/volume/raw"))
        self.assertEqual(settings.delta_root, Path("/Volumes/catalog/schema/volume/delta"))
        self.assertIsNone(settings.spark.master)
        self.assertEqual(settings.spark.shuffle_partitions, "64")

    def test_aws_settings_preserve_s3_roots(self) -> None:
        settings = load_settings(
            {
                "NYC_TAXI_ENV": "aws",
                "NYC_TAXI_STORAGE_MODE": "s3",
                "NYC_TAXI_RAW_ROOT": "s3://example-lakehouse/raw/",
                "NYC_TAXI_DELTA_ROOT": "s3://example-lakehouse/delta/",
            }
        )

        self.assertEqual(settings.environment, RuntimeEnvironment.AWS)
        self.assertEqual(settings.storage_mode, StorageMode.S3)
        self.assertEqual(settings.raw_root, "s3://example-lakehouse/raw")
        self.assertEqual(settings.delta_root, "s3://example-lakehouse/delta")
        self.assertEqual(settings.gold_root, "s3://example-lakehouse/delta/gold")
        self.assertEqual(
            settings.quarantine_root,
            "s3://example-lakehouse/delta/quarantine",
        )
        self.assertEqual(
            settings.monitoring_root,
            "s3://example-lakehouse/delta/monitoring",
        )
        self.assertIsNone(settings.spark.master)

    def test_invalid_environment_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            parse_runtime_environment("invalid")

    def test_invalid_storage_mode_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            parse_storage_mode("invalid")


if __name__ == "__main__":
    unittest.main()
