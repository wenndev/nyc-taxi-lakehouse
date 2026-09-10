from __future__ import annotations

import importlib
import os
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import v2.config.paths as paths_module


class PathsTest(unittest.TestCase):
    def tearDown(self) -> None:
        importlib.reload(paths_module)

    def reload_paths(self, env: dict[str, str]) -> ModuleType:
        with patch.dict(os.environ, env, clear=True):
            return importlib.reload(paths_module)

    def test_local_paths_remain_path_objects(self) -> None:
        module = self.reload_paths({})

        self.assertIsInstance(module.RAW_ROOT, Path)
        self.assertIsInstance(module.DELTA_ROOT, Path)
        self.assertEqual(
            module.nyc_tlc_raw_dir(2025),
            module.RAW_ROOT / "nyc_tlc" / "yellow" / "2025",
        )
        self.assertEqual(
            module.star_schema_gold_dir(2025),
            module.GOLD_ROOT / "star_schema" / "2025",
        )

    def test_s3_paths_preserve_uri_scheme(self) -> None:
        module = self.reload_paths(
            {
                "NYC_TAXI_ENV": "aws",
                "NYC_TAXI_STORAGE_MODE": "s3",
                "NYC_TAXI_RAW_ROOT": "s3://example-lakehouse/raw/",
                "NYC_TAXI_DELTA_ROOT": "s3://example-lakehouse/delta/",
            }
        )

        self.assertEqual(module.RAW_ROOT, "s3://example-lakehouse/raw")
        self.assertEqual(module.DELTA_ROOT, "s3://example-lakehouse/delta")
        self.assertEqual(
            module.nyc_tlc_raw_dir(2025),
            "s3://example-lakehouse/raw/nyc_tlc/yellow/2025",
        )
        self.assertEqual(
            module.taxi_zone_lookup_raw_dir(),
            "s3://example-lakehouse/raw/nyc_tlc/taxi_zone_lookup",
        )
        self.assertEqual(
            module.noaa_silver_dir(2025),
            "s3://example-lakehouse/delta/silver/noaa/ghcnd_nyc/2025",
        )
        self.assertEqual(
            module.star_schema_gold_dir(2025),
            "s3://example-lakehouse/delta/gold/star_schema/2025",
        )
        self.assertEqual(
            module.nyc_tlc_quarantine_dir(2025),
            "s3://example-lakehouse/delta/quarantine/nyc_tlc/yellow/2025",
        )
        self.assertEqual(
            module.nyc_tlc_quality_metrics_dir(2025),
            "s3://example-lakehouse/delta/monitoring/quality/nyc_tlc/yellow/2025",
        )


if __name__ == "__main__":
    unittest.main()
