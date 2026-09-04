# Resumo:
# - Testa regras pequenas da ingestao NOAA sem chamar a API.
# - Garante que download incompleto possa falhar antes de publicar a Bronze.

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from v2.pipelines.ingestion.download_noaa_weather import (
    calculate_retry_wait_seconds,
    is_download_complete,
    read_json,
    write_manifest,
)


class DownloadNOAAWeatherTest(unittest.TestCase):
    def test_download_is_complete_when_counts_match(self) -> None:
        self.assertTrue(
            is_download_complete(
                downloaded_results=75991,
                expected_count=75991,
            )
        )

    def test_download_is_incomplete_when_counts_do_not_match(self) -> None:
        self.assertFalse(
            is_download_complete(
                downloaded_results=1000,
                expected_count=1824,
            )
        )

    def test_download_is_complete_when_api_does_not_return_expected_count(self) -> None:
        self.assertTrue(
            is_download_complete(
                downloaded_results=1000,
                expected_count=None,
            )
        )

    def test_retry_wait_keeps_previous_backoff_when_jitter_is_zero(self) -> None:
        self.assertEqual(
            calculate_retry_wait_seconds(
                attempt=3,
                sleep_seconds=0.25,
                retry_jitter_seconds=0.0,
            ),
            0.75,
        )

    def test_retry_wait_adds_jitter_when_configured(self) -> None:
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.random.uniform",
            return_value=0.4,
        ):
            self.assertAlmostEqual(
                calculate_retry_wait_seconds(
                    attempt=2,
                    sleep_seconds=0.25,
                    retry_jitter_seconds=1.0,
                ),
                0.9,
            )

    def test_manifest_registers_retry_policy(self) -> None:
        with TemporaryDirectory() as temporary_dir:
            output_dir = Path(temporary_dir)
            write_manifest(
                output_dir=output_dir,
                base_params=[("datasetid", "GHCND")],
                limit=1000,
                initial_offset=1,
                sleep_seconds=0.25,
                max_retries=3,
                retry_jitter_seconds=1.0,
                storage_datasetid="GHCND_NYC",
                pages=76,
                downloaded_results=75991,
                expected_count=75991,
                status="success",
                download_complete=True,
            )

            manifest = read_json(output_dir / "_manifest.json")

        self.assertEqual(manifest["sleep_seconds"], 0.25)
        self.assertEqual(manifest["max_retries"], 3)
        self.assertEqual(manifest["retry_jitter_seconds"], 1.0)


if __name__ == "__main__":
    unittest.main()
