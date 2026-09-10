# Resumo:
# - Testa regras pequenas da ingestao NOAA sem chamar a API.
# - Garante que download incompleto possa falhar antes de publicar a Bronze.

from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from v2.pipelines.ingestion.download_noaa_weather import (
    build_base_params,
    calculate_retry_wait_seconds,
    download_pages,
    is_download_complete,
    read_json,
    write_manifest,
    write_json,
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


class DownloadNOAAResumeTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_dir = TemporaryDirectory()
        self.addCleanup(temporary_dir.cleanup)
        self.output = Path(temporary_dir.name)
        self.params = build_base_params(
            datasetid="GHCND",
            datatypes=["PRCP", "TMAX"],
            stationids=["GHCND:USW00094728"],
            locationids=None,
            start_date="2025-01-01",
            end_date="2025-01-31",
            units="metric",
        )
        self.stdout = StringIO()

    def page(self, offset: int = 1, count: int = 3) -> dict:
        size = min(2, max(0, count - offset + 1))
        return {
            "metadata": {"resultset": {"count": count, "limit": 2, "offset": offset}},
            "results": [{"value": index} for index in range(offset, offset + size)],
        }

    def run_download(self, **overrides) -> int:
        kwargs = {
            "token": "test-token-not-for-logs",
            "base_params": self.params,
            "output_dir": self.output,
            "limit": 2,
            "initial_offset": 1,
            "storage_datasetid": "ghcnd",
            "overwrite": False,
            "sleep_seconds": 0,
            "max_retries": 1,
            "retry_jitter_seconds": 0,
        }
        kwargs.update(overrides)
        with redirect_stdout(self.stdout):
            return download_pages(**kwargs)

    def manifest(self) -> dict:
        return read_json(self.output / "_manifest.json")

    def seed_download(self) -> None:
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            side_effect=[self.page(), self.page(offset=3)],
        ):
            self.assertEqual(self.run_download(), 0)

    def test_two_pages_and_identical_query_resume_without_requests(self) -> None:
        self.seed_download()
        self.assertEqual(self.manifest()["downloaded_results"], 3)
        self.assertEqual(self.manifest()["pages"], 2)
        with patch("v2.pipelines.ingestion.download_noaa_weather.request_json") as request:
            self.assertEqual(self.run_download(), 0)
            request.assert_not_called()
        self.assertTrue(self.manifest()["download_complete"])
        self.assertNotIn("test-token-not-for-logs", self.stdout.getvalue())
        self.assertNotIn(
            "test-token-not-for-logs", (self.output / "_manifest.json").read_text()
        )

    def test_changed_query_is_rejected_without_mutating_cache(self) -> None:
        self.seed_download()
        snapshot = {path.name: path.read_bytes() for path in self.output.iterdir()}
        for key, value in (
            ("datasetid", "OTHER"),
            ("startdate", "2025-02-01"),
            ("enddate", "2025-02-28"),
            ("units", "standard"),
            ("datatypeid", "SNOW"),
            ("stationid", "GHCND:OTHER"),
            ("locationid", "CITY:OTHER"),
        ):
            params = [(k, v) for k, v in self.params if k != key] + [(key, value)]
            with self.subTest(key=key), patch(
                "v2.pipelines.ingestion.download_noaa_weather.request_json"
            ) as request:
                with self.assertRaisesRegex(ValueError, "does not match"):
                    self.run_download(base_params=params)
                request.assert_not_called()
                self.assertEqual(
                    snapshot, {p.name: p.read_bytes() for p in self.output.iterdir()}
                )

    def test_changed_pagination_or_storage_is_rejected(self) -> None:
        self.seed_download()
        for overrides in ({"limit": 1}, {"initial_offset": 3}, {"storage_datasetid": "nyc"}):
            with self.subTest(overrides=overrides), patch(
                "v2.pipelines.ingestion.download_noaa_weather.request_json"
            ) as request:
                with self.assertRaisesRegex(ValueError, "does not match"):
                    self.run_download(**overrides)
                request.assert_not_called()

    def test_parameter_order_and_retry_settings_do_not_change_identity(self) -> None:
        self.seed_download()
        with patch("v2.pipelines.ingestion.download_noaa_weather.request_json") as request:
            self.assertEqual(
                self.run_download(
                    base_params=list(reversed(self.params)), max_retries=5,
                    retry_jitter_seconds=0.5,
                ),
                0,
            )
            request.assert_not_called()

    def test_legacy_manifest_with_matching_query_can_resume(self) -> None:
        self.seed_download()
        manifest = self.manifest()
        del manifest["status"]
        del manifest["download_complete"]
        write_json(self.output / "_manifest.json", manifest)
        with patch("v2.pipelines.ingestion.download_noaa_weather.request_json") as request:
            self.assertEqual(self.run_download(), 0)
            request.assert_not_called()

    def test_pages_without_manifest_are_rejected(self) -> None:
        write_json(self.output / "page_000001_offset_000000001.json", self.page())
        with patch("v2.pipelines.ingestion.download_noaa_weather.request_json") as request:
            with self.assertRaisesRegex(ValueError, "no manifest"):
                self.run_download()
            request.assert_not_called()

    def test_invalid_or_wrong_endpoint_manifest_is_rejected(self) -> None:
        self.seed_download()
        original = self.manifest()
        for manifest in ({}, [], {**original, "params": None}, {**original, "endpoint": "other"}):
            with self.subTest(manifest=manifest):
                write_json(self.output / "_manifest.json", manifest)
                with patch("v2.pipelines.ingestion.download_noaa_weather.request_json") as request:
                    with self.assertRaisesRegex(ValueError, "does not match"):
                        self.run_download()
                    request.assert_not_called()

    def test_interrupted_download_has_manifest_and_resumes_missing_page(self) -> None:
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            side_effect=[self.page(), RuntimeError("simulated timeout")],
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated timeout"):
                self.run_download()
        self.assertEqual(self.manifest()["status"], "failed")
        self.assertFalse(self.manifest()["download_complete"])
        self.assertEqual(self.manifest()["pages"], 1)
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            return_value=self.page(offset=3),
        ) as request:
            self.assertEqual(self.run_download(), 0)
            request.assert_called_once()
            self.assertIn("offset=3", request.call_args.kwargs["url"])

    def test_query_is_registered_before_first_http_request(self) -> None:
        def request(**kwargs):
            self.assertEqual(self.manifest()["status"], "in_progress")
            self.assertFalse(self.manifest()["download_complete"])
            raise RuntimeError("first request failed")

        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            side_effect=request,
        ):
            with self.assertRaisesRegex(RuntimeError, "first request failed"):
                self.run_download()
        self.assertEqual(self.manifest()["status"], "failed")

    def test_overwrite_shorter_query_removes_old_pages_only(self) -> None:
        self.seed_download()
        write_json(self.output / "notes.json", {"keep": True})
        changed = [(k, "2025-01-02" if k == "enddate" else v) for k, v in self.params]
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            return_value=self.page(count=1),
        ) as request:
            self.assertEqual(self.run_download(overwrite=True, base_params=changed), 0)
            request.assert_called_once()
        self.assertEqual(len(list(self.output.glob("page_*_offset_*.json"))), 1)
        self.assertTrue((self.output / "notes.json").exists())
        self.assertEqual(self.manifest()["downloaded_results"], 1)

    def test_failed_overwrite_does_not_keep_success_manifest(self) -> None:
        self.seed_download()
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            side_effect=RuntimeError("offline"),
        ):
            with self.assertRaisesRegex(RuntimeError, "offline"):
                self.run_download(overwrite=True)
        self.assertEqual(self.manifest()["status"], "failed")
        self.assertFalse(self.manifest()["download_complete"])
        self.assertEqual(list(self.output.glob("page_*_offset_*.json")), [])

    def test_failed_overwrite_cleanup_cannot_relabel_old_pages(self) -> None:
        self.seed_download()
        unlink = Path.unlink

        def interrupted_unlink(path, **kwargs):
            if path.name.startswith("page_"):
                raise OSError("cleanup interrupted")
            return unlink(path, **kwargs)

        with patch.object(Path, "unlink", autospec=True, side_effect=interrupted_unlink):
            with self.assertRaisesRegex(OSError, "cleanup interrupted"):
                self.run_download(overwrite=True)
        self.assertFalse((self.output / "_manifest.json").exists())
        with self.assertRaisesRegex(ValueError, "no manifest"):
            self.run_download()

    def test_extra_cached_page_prevents_success(self) -> None:
        self.seed_download()
        write_json(self.output / "page_000003_offset_000000005.json", self.page(offset=5))
        with self.assertRaisesRegex(ValueError, "outside this download"):
            self.run_download()
        self.assertFalse(self.manifest()["download_complete"])

    def test_cached_page_with_wrong_offset_is_rejected(self) -> None:
        self.seed_download()
        write_json(self.output / "page_000001_offset_000000001.json", self.page(offset=3))
        with self.assertRaisesRegex(ValueError, "Invalid NOAA page"):
            self.run_download()
        self.assertEqual(self.manifest()["status"], "failed")

    def test_result_count_change_between_pages_is_rejected(self) -> None:
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            side_effect=[self.page(), self.page(offset=3, count=4)],
        ):
            with self.assertRaisesRegex(ValueError, "count changed"):
                self.run_download()
        self.assertEqual(self.manifest()["status"], "failed")
        self.assertFalse((self.output / "page_000002_offset_000000003.json").exists())

    def test_malformed_response_is_not_saved_as_complete(self) -> None:
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            return_value={},
        ):
            with self.assertRaisesRegex(ValueError, "Invalid NOAA page"):
                self.run_download()
        self.assertFalse(self.manifest()["download_complete"])
        self.assertEqual(list(self.output.glob("page_*_offset_*.json")), [])

    def test_short_page_marks_download_incomplete(self) -> None:
        payload = self.page()
        payload["results"] = payload["results"][:1]
        with patch(
            "v2.pipelines.ingestion.download_noaa_weather.request_json",
            return_value=payload,
        ):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(self.manifest()["status"], "incomplete")
        self.assertFalse(self.manifest()["download_complete"])


if __name__ == "__main__":
    unittest.main()
