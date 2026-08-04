# Resumo:
# - Testa regras pequenas da ingestao NOAA sem chamar a API.
# - Garante que download incompleto possa falhar antes de publicar a Bronze.

from __future__ import annotations

import unittest

from v2.pipelines.ingestion.download_noaa_weather import is_download_complete


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


if __name__ == "__main__":
    unittest.main()
