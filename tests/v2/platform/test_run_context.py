from __future__ import annotations

import unittest

from v2.config.settings import RuntimeEnvironment
from v2.platform.run_context import RunContext, discover_pipeline_run_id


class RunContextTest(unittest.TestCase):
    def test_create_uses_explicit_pipeline_run_id(self) -> None:
        context = RunContext.create(
            pipeline_name="silver-nyc-tlc",
            year=2025,
            start_date="2025-01-01",
            end_date="2025-02-01",
            batch_id="2025_01",
            pipeline_run_id="manual-run",
            env={"NYC_TAXI_ENV": "local"},
        )

        self.assertEqual(context.pipeline_run_id, "manual-run")
        self.assertEqual(context.environment, RuntimeEnvironment.LOCAL)
        self.assertEqual(context.year, 2025)
        self.assertEqual(context.batch_id, "2025_01")

    def test_create_discovers_orchestrator_pipeline_run_id(self) -> None:
        context = RunContext.create(
            pipeline_name="silver-noaa-weather",
            env={
                "NYC_TAXI_ENV": "azure",
                "ADF_PIPELINE_RUN_ID": "adf-run-123",
            },
        )

        self.assertEqual(context.pipeline_run_id, "adf-run-123")
        self.assertEqual(context.environment, RuntimeEnvironment.AZURE)

    def test_create_generates_pipeline_run_id_when_missing(self) -> None:
        context = RunContext.create(
            pipeline_name="gold-star-schema",
            env={"NYC_TAXI_ENV": "local"},
        )

        self.assertTrue(context.pipeline_run_id.startswith("gold-star-schema-"))

    def test_as_log_context_is_serializable_shape(self) -> None:
        context = RunContext.create(
            pipeline_name="run-v2-dev-sample",
            pipeline_run_id="dev-run",
            env={"NYC_TAXI_ENV": "local"},
        )

        log_context = context.as_log_context()

        self.assertEqual(log_context["pipeline_name"], "run-v2-dev-sample")
        self.assertEqual(log_context["pipeline_run_id"], "dev-run")
        self.assertEqual(log_context["environment"], "local")
        self.assertIn("started_at", log_context)

    def test_discover_pipeline_run_id_priority(self) -> None:
        pipeline_run_id = discover_pipeline_run_id(
            {
                "PIPELINE_RUN_ID": "manual",
                "ADF_PIPELINE_RUN_ID": "adf",
            }
        )

        self.assertEqual(pipeline_run_id, "manual")


if __name__ == "__main__":
    unittest.main()
