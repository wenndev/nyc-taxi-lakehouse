# Resumo:
# - Testa a prioridade entre falhas, limites de qualidade e avisos de schema.
# - Nao precisa iniciar Spark para testar a decisao de status.

import unittest
from dataclasses import replace

from v2.pipelines.quality.config import (
    NOAAQualityConfig,
    QualityThresholds,
    TLCQualityConfig,
    TaxiZoneLookupQualityConfig,
)
from v2.pipelines.quality.models import QualityStatus, SchemaValidationResult
from v2.pipelines.quality.validators import determine_status


class QualityStatusTest(unittest.TestCase):
    def test_thresholds_with_and_without_tolerated_extra_column(self) -> None:
        for config in (
            NOAAQualityConfig(),
            TLCQualityConfig(),
            TaxiZoneLookupQualityConfig(),
        ):
            config = replace(
                config,
                fail_on_unexpected_columns=False,
                thresholds=QualityThresholds(),
            )
            for extra_column in (False, True):
                schema = SchemaValidationResult(
                    unexpected_columns=("coluna_extra",) if extra_column else ()
                )
                for percentage, expected in (
                    (0.0, QualityStatus.FAIL),
                    (94.99, QualityStatus.FAIL),
                    (95.0, QualityStatus.WARNING),
                    (98.99, QualityStatus.WARNING),
                    (99.0, QualityStatus.PASS),
                    (100.0, QualityStatus.PASS),
                ):
                    if extra_column and expected == QualityStatus.PASS:
                        expected = QualityStatus.WARNING
                    with self.subTest(
                        dataset=config.dataset_name,
                        extra_column=extra_column,
                        percentage=percentage,
                    ):
                        self.assertEqual(
                            determine_status(percentage, schema, config, False, 100),
                            expected,
                        )

    def test_custom_thresholds_are_respected_with_extra_column(self) -> None:
        config = TLCQualityConfig(
            thresholds=QualityThresholds(
                pass_min_percentage=90.0, warning_min_percentage=50.0
            )
        )
        schema = SchemaValidationResult(unexpected_columns=("coluna_extra",))
        for percentage, expected in (
            (49.99, QualityStatus.FAIL),
            (50.0, QualityStatus.WARNING),
            (90.0, QualityStatus.WARNING),
        ):
            with self.subTest(percentage=percentage):
                self.assertEqual(
                    determine_status(percentage, schema, config, False, 100),
                    expected,
                )

    def test_critical_errors_and_empty_input_take_precedence(self) -> None:
        config = TLCQualityConfig()
        extra = SchemaValidationResult(unexpected_columns=("coluna_extra",))
        cases = (
            (extra, config, True, 100),
            (extra, config, False, 0),
            (extra, replace(config, fail_on_unexpected_columns=True), False, 100),
            (
                SchemaValidationResult(missing_columns=("valor_total",)),
                config,
                False,
                100,
            ),
            (
                SchemaValidationResult(incompatible_types=("valor_total",)),
                config,
                False,
                100,
            ),
        )
        for schema, config, critical, total in cases:
            with self.subTest(schema=schema, critical=critical, total=total):
                self.assertEqual(
                    determine_status(100.0, schema, config, critical, total),
                    QualityStatus.FAIL,
                )


if __name__ == "__main__":
    unittest.main()
