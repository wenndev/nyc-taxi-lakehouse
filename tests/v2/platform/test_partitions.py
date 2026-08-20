from __future__ import annotations

import unittest

from v2.platform.partitions import (
    YearMonthPartition,
    resolve_year_month_partition,
    validate_replace_partition_write_mode,
)


class PartitionsPlatformTest(unittest.TestCase):
    def test_resolve_year_month_partition_returns_none_without_replace_args(self) -> None:
        self.assertIsNone(resolve_year_month_partition(base_year=2025))

    def test_resolve_year_month_partition_uses_base_year_when_year_is_omitted(self) -> None:
        partition = resolve_year_month_partition(base_year=2025, replace_month=1)

        self.assertEqual(partition, YearMonthPartition(year=2025, month=1))

    def test_resolve_year_month_partition_accepts_explicit_year(self) -> None:
        partition = resolve_year_month_partition(
            base_year=2025,
            replace_year=2026,
            replace_month=2,
        )

        self.assertEqual(partition, YearMonthPartition(year=2026, month=2))

    def test_resolve_year_month_partition_requires_month_when_year_is_informed(self) -> None:
        with self.assertRaises(ValueError):
            resolve_year_month_partition(base_year=2025, replace_year=2025)

    def test_resolve_year_month_partition_rejects_invalid_month(self) -> None:
        with self.assertRaises(ValueError):
            resolve_year_month_partition(base_year=2025, replace_month=13)

    def test_year_month_partition_rejects_invalid_direct_month(self) -> None:
        with self.assertRaises(ValueError):
            YearMonthPartition(year=2025, month=13)

    def test_year_month_partition_builds_replace_where(self) -> None:
        partition = YearMonthPartition(year=2025, month=1)

        self.assertEqual(partition.replace_where, "ano = 2025 AND mes = 1")

    def test_year_month_partition_builds_regular_month_date_range(self) -> None:
        partition = YearMonthPartition(year=2025, month=1)

        self.assertEqual(partition.date_range(), ("2025-01-01", "2025-02-01"))

    def test_year_month_partition_builds_december_date_range(self) -> None:
        partition = YearMonthPartition(year=2025, month=12)

        self.assertEqual(partition.date_range(), ("2025-12-01", "2026-01-01"))

    def test_validate_replace_partition_write_mode_accepts_overwrite(self) -> None:
        validate_replace_partition_write_mode(
            mode="overwrite",
            partition=YearMonthPartition(year=2025, month=1),
        )

    def test_validate_replace_partition_write_mode_rejects_append(self) -> None:
        with self.assertRaises(ValueError):
            validate_replace_partition_write_mode(
                mode="append",
                partition=YearMonthPartition(year=2025, month=1),
            )


if __name__ == "__main__":
    unittest.main()
