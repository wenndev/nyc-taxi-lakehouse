from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v2.platform.delta import (
    DeltaWriteMode,
    delta_table_exists,
    is_local_path,
    normalize_partition_columns,
    parse_delta_write_mode,
    write_delta_table,
)


class FakeDataFrame:
    def __init__(self) -> None:
        self.write = FakeWriter()


class FakeWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def format(self, value: str) -> FakeWriter:
        self.calls.append(("format", value))
        return self

    def mode(self, value: str) -> FakeWriter:
        self.calls.append(("mode", value))
        return self

    def option(self, key: str, value: str) -> FakeWriter:
        self.calls.append(("option", (key, value)))
        return self

    def partitionBy(self, *columns: str) -> FakeWriter:
        self.calls.append(("partitionBy", columns))
        return self

    def save(self, path: str) -> None:
        self.calls.append(("save", path))


class DeltaPlatformTest(unittest.TestCase):
    def test_parse_delta_write_mode_accepts_supported_values(self) -> None:
        self.assertEqual(parse_delta_write_mode("overwrite"), DeltaWriteMode.OVERWRITE)
        self.assertEqual(parse_delta_write_mode("append"), DeltaWriteMode.APPEND)
        self.assertEqual(
            parse_delta_write_mode(DeltaWriteMode.APPEND),
            DeltaWriteMode.APPEND,
        )

    def test_parse_delta_write_mode_rejects_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            parse_delta_write_mode("merge")

    def test_normalize_partition_columns(self) -> None:
        self.assertEqual(normalize_partition_columns(None), ())
        self.assertEqual(normalize_partition_columns("ano"), ("ano",))
        self.assertEqual(normalize_partition_columns(["ano", "mes"]), ("ano", "mes"))

    def test_write_delta_table_uses_standard_overwrite_options(self) -> None:
        df = FakeDataFrame()

        write_delta_table(df, "/tmp/table", mode="overwrite")

        self.assertEqual(
            df.write.calls,
            [
                ("format", "delta"),
                ("mode", "overwrite"),
                ("option", ("overwriteSchema", "true")),
                ("save", "/tmp/table"),
            ],
        )

    def test_write_delta_table_supports_append_with_merge_schema(self) -> None:
        df = FakeDataFrame()

        write_delta_table(df, "/tmp/metrics", mode="append", merge_schema=True)

        self.assertEqual(
            df.write.calls,
            [
                ("format", "delta"),
                ("mode", "append"),
                ("option", ("mergeSchema", "true")),
                ("save", "/tmp/metrics"),
            ],
        )

    def test_write_delta_table_supports_partition_and_replace_where(self) -> None:
        df = FakeDataFrame()

        write_delta_table(
            df,
            "/tmp/table",
            mode="overwrite",
            partition_by=["ano", "mes"],
            replace_where="ano = 2025 AND mes = 1",
        )

        self.assertEqual(
            df.write.calls,
            [
                ("format", "delta"),
                ("mode", "overwrite"),
                ("partitionBy", ("ano", "mes")),
                ("option", ("replaceWhere", "ano = 2025 AND mes = 1")),
                ("save", "/tmp/table"),
            ],
        )

    def test_write_delta_table_rejects_replace_where_with_append(self) -> None:
        df = FakeDataFrame()

        with self.assertRaises(ValueError):
            write_delta_table(
                df,
                "/tmp/table",
                mode="append",
                replace_where="ano = 2025",
            )

    def test_delta_table_exists_checks_local_delta_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            table_path = Path(temporary_dir) / "table"
            self.assertFalse(delta_table_exists(str(table_path)))

            (table_path / "_delta_log").mkdir(parents=True)
            self.assertTrue(delta_table_exists(str(table_path)))

    def test_is_local_path(self) -> None:
        self.assertTrue(is_local_path("/tmp/table"))
        self.assertFalse(is_local_path("abfss://container@account/path"))


if __name__ == "__main__":
    unittest.main()
