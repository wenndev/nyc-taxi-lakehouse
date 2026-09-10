# Resumo:
# - Testa a dim_localizacao e o contrato de escrita do Star Schema.
# - Garante enriquecimento, bloqueio de append e reexecucao sem duplicar tabelas.

from __future__ import annotations

import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, call, patch, sentinel

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.gold.gold_star_schema import (
    GoldStarSchemaTables,
    build_dim_localizacao,
    main,
    run_gold_star_schema,
    write_gold_tables,
)
from v2.platform.partitions import YEAR_MONTH_PARTITIONS, YearMonthPartition


GOLD_MODULE = "v2.pipelines.gold.gold_star_schema"


TLC_LOCATION_SCHEMA = T.StructType(
    [
        T.StructField("id_local_partida", T.IntegerType(), nullable=True),
        T.StructField("id_local_chegada", T.IntegerType(), nullable=True),
    ]
)

LOOKUP_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.IntegerType(), nullable=True),
        T.StructField("borough", T.StringType(), nullable=True),
        T.StructField("zona", T.StringType(), nullable=True),
        T.StructField("zona_servico", T.StringType(), nullable=True),
    ]
)


class GoldStarSchemaWriteContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tables = GoldStarSchemaTables(
            dim_data=sentinel.dim_data,
            dim_clima=sentinel.dim_clima,
            dim_localizacao=sentinel.dim_localizacao,
            fact_trips=sentinel.fact_trips,
        )

    def test_writer_rejects_append_before_any_table_write(self) -> None:
        for predicate in (None, "ano = 2025 AND mes = 3"):
            with self.subTest(predicate=predicate), patch(
                f"{GOLD_MODULE}.write_delta_table"
            ) as writer:
                with self.assertRaisesRegex(ValueError, "overwrite"):
                    write_gold_tables(
                        self.tables, "unused-output", "append", predicate
                    )
                writer.assert_not_called()

    def test_pipeline_rejects_append_before_reading_inputs(self) -> None:
        for partition in (None, YearMonthPartition(2025, 3)):
            spark = Mock()
            with self.subTest(partition=partition), patch(
                f"{GOLD_MODULE}.write_delta_table"
            ) as writer:
                with self.assertRaisesRegex(ValueError, "overwrite"):
                    run_gold_star_schema(
                        spark=spark,
                        tlc_input_path="unused-tlc",
                        noaa_input_path="unused-noaa",
                        output_path="unused-output",
                        mode="append",
                        replace_partition=partition,
                    )
                self.assertEqual(spark.mock_calls, [])
                writer.assert_not_called()

    def test_cli_rejects_append_even_in_dry_run_without_starting_spark(self) -> None:
        with (
            patch("sys.argv", ["gold-star-schema", "--mode", "append", "--dry-run"]),
            patch(f"{GOLD_MODULE}.create_spark") as create,
            redirect_stderr(StringIO()),
        ):
            with self.assertRaises(SystemExit) as error:
                main()
            self.assertNotEqual(error.exception.code, 0)
            create.assert_not_called()

    def test_overwrite_preserves_dimension_and_fact_write_options(self) -> None:
        for root in ("/tmp/gold", "s3://example-bucket/gold/"):
            for predicate in (None, "ano = 2025 AND mes = 3"):
                with self.subTest(root=root, predicate=predicate), patch(
                    f"{GOLD_MODULE}.write_delta_table"
                ) as writer:
                    write_gold_tables(self.tables, root, "overwrite", predicate)
                    self.assertEqual(
                        writer.call_args_list,
                        [
                            call(
                                getattr(self.tables, table),
                                f"{root.rstrip('/')}/{table}",
                                mode="overwrite",
                                partition_by=(
                                    YEAR_MONTH_PARTITIONS if table == "fact_trips" else None
                                ),
                                replace_where=(predicate if table == "fact_trips" else None),
                            )
                            for table in (
                                "dim_data", "dim_clima", "dim_localizacao", "fact_trips"
                            )
                        ],
                    )


class GoldStarSchemaLocationDimensionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("TestGoldStarSchemaLocationDimension")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def make_tlc_df(self, rows: list[tuple[int | None, int | None]]) -> DataFrame:
        return self.spark.createDataFrame(rows, schema=TLC_LOCATION_SCHEMA)

    def make_lookup_df(self, rows: list[tuple[int, str, str, str]]) -> DataFrame:
        return self.spark.createDataFrame(rows, schema=LOOKUP_SCHEMA)

    def test_build_dim_localizacao_enriches_with_taxi_zone_lookup(self) -> None:
        df_tlc = self.make_tlc_df([(1, 2), (3, 1)])
        df_lookup = self.make_lookup_df(
            [
                (1, "Manhattan", "Midtown Center", "Yellow Zone"),
                (2, "Queens", "Jamaica Bay", "Boro Zone"),
                (4, "Brooklyn", "Park Slope", "Boro Zone"),
            ]
        )

        rows = {
            row["location_id"]: row.asDict()
            for row in build_dim_localizacao(df_tlc, df_lookup).collect()
        }

        self.assertEqual(set(rows), {1, 2, 3, 4})
        self.assertEqual(rows[1]["localizacao_id"], 1)
        self.assertEqual(rows[1]["borough"], "Manhattan")
        self.assertEqual(rows[1]["zona"], "Midtown Center")
        self.assertEqual(rows[1]["zona_servico"], "Yellow Zone")
        self.assertFalse(rows[1]["localizacao_sem_lookup"])
        self.assertEqual(rows[3]["borough"], "desconhecido")
        self.assertTrue(rows[3]["localizacao_sem_lookup"])

    def test_build_dim_localizacao_without_lookup_keeps_used_ids_as_unknown(self) -> None:
        df_tlc = self.make_tlc_df([(1, 2), (None, 3)])

        rows = {
            row["location_id"]: row.asDict()
            for row in build_dim_localizacao(df_tlc).collect()
        }

        self.assertEqual(set(rows), {1, 2, 3})
        self.assertTrue(all(row["localizacao_sem_lookup"] for row in rows.values()))
        self.assertEqual(rows[1]["borough"], "desconhecido")
        self.assertEqual(rows[1]["zona"], "desconhecida")
        self.assertEqual(rows[1]["zona_servico"], "desconhecida")

    def test_delta_rerun_and_monthly_replacement_preserve_rows_and_unique_keys(self) -> None:
        tables = GoldStarSchemaTables(
            dim_data=self.spark.createDataFrame([(20250101,)], "data_id int"),
            dim_clima=self.spark.createDataFrame([(20250101,)], "clima_id int"),
            dim_localizacao=self.spark.createDataFrame([(1,)], "localizacao_id int"),
            fact_trips=self.spark.createDataFrame(
                [(2025, 1, "janeiro"), (2025, 2, "fevereiro"), (2025, 3, "marco")],
                "ano int, mes int, corrida string",
            ),
        )
        with TemporaryDirectory() as directory:
            output = str(Path(directory) / "star_schema")
            for _ in range(2):
                write_gold_tables(tables, output, "overwrite")
            fact_path = f"{output}/fact_trips"
            self.assertEqual(self.spark.read.format("delta").load(fact_path).count(), 3)

            tables.fact_trips = self.spark.createDataFrame(
                [(2025, 3, "marco_corrigido")], "ano int, mes int, corrida string"
            )
            for _ in range(2):
                write_gold_tables(
                    tables, output, "overwrite", "ano = 2025 AND mes = 3"
                )
            rows = self.spark.read.format("delta").load(fact_path).collect()
            self.assertCountEqual(
                [(row.ano, row.mes, row.corrida) for row in rows],
                [(2025, 1, "janeiro"), (2025, 2, "fevereiro"), (2025, 3, "marco_corrigido")],
            )
            for table, key in (
                ("dim_data", "data_id"),
                ("dim_clima", "clima_id"),
                ("dim_localizacao", "localizacao_id"),
            ):
                df = self.spark.read.format("delta").load(f"{output}/{table}")
                self.assertEqual(df.count(), 1)
                self.assertEqual(df.select(key).distinct().count(), 1)


if __name__ == "__main__":
    unittest.main()
