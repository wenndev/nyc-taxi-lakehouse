# Resumo:
# - Testa a montagem da dim_localizacao da Gold.
# - Garante enriquecimento via Taxi Zone Lookup e flag para IDs sem lookup.

from __future__ import annotations

import unittest

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from v2.config.spark import create_spark
from v2.pipelines.gold.gold_star_schema import build_dim_localizacao


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


if __name__ == "__main__":
    unittest.main()
