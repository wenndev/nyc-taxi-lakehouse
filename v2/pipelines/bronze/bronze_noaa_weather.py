# Resumo:
# - Lê as páginas JSON brutas da NOAA e persiste os dados na camada Bronze
#   no formato Delta.
# - Mantém metadados do arquivo de origem e a data/hora de processamento.
#
# Conceitos Spark aplicados:
# - DataFrame: os arquivos JSON brutos da NOAA são lidos em um DataFrame Spark.
# - Transformações: withColumn adiciona os metadados de ingestão ao DataFrame.
# - Lazy Evaluation: o Spark adia a execução das transformações até que uma
#   ação exija o resultado.
# - Action: write.save() dispara a execução do plano e persiste os dados
#   da camada Bronze.
# - count(): quando habilitado, dispara uma nova ação para contar os registros.
# - Shuffle: não há shuffle evidente nesta etapa, pois não são utilizadas
#   operações como groupBy, join, distinct ou repartition.
# - Particionamento: não é definido manualmente neste script; o Spark determina
#   as partições com base nos dados de entrada e nas configurações da execução.
# - Delta Lake: o DataFrame da camada Bronze é persistido no formato Delta..

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from v2.config.paths import noaa_bronze_dir, noaa_raw_dir
from v2.config.spark import create_spark
from v2.config.sources import NOAA_CDO_DATA_URL, NOAA_GHCND_NYC_STORAGE_ID
from v2.platform.delta import write_delta_table


def json_page_pattern(input_path: str) -> str:
    return f"{input_path.rstrip('/')}/page_*.json"


def is_local_path(path: str) -> bool:
    return "://" not in path


def read_metric_units(spark: SparkSession, input_path: str) -> str:
    """Confirma a unidade no manifesto RAW sem presumir unidades pelo valor."""
    # Spark JSON ignores '_' files; read the manifest through its Hadoop filesystem.
    context = spark.sparkContext
    jvm = context._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{input_path.rstrip('/')}/_manifest.json")
    filesystem = path.getFileSystem(context._jsc.hadoopConfiguration())
    stream = filesystem.open(path)
    try:
        manifest = json.loads(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()
    if not isinstance(manifest, dict) or not isinstance(manifest.get("params"), list):
        raise ValueError("NOAA Bronze requires a CDO API manifest with units=metric.")
    units = [
        pair[1]
        for pair in manifest["params"]
        if isinstance(pair, list) and len(pair) == 2 and pair[0] == "units"
    ]
    if manifest.get("endpoint") != NOAA_CDO_DATA_URL or units != ["metric"]:
        raise ValueError(
            "NOAA Bronze requires a CDO API manifest with units=metric. "
            "Unknown or standard units cannot be labelled as millimeters/Celsius. "
            "Ingest a verified metric batch into a separate RAW output."
        )
    return units[0]


def build_bronze_noaa_dataframe(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    Cria o DataFrame Bronze da NOAA a partir das páginas JSON brutas.

    Confirma units=metric no manifesto e adiciona unidade, origem e timestamp.
    Não persiste os dados.
    """
    units = read_metric_units(spark, input_path)
    return (
        spark.read.option("multiLine", "true")
        .json(json_page_pattern(input_path))
        .withColumn("arquivo_origem", input_file_name())
        .withColumn("data_processamento_bronze", current_timestamp())
        .withColumn("unidades_noaa", lit(units))
    )


def write_bronze_delta(
    df_bronze: DataFrame,
    output_path: str,
    mode: str,
) -> None:
    """
    Persiste o DataFrame Bronze no formato Delta.
    """
    write_delta_table(df_bronze, output_path, mode=mode)


def run_bronze_noaa_weather(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    mode: str = "overwrite",
) -> DataFrame:
    """
    Executa a etapa Bronze da NOAA.

    Lê os JSONs brutos, adiciona metadados e persiste o resultado em Delta.

    Retorna:
        DataFrame Bronze utilizado na escrita.
    """
    df_bronze = build_bronze_noaa_dataframe(
        spark=spark,
        input_path=input_path,
    )

    write_bronze_delta(
        df_bronze=df_bronze,
        output_path=output_path,
        mode=mode,
    )

    return df_bronze


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create NOAA Weather Bronze Delta table"
    )

    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument(
        "--datasetid",
        default=NOAA_GHCND_NYC_STORAGE_ID,
    )
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-count", action="store_true")

    args = parser.parse_args()

    input_path = (
        args.input
        if args.input
        else str(noaa_raw_dir(args.year, args.datasetid))
    )

    output_path = (
        args.output
        if args.output
        else str(noaa_bronze_dir(args.year, args.datasetid))
    )

    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("Format: NOAA raw JSON -> Delta")
    print(f"Files : {json_page_pattern(input_path)}")

    if args.dry_run:
        return 0

    if is_local_path(input_path) and not Path(input_path).exists():
        print(f"Input path not found: {input_path}")
        return 1

    spark = create_spark("BronzeNOAAWeather")

    try:
        df_bronze = run_bronze_noaa_weather(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            mode=args.mode,
        )

        print("Bronze NOAA Weather saved.")
        df_bronze.printSchema()

        if not args.skip_count:
            print(f"Rows: {df_bronze.count()}")

        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
