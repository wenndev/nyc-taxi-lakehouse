# Resumo:
# - Cria a SparkSession local com suporte a Delta Lake.
# - Esse ponto unico facilita rodar o mesmo PySpark localmente e no Databricks.

from __future__ import annotations

from pyspark.sql import SparkSession

from v2.config.settings import SparkSettings, load_settings


def create_spark(
    app_name: str,
    spark_settings: SparkSettings | None = None,
) -> SparkSession:
    settings = spark_settings or load_settings().spark
    builder = SparkSession.builder.appName(app_name)

    if settings.master:
        builder = builder.master(settings.master)

    builder = (
        builder.config("spark.driver.memory", settings.driver_memory)
        .config("spark.driver.maxResultSize", settings.driver_max_result_size)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", settings.max_partition_bytes)
        .config(
            "spark.databricks.delta.snapshotPartitions",
            settings.delta_snapshot_partitions,
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", settings.session_timezone)
        .config(
            "spark.sql.debug.maxToStringFields",
            settings.debug_max_to_string_fields,
        )
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            configure_spark_with_delta_pip(builder)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    except ImportError:
        pass

    return builder.getOrCreate()
