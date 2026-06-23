from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.files.maxPartitionBytes", "32m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.debug.maxToStringFields", "200")
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
