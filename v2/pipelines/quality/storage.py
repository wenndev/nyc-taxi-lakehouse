from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from v2.pipelines.quality.models import DataQualityResult, QualityMetrics


def write_quality_outputs(
    result: DataQualityResult,
    quarantine_path: str,
    metrics_path: str,
    quarantine_mode: str = "overwrite",
    metrics_mode: str = "append",
) -> None:
    result.invalid_records.write.format("delta").mode(quarantine_mode).option(
        "overwriteSchema", "true"
    ).save(quarantine_path)

    metrics_df = metrics_to_dataframe(
        spark=result.valid_records.sparkSession,
        metrics=result.metrics,
    )
    metrics_df.write.format("delta").mode(metrics_mode).option(
        "mergeSchema", "true"
    ).save(metrics_path)


def metrics_to_dataframe(spark: SparkSession, metrics: QualityMetrics):
    schema = T.StructType(
        [
            T.StructField("pipeline_run_id", T.StringType(), nullable=False),
            T.StructField("dataset_name", T.StringType(), nullable=False),
            T.StructField("execution_timestamp", T.TimestampType(), nullable=False),
            T.StructField("total_records", T.LongType(), nullable=False),
            T.StructField("valid_records", T.LongType(), nullable=False),
            T.StructField("invalid_records", T.LongType(), nullable=False),
            T.StructField("quality_percentage", T.DoubleType(), nullable=False),
            T.StructField("duplicate_count", T.LongType(), nullable=False),
            T.StructField("null_error_count", T.LongType(), nullable=False),
            T.StructField("schema_error_count", T.LongType(), nullable=False),
            T.StructField("range_error_count", T.LongType(), nullable=False),
            T.StructField("future_date_count", T.LongType(), nullable=False),
            T.StructField("pipeline_status", T.StringType(), nullable=False),
            T.StructField(
                "missing_columns",
                T.ArrayType(T.StringType(), containsNull=False),
                nullable=False,
            ),
            T.StructField(
                "unexpected_columns",
                T.ArrayType(T.StringType(), containsNull=False),
                nullable=False,
            ),
            T.StructField(
                "incompatible_types",
                T.ArrayType(T.StringType(), containsNull=False),
                nullable=False,
            ),
        ]
    )

    return spark.createDataFrame([metrics.as_row()], schema=schema)

