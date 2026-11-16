"""
Streaming Processing & Transformation Module.

Defines the continuous transformation pipeline for PySpark Structured Streaming:
1. Cleansing & filtering incoming streaming micro-batches.
2. Continuous stateful/stateless aggregations (grouping by location and metric type).
3. Configuring streaming sinks and checkpointing.
"""

from typing import Optional

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        avg,
        col,
        count,
        max as spark_max,
        min as spark_min,
        round as spark_round,
        trim,
    )
    from pyspark.sql.streaming import StreamingQuery
except ImportError:
    DataFrame = None
    StreamingQuery = None


def filter_streaming_records(stream_df: DataFrame) -> DataFrame:
    """
    Cleanses and validates incoming streaming records:
    1. Trims string fields.
    2. Drops records missing device_id, location, or metric_type.
    3. Filters out corrupt or invalid metric values (null or < 0.0).
    4. Filters out status flagged as 'INVALID'.

    Args:
        stream_df: Raw streaming DataFrame from readStream.

    Returns:
        Filtered streaming DataFrame.
    """
    cleaned = stream_df \
        .withColumn("device_id", trim(col("device_id"))) \
        .withColumn("location", trim(col("location"))) \
        .withColumn("metric_type", trim(col("metric_type"))) \
        .withColumn("status", trim(col("status")))

    valid_stream = cleaned.filter(
        col("device_id").isNotNull() & (col("device_id") != "") &
        col("location").isNotNull() & (col("location") != "") &
        col("metric_type").isNotNull() & (col("metric_type") != "") &
        col("metric_value").isNotNull() & (col("metric_value") >= 0.0) &
        (col("status") != "INVALID")
    )
    return valid_stream


def aggregate_streaming_metrics(valid_stream_df: DataFrame) -> DataFrame:
    """
    Performs streaming aggregations across continuous micro-batches:
    - Groups by deployment location and metric type.
    - Calculates Total Events, Average Value, Max Value, and Min Value.

    Args:
        valid_stream_df: Validated streaming DataFrame.

    Returns:
        Aggregated streaming DataFrame.
    """
    return valid_stream_df.groupBy("location", "metric_type").agg(
        count("device_id").alias("TotalEvents"),
        spark_round(avg("metric_value"), 2).alias("AvgValue"),
        spark_round(spark_max("metric_value"), 2).alias("MaxValue"),
        spark_round(spark_min("metric_value"), 2).alias("MinValue")
    )


def create_streaming_sink(
    aggregated_df: DataFrame,
    checkpoint_location: str,
    output_mode: str = "complete",
    trigger_seconds: int = 5
) -> StreamingQuery:
    """
    Configures and initiates the streaming sink query.

    Args:
        aggregated_df: Aggregated streaming DataFrame.
        checkpoint_location: File path for state recovery checkpoint metadata.
        output_mode: 'complete' (for full aggregate state) or 'update'.
        trigger_seconds: Processing trigger interval in seconds.

    Returns:
        Active StreamingQuery instance.
    """
    query = aggregated_df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("checkpointLocation", checkpoint_location) \
        .option("truncate", "false") \
        .trigger(processingTime=f"{trigger_seconds} seconds") \
        .start()

    return query
