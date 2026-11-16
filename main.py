"""
Spark Structured Streaming Application on a File-Based Data Source.

Demonstrates continuous real-time stream ingestion and transformation:
1. Continuous file-based stream ingestion with explicit schema.
2. Micro-batch streaming filter and anomaly exclusion.
3. Stateful streaming aggregation by location and metric category.
4. Console streaming sink with fault-tolerant checkpointing.
5. Clean, graceful shutdown and resource cleanup.
"""

import argparse
import os
import sys
import time
from typing import Optional

# Add local directory to path for relative imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.schema import get_sensor_stream_schema
from processing import aggregate_streaming_metrics, create_streaming_sink, filter_streaming_records

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


def create_streaming_session(app_name: str = "SensorTelemetryStreaming") -> SparkSession:
    """Initializes a SparkSession optimized for Structured Streaming."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def run_streaming_job(
    input_directory: str,
    checkpoint_directory: str,
    trigger_seconds: int = 5,
    timeout_seconds: Optional[int] = None
) -> None:
    """
    Executes the PySpark Structured Streaming pipeline.

    Args:
        input_directory: Directory being watched for streaming CSV file arrivals.
        checkpoint_directory: Path where streaming metadata/state is checkpointed.
        trigger_seconds: Micro-batch evaluation interval.
        timeout_seconds: Optional execution timeout (for bounded automated test runs).
    """
    spark = create_streaming_session()

    try:
        print(f"[Spark Streaming] Initializing Structured Streaming Job (Spark {spark.version})...")
        schema = get_sensor_stream_schema()

        abs_input = os.path.abspath(input_directory)
        abs_checkpoint = os.path.abspath(checkpoint_directory)
        os.makedirs(abs_checkpoint, exist_ok=True)

        print(f"[Streaming Source] Monitoring directory: {abs_input}")
        print(f"[Checkpoint Sink] State checkpointing at: {abs_checkpoint}")

        # Configure file-based streaming source
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .option("maxFilesPerTrigger", "1") \
            .schema(schema) \
            .load(abs_input)

        print(f"[Streaming Pipeline] Is streaming: {streaming_df.isStreaming}")

        # Transformation & Cleansing
        cleaned_stream = filter_streaming_records(streaming_df)

        # Stateful Aggregations
        aggregated_stream = aggregate_streaming_metrics(cleaned_stream)

        # Streaming Sink Execution
        print(f"[Streaming Sink] Starting console sink with trigger interval: {trigger_seconds}s...")
        query = create_streaming_sink(
            aggregated_df=aggregated_stream,
            checkpoint_location=abs_checkpoint,
            output_mode="complete",
            trigger_seconds=trigger_seconds
        )

        print("[Streaming Application] Query active. Awaiting micro-batch file arrivals...")
        print("Press Ctrl+C to terminate the stream cleanly.\n")

        if timeout_seconds:
            query.awaitTermination(timeout=timeout_seconds)
            print(f"[Streaming Application] Timeout of {timeout_seconds}s reached.")
        else:
            query.awaitTermination()

    except KeyboardInterrupt:
        print("\n[Streaming Application] Caught interrupt signal (Ctrl+C). Initiating graceful shutdown...")
    finally:
        if 'query' in locals() and query.isActive:
            print("[Streaming Application] Stopping active streaming query...")
            query.stop()
        print("[Streaming Application] Stopping SparkSession.")
        spark.stop()
        print("[Streaming Application] Shutdown complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Application")
    default_input = os.path.join(os.path.dirname(__file__), "data", "stream_input")
    default_ckpt = os.path.join(os.path.dirname(__file__), "checkpoint")

    parser.add_argument("--input-dir", "-i", default=default_input, help=f"Streaming input directory (default: {default_input})")
    parser.add_argument("--checkpoint-dir", "-c", default=default_ckpt, help=f"Checkpoint directory (default: {default_ckpt})")
    parser.add_argument("--trigger", "-t", type=int, default=5, help="Micro-batch trigger interval in seconds (default: 5)")
    parser.add_argument("--timeout", type=int, default=None, help="Optional duration to run stream before stopping (seconds)")

    args = parser.parse_args()

    # Guard check when executed in environment without PySpark installed
    if SparkSession is None:
        print(
            "ERROR: PySpark is not installed in the current environment.\n"
            "This application requires an Apache Spark runtime (Spark 3.x+).\n"
            "To test the streaming data logic and schema verification independently, run:\n"
            "    python test_logic.py\n"
            "To run on an actual Spark environment, execute via spark-submit:\n"
            "    spark-submit main.py --input-dir data/stream_input",
            file=sys.stderr
        )
        sys.exit(1)

    run_streaming_job(
        input_directory=args.input_dir,
        checkpoint_directory=args.checkpoint_dir,
        trigger_seconds=args.trigger,
        timeout_seconds=args.timeout
    )


if __name__ == "__main__":
    main()
