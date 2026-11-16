"""
Schema Definition Module for Spark Structured Streaming.

Explicit schema definition is mandatory in PySpark Structured Streaming
when reading from file-based streaming sources (such as CSV/JSON directories).
"""

try:
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:
    # PySpark is not installed in the local environment.
    # Fallback types for static inspection.
    DoubleType = None
    StringType = None
    StructField = None
    StructType = None
    TimestampType = None


def get_sensor_stream_schema():
    """
    Returns the explicit StructType schema for IoT sensor telemetry streaming.

    Fields:
    - device_id: Unique hardware identifier for the sensor node.
    - timestamp: ISO-8601 event timestamp (e.g., '2026-01-15 10:00:00').
    - location: Facility or zone where the device is deployed.
    - metric_type: Telemetry measurement type (temperature, humidity, vibration).
    - metric_value: Numeric measurement value.
    - status: Operational status ('NORMAL', 'WARNING', 'CRITICAL').
    """
    if StructType is None:
        return None

    return StructType([
        StructField("device_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("location", StringType(), nullable=False),
        StructField("metric_type", StringType(), nullable=False),
        StructField("metric_value", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ])


# Standard field names for independent verification
SCHEMA_FIELDS = [
    ("device_id", "StringType", False),
    ("timestamp", "StringType", False),
    ("location", "StringType", False),
    ("metric_type", "StringType", False),
    ("metric_value", "DoubleType", True),
    ("status", "StringType", True),
]
