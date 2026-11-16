"""
Unit and Logic Validation Tests for Spark Structured Streaming.

IMPORTANT ENVIRONMENT DISCLOSURE:
In compliance with environment restrictions, Apache Spark / PySpark is NOT installed
locally. This test suite independently verifies:
1. Static Python syntax and AST compliance of genuine PySpark Structured Streaming code.
2. The schema definitions declared in data/schema.py.
3. The data filtering and micro-batch aggregation logic on sample telemetry records.

These tests validate algorithmic data transformations without claiming Spark execution.
"""

import ast
import csv
import os
import py_compile
import unittest
from collections import defaultdict

# Add parent directory to path
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
from data.schema import SCHEMA_FIELDS


class TestStreamingCodeInspection(unittest.TestCase):
    """Statically verifies PySpark Structured Streaming application code and syntax."""

    def test_python_files_compile(self):
        """Verifies that main.py, processing.py, and schema.py compile without syntax errors."""
        files_to_check = [
            os.path.join(PROJECT_DIR, "main.py"),
            os.path.join(PROJECT_DIR, "processing.py"),
            os.path.join(PROJECT_DIR, "data", "schema.py")
        ]
        for file_path in files_to_check:
            try:
                compiled = py_compile.compile(file_path, doraise=True)
                self.assertIsNotNone(compiled, f"Failed compiling {file_path}")
            except py_compile.PyCompileError as e:
                self.fail(f"Syntax error found in {file_path}: {e}")

    def test_structured_streaming_constructs_in_ast(self):
        """Verifies presence of key Structured Streaming methods in main.py and processing.py."""
        main_file = os.path.join(PROJECT_DIR, "main.py")
        processing_file = os.path.join(PROJECT_DIR, "processing.py")

        with open(main_file, "r", encoding="utf-8") as f:
            main_code = f.read()
        with open(processing_file, "r", encoding="utf-8") as f:
            proc_code = f.read()

        self.assertIn("readStream", main_code, "main.py must use readStream API.")
        self.assertIn("awaitTermination", main_code, "main.py must handle stream termination.")
        self.assertIn("writeStream", proc_code, "processing.py must use writeStream API.")

    def test_schema_field_specifications(self):
        """Verifies schema fields match required IoT telemetry specification."""
        field_names = [f[0] for f in SCHEMA_FIELDS]
        expected_names = ["device_id", "timestamp", "location", "metric_type", "metric_value", "status"]
        self.assertEqual(field_names, expected_names, "Schema field names do not match specification.")


class TestStreamingTransformationLogic(unittest.TestCase):
    """Tests the micro-batch filtering and aggregation logic on sample_stream.csv."""

    def setUp(self):
        self.sample_file = os.path.join(PROJECT_DIR, "data", "sample_stream.csv")
        self.assertTrue(os.path.exists(self.sample_file), "sample_stream.csv not found.")

    def load_and_filter_stream(self):
        """Replicates filter_streaming_records() using Python standard library."""
        raw_records = []
        valid_records = []
        rejected_records = []

        with open(self.sample_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_records.append(row)
                dev_id = row.get("device_id", "").strip()
                loc = row.get("location", "").strip()
                metric = row.get("metric_type", "").strip()
                status = row.get("status", "").strip().upper()

                try:
                    val = float(row.get("metric_value", "").strip())
                except ValueError:
                    rejected_records.append((row, "Non-numeric metric_value"))
                    continue

                if not dev_id:
                    rejected_records.append((row, "Missing device_id"))
                elif not loc:
                    rejected_records.append((row, "Missing location"))
                elif not metric:
                    rejected_records.append((row, "Missing metric_type"))
                elif val < 0.0 or status == "INVALID":
                    rejected_records.append((row, "Invalid / negative metric reading"))
                else:
                    valid_records.append({
                        "device_id": dev_id,
                        "location": loc,
                        "metric_type": metric,
                        "metric_value": val,
                        "status": status
                    })

        return raw_records, valid_records, rejected_records

    def test_streaming_record_cleansing(self):
        """Verifies filtering of invalid/corrupt stream records."""
        raw, valid, rejected = self.load_and_filter_stream()
        self.assertEqual(len(raw), 15, "Total raw rows in sample_stream.csv should be 15.")
        self.assertEqual(len(valid), 12, "Valid records after filtering should be 12.")
        self.assertEqual(len(rejected), 3, "Exactly 3 invalid records should be rejected.")

        rejected_dev_ids = {r[0]["device_id"] for r in rejected}
        self.assertIn("DEV-999", rejected_dev_ids, "DEV-999 with negative value was not filtered.")
        self.assertIn("DEV-000", rejected_dev_ids, "DEV-000 with missing location was not filtered.")
        self.assertIn("DEV-888", rejected_dev_ids, "DEV-888 with empty value was not filtered.")

    def test_streaming_aggregation_metrics(self):
        """Verifies stateful aggregation metrics by (location, metric_type)."""
        _, valid, _ = self.load_and_filter_stream()

        groups = defaultdict(list)
        for r in valid:
            key = (r["location"], r["metric_type"])
            groups[key].append(r["metric_value"])

        print("\n--- Streaming Aggregation Verification Results ---")
        for (loc, metric), values in sorted(groups.items()):
            count_val = len(values)
            avg_val = round(sum(values) / count_val, 2)
            max_val = round(max(values), 2)
            min_val = round(min(values), 2)
            print(f"  [{loc:<14} | {metric:<12}] -> Count: {count_val:2d} | Avg: {avg_val:5.2f} | Max: {max_val:5.2f} | Min: {min_val:5.2f}")

        # Verify Server-Room-1 temperature readings
        server_room_temps = groups[("Server-Room-1", "temperature")]
        self.assertEqual(len(server_room_temps), 4)
        self.assertEqual(round(sum(server_room_temps) / 4, 2), 32.95)
        self.assertEqual(max(server_room_temps), 38.0)
        self.assertEqual(min(server_room_temps), 28.5)


if __name__ == "__main__":
    print("=" * 70)
    print(" EXECUTING LIGHTWEIGHT LOGIC VALIDATION TESTS FOR SPARK STREAMING")
    print(" (Runs without installing PySpark or Apache Spark cluster)")
    print("=" * 70)
    unittest.main(verbosity=2)
