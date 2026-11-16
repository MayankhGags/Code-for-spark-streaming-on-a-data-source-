# Spark Structured Streaming on a Data Source

A real-time data engineering application developed using **PySpark Structured Streaming** to continuously ingest, validate, and aggregate IoT sensor telemetry records from a streaming data source.

---

## 1. Project Objective

The objective of this project is to implement an end-to-end continuous stream processing pipeline:
- Ingest streaming data files from an arrival directory using `readStream`.
- Enforce an explicit telemetry schema (`StructType`) required for file-based streaming sources.
- Filter corrupt, missing, or out-of-range sensor readings in streaming micro-batches.
- Perform continuous stateful aggregations across incoming events (Total Events, Average, Max, Min values grouped by Location and Metric Type).
- Output stream updates to a console streaming sink with fault-tolerant checkpointing.
- Handle streaming shutdown cleanly without data loss.

---

## 2. Technologies Used

- **Framework:** Apache Spark 3.x / PySpark Structured Streaming
- **Language:** Python 3.8+
- **Testing & Verification:** `unittest`, `ast`, `py_compile`, `csv` (Python Standard Library)

---

## 3. Environment Notice & Testing Design

> [!NOTE]
> **Environment Compliance Disclosure:**
> In accordance with academic environment instructions prohibiting local Spark / JDK installations:
> 1. `main.py`, `processing.py`, and `data/schema.py` contain genuine, production-grade PySpark Structured Streaming code ready for deployment on a Spark cluster.
> 2. `test_logic.py` provides independent static syntax compilation (AST verification) and unit testing of the streaming transformation logic using Python standard libraries.

---

## 4. File Structure

```
Code-for-spark-streaming-on-a-data-source-/
├── README.md                 # Project documentation and streaming architecture
├── requirements.txt          # Cluster dependencies (pyspark>=3.3.0)
├── main.py                   # Main streaming orchestrator and lifecycle runner
├── processing.py             # Micro-batch filtering, aggregation, and sink configuration
├── test_logic.py             # Independent logic validation and AST syntax verification
├── data/
│   ├── schema.py             # Explicit StructType streaming schema definition
│   ├── sample_stream.csv     # Complete illustrative sensor telemetry dataset
│   └── stream_input/         # Streaming watch directory for incoming batch files
│       ├── batch_01.csv      # First arrival micro-batch
│       └── batch_02.csv      # Second arrival micro-batch
├── checkpoint/               # Fault-tolerant streaming state store (auto-created)
└── .gitignore                # Standard Python and checkpoint gitignore rules
```

---

## 5. Streaming Architecture

```
[ Incoming Micro-Batch CSVs ] (data/stream_input/)
              │
              ▼
    ┌───────────────────┐
    │  SPARK READSTREAM │  Reads streaming files incrementally
    └─────────┬─────────┘  with schema enforcement (maxFilesPerTrigger=1).
              │
              ▼
    ┌───────────────────┐
    │ STREAM CLEANSING  │  Trims fields, filters corrupt/null rows,
    └─────────┬─────────┘  and discards anomalous negative readings.
              │
              ▼
    ┌───────────────────┐
    │ STREAM AGGREGATE  │  groupBy("location", "metric_type").agg(
    └─────────┬─────────┘    count(), avg(), max(), min())
              │
              ▼
    ┌───────────────────┐
    │  CHECKPOINT SINK  │  Fault-tolerant checkpointing to local/HDFS directory
    └─────────┬─────────┘  with console streaming sink (complete mode).
              │
              ▼
[ Live Console Stream Output ]
```

---

## 6. Execution Instructions

### A. Testing Without Installing PySpark (Local Environment):
Run the independent logic and AST validation test suite:
```bash
python test_logic.py
```

### B. Executing with PySpark / Cluster Submission:
When executing on an environment with Apache Spark installed:
```bash
# Run the streaming application monitoring the input directory:
spark-submit main.py --input-dir data/stream_input --checkpoint-dir checkpoint

# Run with a bounded timeout (e.g., 30 seconds for automated test runs):
spark-submit main.py --input-dir data/stream_input --timeout 30
```

---

## 7. Expected Streaming Console Output

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------+-----------+-----------+--------+--------+--------+
|location      |metric_type|TotalEvents|AvgValue|MaxValue|MinValue|
+--------------+-----------+-----------+--------+--------+--------+
|Warehouse-A   |temperature|1          |22.40   |22.40   |22.40   |
|Server-Room-1 |temperature|2          |32.35   |36.20   |28.50   |
|Warehouse-A   |humidity   |1          |55.00   |55.00   |55.00   |
|Factory-Floor |vibration  |1          |1.20    |1.20    |1.20    |
+--------------+-----------+-----------+--------+--------+--------+

-------------------------------------------
Batch: 1 (Stateful update after batch_02 arrives)
-------------------------------------------
+--------------+-----------+-----------+--------+--------+--------+
|location      |metric_type|TotalEvents|AvgValue|MaxValue|MinValue|
+--------------+-----------+-----------+--------+--------+--------+
|Server-Room-1 |temperature|4          |32.95   |38.00   |28.50   |
|Warehouse-A   |humidity   |2          |56.60   |58.20   |55.00   |
|Warehouse-A   |temperature|2          |22.75   |23.10   |22.40   |
|Factory-Floor |vibration  |2          |3.00    |4.80    |1.20    |
|Factory-Floor |temperature|1          |24.80   |24.80   |24.80   |
|Warehouse-A   |vibration  |1          |0.80    |0.80    |0.80    |
+--------------+-----------+-----------+--------+--------+--------+
```
