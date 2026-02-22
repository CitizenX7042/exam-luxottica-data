📌 Overview

This project implements a batch data ingestion pipeline using a Medallion Architecture (Bronze → Silver → Gold) in Databricks.

The pipeline:
Detects new CSV files in a Bronze volume.
Prevents duplicate ingestion using a Delta-based tracking mechanism.
Parses and cleans JSON payload data.
Deduplicates records.
Writes structured results into a Silver table using idempotent MERGE logic.
Tracks processing metadata in a Gold tracking table.

🏗 Architecture
🔹 Bronze Layer

Location: /Volumes/luxottica/bronze/raw_files/
Stores raw CSV files as received.
No transformations are applied.
Serves as immutable ingestion layer.

🔹 Silver Layer
Table: luxottica.silver.orders
Contains cleaned, structured, and deduplicated order data.
Enforces:
JSON parsing
Field normalization
Timestamp standardization
Status normalization
Deduplication by order_id
Uses Delta MERGE to ensure idempotent writes.

🔹 Gold Layer

Table: luxottica.gold.file_ingestion_tracker
Tracks:
file_name
file_path
file_size
batch_tag
status (PROCESSING / SUCCESS / FAILED)
processed_at
record_count
error_message

This layer provides observability and prevents duplicate file ingestion.

🔄 Pipeline Flow
1️⃣ Orchestrator Notebook

Scans Bronze for CSV files.

Uses atomic MERGE into the tracker table to prevent duplicate file entries.

Inserts files with status = PROCESSING.

Triggers the processing notebook per batch_tag.

2️⃣ Processing Notebook

Reads tracker table for files in PROCESSING state.
Loads and parses data.
Cleans and normalizes fields.
Deduplicates by order_id.
Writes to Silver using Delta MERGE.
Updates tracker status:
SUCCESS on completion
FAILED on error

🛡 Duplicate File Handling Strategy

Duplicate ingestion is prevented at two levels:
File-Level Protection
Files are inserted into tracker using an atomic Delta MERGE.
Prevents race conditions under concurrent runs.
Files already marked SUCCESS or PROCESSING are not reprocessed.
Record-Level Protection
Silver writes use MERGE on order_id.
Ensures idempotent behavior.

⚠️ Failure Handling

If processing fails:
Tracker status is updated to FAILED.
Error message is recorded.
Files can be retried safely.

🧠 Design Considerations

Medallion architecture for clear separation of concerns.
Idempotent processing using Delta MERGE.
Concurrency-safe file registration using atomic operations.
Status-based state machine for processing lifecycle.
Defensive data cleaning (null handling, timestamp parsing).
Deduplication using window functions.

▶️ How To Run

Place CSV files in:
/Volumes/luxottica/bronze/raw_files/

Run:
luxottica-orchestrator
The orchestrator will automatically:
Register new files
Trigger processing
Update tracker metadata

📊 Validation

After execution:

Check luxottica.silver.orders
Check luxottica.gold.file_ingestion_tracker

🔮 Possible Improvements

Introduce run_id to fully isolate concurrent executions.
Add checksum validation for stronger duplicate protection.
Add data quality validation framework.

Implement automated recovery for stuck PROCESSING states.

That’s it.
