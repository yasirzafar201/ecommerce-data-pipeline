# 🔍 Real-World Data Engineering Problems & Engineering Solutions

When moving a data pipeline from a local prototype into production, data engineers rarely struggle with writing basic `for` loops—they struggle with **dirty upstream data, silent corruption, partial failures, and non-idempotent reloads**.

Below is an analysis of **5 critical real-world Data Engineering failure modes** audited in this project and the exact production patterns implemented to solve each one.

---

## 1. Silent Data Loss on Malformed Rows (The "Quiet `continue`" Bug)

### The Real-World Problem
In naive ETL scripts, when a row contains a corrupted price (e.g., `"N/A"`, `"$1,250.50"`, or `None`) or negative quantity, developers often wrap parsing in `try/except: continue`. The pipeline finishes with a green checkmark (`EXIT 0`), **but 5% of orders quietly vanished**. Finance discovers weeks later that revenue reports are missing tens of thousands of dollars with zero audit trail.

### How We Solved It (`src/quarantine.py`, `src/transform.py`, `src/load.py`)
* **Dirty Numeric Sanitization:** `DataTransformer.parse_price()` strips real-world formatting (`$`, `€`, `£`, commas, whitespace) before rejecting values.
* **Dead-Letter Queue (DLQ) / Quarantine Zone:** Any record that truly fails validation is routed to `data/quarantine/dlq_<run_id>.json` AND recorded in the warehouse `quarantine_records` SQL table with its `row_index`, `rejection_reason`, and full `raw_payload`.
* **Result:** Zero silent data loss. Engineers can inspect rejected payloads, fix upstream bugs, and replay quarantined batches cleanly.

---

## 2. Silent Financial Corruption from Schema Drift & Unknown Currencies

### The Real-World Problem
Upstream backend teams frequently deploy new features without notifying the Data team:
1. Adding new columns (e.g., `promo_code_v2`) or renaming/dropping required columns (**Schema Drift**).
2. Launching in a new country and sending a new currency code like `"JPY"` or `"INR"`. A naive fallback like `exchange_rates.get(currency, 1.0)` silently treats **15,000 JPY as \$15,000 USD**, destroying executive revenue dashboards!

### How We Solved It (`src/config.py`, `src/transform.py`)
* **Schema Contract Enforcement:** Every incoming record is checked against `REQUIRED_SCHEMA_FIELDS` and `EXPECTED_SCHEMA_FIELDS`.
  * Missing required fields route the row to Quarantine (`missing_required_fields`).
  * Unexpected new columns trigger a `[SCHEMA DRIFT WARNING]` in logs and execution summaries without breaking the pipeline.
* **Strict Currency Lookup:** Removed the dangerous `1.0` default fallback. Unknown currencies are explicitly rejected to the Dead-Letter Queue (`unsupported_currency:JPY`) until exchange rates are updated.

---

## 3. Incremental Overwrites & Corrupted Historical Aggregates

### The Real-World Problem
In a naive batch pipeline, daily or category summary tables (`agg_daily_sales`, `agg_category_sales`) are computed in Python using **only the current batch**, then written using `INSERT OR REPLACE`.
If Batch 1 loads \$10,000 of Electronics on Monday morning and Batch 2 loads \$5,000 of Electronics on Monday afternoon, **Batch 2 overwrites Batch 1**—reporting \$5,000 instead of \$15,000!

### How We Solved It (`src/load.py`)
* **Full-Warehouse ELT SQL Rebuilds:** Rather than aggregating partial batches in Python, `DataLoader.load_transactional_batch()` upserts into the granular `fact_orders` table first, then rebuilds `dim_customers`, `agg_category_sales`, and `agg_daily_sales` directly in SQL across the **entire deduplicated warehouse history**.
* **ACID Atomicity:** All fact upserts, quarantine inserts, dimension rebuilds, and metadata logs execute inside a single SQLite ACID transaction (`with conn:`). If any query fails mid-run, the entire batch rolls back so the warehouse is never left in a half-updated state.

---

## 4. Out-of-Order Events & Non-Idempotent Re-Runs

### The Real-World Problem
Distributed systems (Kafka, SQS, webhooks, retries) guarantee *at-least-once* delivery, not strict ordering:
1. **Duplicate Replays:** An orchestrator retries a failed job and accidentally feeds the exact same raw batch file twice.
2. **Stale Out-of-Order Events:** Order `ORD-1` arrives as `COMPLETED` at `14:00`, and 10 minutes later a delayed webhook delivers an older `PENDING` event from `12:00`. A naive `INSERT OR REPLACE` **regresses** the order back to `PENDING`!

### How We Solved It (`src/extract.py`, `src/transform.py`, `src/load.py`)
* **Batch-Level Idempotency (`SHA-256` Checksums):** Every incoming batch is fingerprinted with SHA-256 (`DataExtractor.compute_batch_hash`) and logged in `etl_run_metadata`. Re-running an identical file automatically skips duplicate work (`SKIPPED_DUPLICATE_BATCH`) unless `--force` is specified.
* **Latest-Timestamp-Wins Deduplication & Conditional SQL Upsert:**
  * Within a batch, duplicate `order_id` records keep the newest `order_timestamp`.
  * Across batches, SQLite uses:
    ```sql
    ON CONFLICT(order_id) DO UPDATE SET ...
    WHERE excluded.order_timestamp >= fact_orders.order_timestamp;
    ```
  * Stale delayed events can never overwrite newer warehouse state.

---

## 5. Mixed Timestamp Formats & Fake Fallback Dates

### The Real-World Problem
Different upstream services emit dates inconsistently (`2026-11-10 14:00:00`, ISO-8601 `2026-11-10T14:00:00Z`, or US format `11/10/2026 14:00:00`). Many beginner pipelines catch parsing errors and default to `datetime.now()`, which **silently shifts historical orders into today's revenue chart**.

### How We Solved It (`src/transform.py`)
* **Multi-Format Parser:** `DataTransformer.parse_timestamp()` supports standard SQL, ISO-8601 UTC (`Z`), and slash formats.
* **No Fabricated Timestamps:** If a timestamp is genuinely corrupted (`"invalid-date-format"`), it is quarantined (`invalid_timestamp`) rather than poisoning today's analytics with `datetime.now()`.

---

## 🧪 Verification

Every solution above is covered by automated unit tests in [`tests/test_pipeline.py`](../tests/test_pipeline.py):
```bash
python -m unittest discover tests -v
```
