# 🛒 E-Commerce Sales Data Engineering ETL Pipeline

A production-resilient, end-to-end Python Data Engineering ETL/ELT pipeline that ingests messy semi-structured e-commerce transactions, enforces schema contracts & idempotency, isolates bad records in a Dead-Letter Queue (Quarantine Zone), and atomically loads a Star-Schema Analytical Warehouse (SQLite) and cumulative CSV data marts.

---

## 🏗️ Production Architecture & Data Flow

```
[Raw Sources: JSON / Streaming Batches]
                    │
                    ▼
        ┌───────────────────────┐
        │      1. EXTRACT       │ ➔ Raw ingestion + SHA-256 batch checksum
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐
        │ 2. IDEMPOTENCY GATE   │ ➔ Checks etl_run_metadata (skips duplicate replays)
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐       ┌───────────────────────────────┐
        │   3. TRANSFORM &      │──────▶│   DEAD-LETTER QUEUE (DLQ)     │
        │   SCHEMA CONTRACT     │ (Bad) │ - data/quarantine/dlq_*.json  │
        └───────────┬───────────┘       │ - SQL: quarantine_records     │
                    │ (Clean)           └───────────────────────────────┘
                    ▼
        ┌───────────────────────┐
        │  4. QUALITY AUDIT     │ ➔ PK uniqueness, non-negative rules, statuses
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐
        │ 5. ATOMIC ACID LOAD   │ ➔ Out-of-order-safe SQLite UPSERT + SQL Marts
        └─────┬───────────┬─────┘
              │           │
              ▼           ▼
   [SQLite Warehouse]      [Cumulative CSV Marts]
   - fact_orders           - clean_orders.csv
   - dim_customers         - dim_customers.csv
   - agg_category_sales    - category_performance.csv
   - agg_daily_sales       - daily_sales_trends.csv
   - etl_run_metadata
   - quarantine_records
```

---

## 🛡️ Real-World Data Engineering Problems Solved

This repository explicitly addresses the most common production data pipeline failures. Read the full technical deep-dive in **[docs/REAL_WORLD_CHALLENGES.md](docs/REAL_WORLD_CHALLENGES.md)**:

| Production Problem | Naive Failure Mode | Implemented Engineering Solution |
| :--- | :--- | :--- |
| **Silent Data Loss** | `try/except: continue` drops broken rows silently | **Dead-Letter Queue (DLQ)** (`src/quarantine.py` + `quarantine_records` table) captures rejected payloads with explicit reasons |
| **Schema Drift & Bad Currencies** | Unknown currencies silently multiply by `1.0` USD; extra columns break parsers | **Schema Contract Validator** flags unexpected columns as warnings and quarantines unsupported currencies (`JPY`) or missing required columns |
| **Incremental Overwrites** | Re-computing daily/category totals per batch wipes previous batch metrics | **Full-Warehouse SQL Mart Rebuilds** inside an atomic ACID transaction recalculate `dim_customers`, `agg_category_sales`, and `agg_daily_sales` from full `fact_orders` history |
| **Out-of-Order Events** | Delayed `PENDING` webhook overwrites newer `COMPLETED` status | **Conditional Timestamp Upsert** (`ON CONFLICT ... WHERE excluded.order_timestamp >= fact_orders.order_timestamp`) prevents stale regressions |
| **Duplicate Replays** | Retrying a job re-processes the exact same batch file | **SHA-256 Batch Fingerprinting** tracked in `etl_run_metadata` makes runs safely idempotent |
| **Mixed Date Formats** | Non-standard dates silently replaced with `datetime.now()` | **Multi-Format Parser** handles SQL, ISO-8601 (`Z`), and slash dates; quarantines genuinely corrupted timestamps |

---

## 📂 Project Structure

```text
ecommerce-data-pipeline/
├── data/
│   ├── raw/                 # Raw ingested batch files
│   ├── processed/           # Cumulative analytical CSV data marts
│   ├── quarantine/          # Dead-Letter Queue (DLQ) JSON files for rejected rows
│   └── warehouse.db         # SQLite analytical data warehouse
├── docs/
│   └── REAL_WORLD_CHALLENGES.md  # Deep-dive analysis of real-world DE problems solved
├── logs/
│   └── pipeline.log         # Execution audit logs
├── src/
│   ├── __init__.py
│   ├── config.py            # Paths, exchange rates, schema contract definitions
│   ├── extract.py           # Ingestion, SHA-256 hashing & anomaly-aware generator
│   ├── transform.py         # Schema drift detection, price/date sanitization, dedup
│   ├── quarantine.py        # Dead-Letter Queue (DLQ) persistence layer
│   ├── quality.py           # Automated Data Quality assertions
│   ├── load.py              # ACID warehouse loader & incremental SQL mart rebuilds
│   └── pipeline.py          # Production ETL orchestrator
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py     # Unit tests covering all ETL stages & failure modes
├── .gitignore
├── README.md
├── requirements.txt
└── run_pipeline.py          # CLI runner
```

---

## ⚡ Quickstart & Usage

### 1. Run the Pipeline (with Real-World Anomalies Enabled)
Generates a batch of 150 orders plus injected real-world anomalies (dirty prices, unknown currency, bad timestamp, late duplicate update, schema drift):
```bash
python run_pipeline.py --records 150
```

### 2. Replay an Existing Raw File (Test Idempotency)
Running the exact same raw file a second time automatically skips duplicate processing:
```bash
python run_pipeline.py --input-file data/raw/batch_orders_<timestamp>.json
```
To force reprocessing:
```bash
python run_pipeline.py --input-file data/raw/batch_orders_<timestamp>.json --force
```

### 3. Run with Strict Quality Gate
```bash
python run_pipeline.py --records 100 --strict
```

### 4. Run Unit Test Suite
```bash
python -m unittest discover tests -v
```

---

## 📊 Sample Output

```text
=======================================================
           PIPELINE EXECUTION SUMMARY
=======================================================
 Run Id                   : RUN-20261118092500123
 Status                   : COMPLETED_WITH_WARNINGS
 Batch Hash               : 9f4e2a10c8b1
 Raw Records              : 154
 Processed Records        : 150
 Quarantined Records      : 3
 Duplicates Merged        : 1
 Schema Drift Fields      : ['promo_code_v2']
 Dlq File                 : data/quarantine/dlq_RUN-20261118092500123.json
 Duration Seconds         : 0.084
 Warehouse Db             : data/warehouse.db
=======================================================
```

---

## 👤 Author
* GitHub: [@yasirzafar201](https://github.com/yasirzafar201)
