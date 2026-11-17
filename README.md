# 🛒 E-Commerce Sales Data Engineering ETL Pipeline

An end-to-end Python-based Data Engineering ETL (Extract, Transform, Load) pipeline that ingests raw, semi-structured e-commerce transaction data, enforces automated data quality gates, normalizes multi-currency financial records, and loads clean data into an analytical Star-Schema Data Warehouse (SQLite) and downstream CSV data marts.

---

## 🏗️ Architecture & Data Flow

```
[Raw Sources: JSON / Streaming Batches]
                    │
                    ▼
          ┌───────────────────┐
          │    1. EXTRACT     │ ➔ Ingests raw batches (casing variances, currency variations)
          └─────────┬─────────┘
                    ▼
          ┌───────────────────┐
          │   2. TRANSFORM    │ ➔ Deduplication, USD currency conversion, date dimensions
          └─────────┬─────────┘
                    ▼
          ┌───────────────────┐
          │ 3. QUALITY AUDIT  │ ➔ Schema checks, null PKs, non-negative amounts, date sanity
          └─────────┬─────────┘
                    ▼
          ┌───────────────────┐
          │      4. LOAD      │
          └────┬─────────┬────┘
               │         │
               ▼         ▼
     [SQLite Warehouse]   [Analytical CSV Marts]
     - fact_orders        - clean_orders.csv
     - dim_customers      - category_performance.csv
     - agg_daily_sales    - daily_sales_trends.csv
```

---

## 🚀 Key Features

* **Zero-Dependency Core:** Built using pure Python standard libraries (`sqlite3`, `csv`, `json`, `datetime`, `unittest`) so it runs anywhere out-of-the-box.
* **Currency Normalization Engine:** Automatically converts multi-currency orders (USD, EUR, GBP, CAD, PKR) to standard USD baseline for financial reporting.
* **Automated Data Quality Gate:** Validates primary key uniqueness, nullability, logical constraints (e.g., non-negative prices, future timestamps), and raises alerts before loading.
* **Star-Schema Analytical Warehouse:** Automatically designs and updates facts, dimensions, and pre-aggregated analytical marts.
* **Auditing & Structured Logging:** Full execution metrics tracked per run (elapsed time, rows processed, failure metrics) saved to `logs/pipeline.log`.
* **Automated Test Suite:** Built-in unit test suite covering each stage of the ETL pipeline.

---

## 📂 Project Structure

```text
ecommerce-data-pipeline/
├── data/
│   ├── raw/                 # Raw ingested batch files
│   ├── processed/           # Analytical CSV data marts
│   └── warehouse.db         # SQLite analytical data warehouse
├── logs/
│   └── pipeline.log         # Execution audit logs
├── src/
│   ├── __init__.py
│   ├── config.py            # Environment, paths, exchange rates
│   ├── extract.py           # Ingestion and synthetic stream generator
│   ├── transform.py         # Cleaning, normalization, and aggregations
│   ├── quality.py           # Automated Data Quality rules and assertions
│   ├── load.py              # Warehouse schema and data loader
│   └── pipeline.py          # ETL orchestrator
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py     # Unit test suite
├── .gitignore
├── README.md
├── requirements.txt
└── run_pipeline.py          # CLI runner
```

---

## 🛠️ Warehouse Schema Design

### Fact Table: `fact_orders`
* `order_id` (TEXT, PK): Unique order identifier
* `customer_id` (TEXT): Normalized customer ID
* `product_name` (TEXT): Item description
* `category` (TEXT): Product classification
* `unit_price_usd` (REAL): Price converted to USD
* `quantity` (INTEGER): Number of units purchased
* `total_amount_usd` (REAL): Total gross order value in USD
* `payment_method` (TEXT): Checkout method
* `status` (TEXT): Normalized status (`COMPLETED`, `REFUNDED`, `PENDING`)
* `order_timestamp` (TEXT): Original order timestamp
* `order_date` (TEXT): Extracted date (`YYYY-MM-DD`)
* `day_of_week` (TEXT): Day dimension (`Monday`, `Tuesday`, etc.)
* `loaded_at` (TEXT): ETL load audit timestamp

### Aggregate Mart: `agg_category_sales`
* `category` (TEXT, PK)
* `total_revenue_usd` (REAL)
* `total_units_sold` (INTEGER)
* `total_orders` (INTEGER)
* `avg_order_value_usd` (REAL)
* `last_updated` (TEXT)

---

## ⚡ Quickstart & Usage

### 1. Run the Pipeline
Generate and process a batch of 150 transactions:
```bash
python run_pipeline.py --records 150
```

### 2. Run with Strict Quality Gate
Halt pipeline immediately if any data anomaly is flagged:
```bash
python run_pipeline.py --records 100 --strict
```

### 3. Run Unit Tests
```bash
python -m unittest discover tests
```

---

## 📊 Sample Output

```text
=============================================
      PIPELINE EXECUTION SUMMARY
=============================================
 Run Id                   : RUN-20261117223000
 Status                   : SUCCESS
 Raw Records              : 151
 Processed Records        : 150
 Categories Summarized    : 4
 Days Analyzed            : 8
 Duration Seconds         : 0.12
 Warehouse Db             : data/warehouse.db
=============================================
```

---

## 👤 Author
* GitHub: [@yasirzafar201](https://github.com/yasirzafar201)
