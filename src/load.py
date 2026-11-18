import json
import sqlite3
import csv
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from src.config import DB_PATH, PROCESSED_DATA_DIR


class DataLoader:
    """
    Production-grade Analytical Data Warehouse Loader (SQLite):
    - Idempotent batch tracking via SHA-256 checksums (`etl_run_metadata`)
    - Out-of-order safe upserts (`ON CONFLICT ... WHERE excluded.order_timestamp >= fact_orders.order_timestamp`)
    - Atomic ACID transactions across fact, dimension, and aggregate mart tables
    - Full-warehouse SQL rebuild of `dim_customers`, `agg_category_sales`, and `agg_daily_sales`
      so incremental batches never overwrite historical totals
    """

    def __init__(self, db_path: Path = DB_PATH, processed_dir: Path = PROCESSED_DATA_DIR):
        self.db_path = db_path
        self.processed_dir = processed_dir
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def init_warehouse_schema(self):
        """Initializes Star-Schema tables plus audit & quarantine metadata tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Fact Table: Orders (Source of Truth)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                customer_email TEXT,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                unit_price_usd REAL NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount_usd REAL NOT NULL,
                payment_method TEXT,
                status TEXT NOT NULL,
                order_timestamp TEXT NOT NULL,
                order_date TEXT NOT NULL,
                day_of_week TEXT NOT NULL,
                loaded_at TEXT NOT NULL
            );
            """)

            # Dimension Table: Customers (derived from full fact_orders history)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id TEXT PRIMARY KEY,
                customer_email TEXT,
                total_orders INTEGER NOT NULL,
                completed_orders INTEGER NOT NULL,
                lifetime_spend_usd REAL NOT NULL,
                first_order_at TEXT,
                last_active_at TEXT,
                last_updated TEXT NOT NULL
            );
            """)

            # Aggregate Mart: Category Performance
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_category_sales (
                category TEXT PRIMARY KEY,
                total_revenue_usd REAL NOT NULL,
                total_units_sold INTEGER NOT NULL,
                total_orders INTEGER NOT NULL,
                avg_order_value_usd REAL NOT NULL,
                last_updated TEXT NOT NULL
            );
            """)

            # Aggregate Mart: Daily Trends
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_daily_sales (
                order_date TEXT PRIMARY KEY,
                daily_revenue_usd REAL NOT NULL,
                completed_orders INTEGER NOT NULL,
                refunded_orders INTEGER NOT NULL,
                total_orders INTEGER NOT NULL,
                last_updated TEXT NOT NULL
            );
            """)

            # Audit Table: Pipeline Run Metadata & Idempotency Registry
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_run_metadata (
                run_id TEXT PRIMARY KEY,
                batch_hash TEXT NOT NULL,
                raw_records INTEGER NOT NULL,
                loaded_records INTEGER NOT NULL,
                quarantined_records INTEGER NOT NULL,
                status TEXT NOT NULL,
                executed_at TEXT NOT NULL
            );
            """)

            # Audit Table: Quarantined Records (DLQ mirror in SQL for monitoring)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                rejection_reason TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                quarantined_at TEXT NOT NULL
            );
            """)
            conn.commit()

    def is_batch_already_processed(self, batch_hash: str) -> bool:
        """Checks whether an identical batch checksum has already been loaded."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM etl_run_metadata WHERE batch_hash = ? AND status IN ('SUCCESS', 'COMPLETED_WITH_WARNINGS') LIMIT 1;",
                (batch_hash,)
            )
            return cursor.fetchone() is not None

    def load_transactional_batch(
        self,
        run_id: str,
        batch_hash: str,
        cleaned_records: List[Dict[str, Any]],
        quarantined_records: List[Dict[str, Any]],
        raw_count: int,
        status: str
    ):
        """
        Atomically executes:
        1. Out-of-order-safe upsert into `fact_orders`
        2. Recording rejected rows in `quarantine_records`
        3. Full-warehouse SQL rebuild of `dim_customers`, `agg_category_sales`, `agg_daily_sales`
        4. Logging run provenance in `etl_run_metadata`

        If any step fails, the entire transaction rolls back cleanly.
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(self.db_path)
        try:
            with conn:
                cursor = conn.cursor()

                # 1. Upsert fact_orders (protects against stale/out-of-order events!)
                for r in cleaned_records:
                    cursor.execute("""
                    INSERT INTO fact_orders (
                        order_id, customer_id, customer_email, product_name, category,
                        unit_price_usd, quantity, total_amount_usd,
                        payment_method, status, order_timestamp,
                        order_date, day_of_week, loaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        customer_id = excluded.customer_id,
                        customer_email = excluded.customer_email,
                        product_name = excluded.product_name,
                        category = excluded.category,
                        unit_price_usd = excluded.unit_price_usd,
                        quantity = excluded.quantity,
                        total_amount_usd = excluded.total_amount_usd,
                        payment_method = excluded.payment_method,
                        status = excluded.status,
                        order_timestamp = excluded.order_timestamp,
                        order_date = excluded.order_date,
                        day_of_week = excluded.day_of_week,
                        loaded_at = excluded.loaded_at
                    WHERE excluded.order_timestamp >= fact_orders.order_timestamp;
                    """, (
                        r["order_id"], r["customer_id"], r.get("customer_email", "unknown@domain.local"),
                        r["product_name"], r["category"], r["unit_price_usd"], r["quantity"],
                        r["total_amount_usd"], r["payment_method"], r["status"],
                        r["order_timestamp"], r["order_date"], r["day_of_week"], now_str
                    ))

                # 2. Persist Quarantine / DLQ entries into warehouse audit table
                for q in quarantined_records:
                    cursor.execute("""
                    INSERT INTO quarantine_records (run_id, rejection_reason, raw_payload, quarantined_at)
                    VALUES (?, ?, ?, ?);
                    """, (
                        run_id,
                        q.get("rejection_reason", "unknown"),
                        json.dumps(q.get("raw_record"), default=str),
                        now_str
                    ))

                # 3. Rebuild dim_customers from FULL fact_orders (idempotent across batches)
                cursor.execute("DELETE FROM dim_customers;")
                cursor.execute("""
                INSERT INTO dim_customers (
                    customer_id, customer_email, total_orders, completed_orders,
                    lifetime_spend_usd, first_order_at, last_active_at, last_updated
                )
                SELECT
                    customer_id,
                    MAX(customer_email) AS customer_email,
                    COUNT(*) AS total_orders,
                    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
                    ROUND(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount_usd ELSE 0.0 END), 2) AS lifetime_spend_usd,
                    MIN(order_timestamp) AS first_order_at,
                    MAX(order_timestamp) AS last_active_at,
                    ? AS last_updated
                FROM fact_orders
                GROUP BY customer_id;
                """, (now_str,))

                # 4. Rebuild agg_category_sales from FULL fact_orders
                cursor.execute("DELETE FROM agg_category_sales;")
                cursor.execute("""
                INSERT INTO agg_category_sales (
                    category, total_revenue_usd, total_units_sold,
                    total_orders, avg_order_value_usd, last_updated
                )
                SELECT
                    category,
                    ROUND(SUM(total_amount_usd), 2) AS total_revenue_usd,
                    SUM(quantity) AS total_units_sold,
                    COUNT(*) AS total_orders,
                    ROUND(SUM(total_amount_usd) / COUNT(*), 2) AS avg_order_value_usd,
                    ? AS last_updated
                FROM fact_orders
                WHERE status = 'COMPLETED'
                GROUP BY category;
                """, (now_str,))

                # 5. Rebuild agg_daily_sales from FULL fact_orders
                cursor.execute("DELETE FROM agg_daily_sales;")
                cursor.execute("""
                INSERT INTO agg_daily_sales (
                    order_date, daily_revenue_usd, completed_orders,
                    refunded_orders, total_orders, last_updated
                )
                SELECT
                    order_date,
                    ROUND(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount_usd ELSE 0.0 END), 2) AS daily_revenue_usd,
                    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
                    SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_orders,
                    COUNT(*) AS total_orders,
                    ? AS last_updated
                FROM fact_orders
                GROUP BY order_date
                ORDER BY order_date;
                """, (now_str,))

                # 6. Record pipeline execution audit metadata
                cursor.execute("""
                INSERT INTO etl_run_metadata (
                    run_id, batch_hash, raw_records, loaded_records,
                    quarantined_records, status, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """, (
                    run_id, batch_hash, raw_count, len(cleaned_records),
                    len(quarantined_records), status, now_str
                ))

            print(
                f"[LOAD] Transaction committed | Upserted {len(cleaned_records)} orders | "
                f"Rebuilt dim_customers & analytical marts from full warehouse state."
            )
        finally:
            conn.close()

    def export_warehouse_marts_to_csv(self):
        """Exports cumulative analytical warehouse tables to CSV marts for BI tools."""
        table_exports = {
            "fact_orders": "clean_orders.csv",
            "dim_customers": "dim_customers.csv",
            "agg_category_sales": "category_performance.csv",
            "agg_daily_sales": "daily_sales_trends.csv",
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            for table, filename in table_exports.items():
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()
                if not rows:
                    continue
                output_path = self.processed_dir / filename
                headers = rows[0].keys()
                with open(output_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    for row in rows:
                        writer.writerow([row[h] for h in headers])

        print(f"[EXPORT] Cumulative warehouse marts exported to: {self.processed_dir}")
