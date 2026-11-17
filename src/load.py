import sqlite3
import csv
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from src.config import DB_PATH, PROCESSED_DATA_DIR

class DataLoader:
    """Loads cleaned data and analytical aggregates into SQLite Warehouse and CSV marts."""

    def __init__(self, db_path: Path = DB_PATH, processed_dir: Path = PROCESSED_DATA_DIR):
        self.db_path = db_path
        self.processed_dir = processed_dir

    def init_warehouse_schema(self):
        """Initializes tables for a star-schema analytical warehouse."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Fact Table: Orders
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
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

            # Dimension Table: Customers
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id TEXT PRIMARY KEY,
                customer_email TEXT,
                total_orders INTEGER DEFAULT 1,
                lifetime_spend_usd REAL DEFAULT 0.0,
                last_active TEXT
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
            conn.commit()

    def load_fact_orders(self, records: List[Dict[str, Any]]):
        """Loads or updates fact_orders table."""
        loaded_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for r in records:
                cursor.execute("""
                INSERT OR REPLACE INTO fact_orders (
                    order_id, customer_id, product_name, category,
                    unit_price_usd, quantity, total_amount_usd,
                    payment_method, status, order_timestamp,
                    order_date, day_of_week, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    r["order_id"], r["customer_id"], r["product_name"], r["category"],
                    r["unit_price_usd"], r["quantity"], r["total_amount_usd"],
                    r["payment_method"], r["status"], r["order_timestamp"],
                    r["order_date"], r["day_of_week"], loaded_at
                ))
            conn.commit()
        print(f"[LOAD] Loaded {len(records)} records into warehouse table: fact_orders")

    def load_aggregates(self, category_agg: List[Dict[str, Any]], daily_agg: List[Dict[str, Any]]):
        """Loads pre-computed aggregation tables."""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for c in category_agg:
                cursor.execute("""
                INSERT OR REPLACE INTO agg_category_sales (
                    category, total_revenue_usd, total_units_sold, total_orders, avg_order_value_usd, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?);
                """, (
                    c["category"], c["total_revenue_usd"], c["total_units_sold"],
                    c["total_orders"], c["avg_order_value_usd"], now_str
                ))

            for d in daily_agg:
                cursor.execute("""
                INSERT OR REPLACE INTO agg_daily_sales (
                    order_date, daily_revenue_usd, completed_orders, refunded_orders, total_orders, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?);
                """, (
                    d["order_date"], d["daily_revenue_usd"], d["completed_orders"],
                    d["refunded_orders"], d["total_orders"], now_str
                ))
            conn.commit()
        print("[LOAD] Analytical aggregate marts refreshed.")

    def export_marts_to_csv(self, cleaned_records: List[Dict[str, Any]], category_agg: List[Dict[str, Any]], daily_agg: List[Dict[str, Any]]):
        """Exports processed data into clean CSV files for downstream BI or data scientists."""
        if cleaned_records:
            orders_path = self.processed_dir / "clean_orders.csv"
            with open(orders_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(cleaned_records[0].keys()))
                writer.writeheader()
                writer.writerows(cleaned_records)

        if category_agg:
            cat_path = self.processed_dir / "category_performance.csv"
            with open(cat_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(category_agg[0].keys()))
                writer.writeheader()
                writer.writerows(category_agg)

        if daily_agg:
            daily_path = self.processed_dir / "daily_sales_trends.csv"
            with open(daily_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(daily_agg[0].keys()))
                writer.writeheader()
                writer.writerows(daily_agg)

        print(f"[EXPORT] CSV analytical files exported to: {self.processed_dir}")
