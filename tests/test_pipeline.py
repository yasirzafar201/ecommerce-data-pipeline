import unittest
import sqlite3
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.quality import DataQualityValidator
from src.load import DataLoader


class TestEcommerceETLPipeline(unittest.TestCase):

    def setUp(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.validator = DataQualityValidator()
        self.test_db = PROJECT_ROOT / "data" / "test_warehouse.db"
        self.loader = DataLoader(db_path=self.test_db)

    def tearDown(self):
        if self.test_db.exists():
            self.test_db.unlink()

    def test_extract_generates_valid_keys_and_hash(self):
        records, batch_hash = self.extractor.generate_synthetic_batch(
            count=5, save_to_disk=False, inject_anomalies=False
        )
        self.assertEqual(len(records), 5)
        self.assertEqual(len(batch_hash), 64)  # SHA-256 hex digest length
        sample = records[0]
        required_keys = [
            "order_id", "customer_id", "product_name",
            "unit_price", "currency", "order_timestamp"
        ]
        for key in required_keys:
            self.assertIn(key, sample)

    def test_dirty_price_sanitization_and_mixed_timestamps(self):
        raw_data = [
            {
                "order_id": "ORD-100",
                "customer_id": "  CUST_100  ",
                "product_name": "Laptop",
                "category": "Electronics",
                "unit_price": "$1,250.50",
                "quantity": 1,
                "currency": "USD",
                "status": "completed",
                "order_timestamp": "2026-11-10T14:30:00Z"
            },
            {
                "order_id": "ORD-101",
                "customer_id": "cust_101",
                "product_name": "Headphones",
                "category": "Electronics",
                "unit_price": "100.00",
                "quantity": 2,
                "currency": "EUR",
                "status": "COMPLETED",
                "order_timestamp": "11/10/2026 16:00:00"
            }
        ]

        cleaned, quarantined, stats = self.transformer.clean_and_normalize(raw_data)
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(len(quarantined), 0)

        by_id = {r["order_id"]: r for r in cleaned}
        self.assertEqual(by_id["ORD-100"]["customer_id"], "cust_100")
        self.assertEqual(by_id["ORD-100"]["unit_price_usd"], 1250.50)
        self.assertEqual(by_id["ORD-100"]["order_date"], "2026-11-10")

        # 100 EUR * 1.08 = 108 USD * 2 = 216 USD
        self.assertEqual(by_id["ORD-101"]["total_amount_usd"], 216.0)
        self.assertEqual(by_id["ORD-101"]["order_timestamp"], "2026-11-10 16:00:00")

    def test_quarantine_routes_bad_records_without_silent_loss(self):
        raw_bad_data = [
            {
                # Bad price ("N/A")
                "order_id": "ORD-BAD-1",
                "customer_id": "cust_1",
                "product_name": "Laptop",
                "category": "Electronics",
                "unit_price": "N/A",
                "quantity": 1,
                "currency": "USD",
                "status": "COMPLETED",
                "order_timestamp": "2026-11-10 10:00:00"
            },
            {
                # Unsupported currency ("JPY" must NOT silently default to 1.0 USD)
                "order_id": "ORD-BAD-2",
                "customer_id": "cust_2",
                "product_name": "Keyboard",
                "category": "Electronics",
                "unit_price": 15000,
                "quantity": 1,
                "currency": "JPY",
                "status": "COMPLETED",
                "order_timestamp": "2026-11-10 10:00:00"
            },
            {
                # Unparseable timestamp (must NOT fake datetime.now())
                "order_id": "ORD-BAD-3",
                "customer_id": "cust_3",
                "product_name": "Mouse",
                "category": "Electronics",
                "unit_price": 45.0,
                "quantity": 1,
                "currency": "USD",
                "status": "COMPLETED",
                "order_timestamp": "not-a-timestamp"
            },
            {
                # Missing required schema column (`unit_price`)
                "order_id": "ORD-BAD-4",
                "customer_id": "cust_4"
            }
        ]

        cleaned, quarantined, stats = self.transformer.clean_and_normalize(raw_bad_data)
        self.assertEqual(len(cleaned), 0)
        self.assertEqual(len(quarantined), 4)
        reasons = [q["rejection_reason"] for q in quarantined]
        self.assertTrue(any("invalid_unit_price" in r for r in reasons))
        self.assertTrue(any("unsupported_currency:JPY" in r for r in reasons))
        self.assertTrue(any("invalid_timestamp" in r for r in reasons))
        self.assertTrue(any("missing_required_fields" in r for r in reasons))

    def test_schema_drift_detection_and_latest_wins_deduplication(self):
        raw_data = [
            {
                "order_id": "ORD-DUP-1",
                "customer_id": "cust_50",
                "product_name": "Air Fryer",
                "category": "Home & Kitchen",
                "unit_price": 120.0,
                "quantity": 1,
                "currency": "USD",
                "status": "PENDING",
                "order_timestamp": "2026-11-10 10:00:00",
                "promo_code_v2": "SAVE20"  # Unexpected upstream schema drift field
            },
            {
                # Newer update event for the same order
                "order_id": "ORD-DUP-1",
                "customer_id": "cust_50",
                "product_name": "Air Fryer",
                "category": "Home & Kitchen",
                "unit_price": 120.0,
                "quantity": 1,
                "currency": "USD",
                "status": "COMPLETED",
                "order_timestamp": "2026-11-10 12:30:00"
            }
        ]

        cleaned, quarantined, stats = self.transformer.clean_and_normalize(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["status"], "COMPLETED")
        self.assertEqual(stats["duplicates_merged"], 1)
        self.assertEqual(stats["schema_drift_unexpected_fields"], ["promo_code_v2"])

    def test_out_of_order_upsert_and_incremental_sql_marts(self):
        self.loader.init_warehouse_schema()

        # Batch 1: ORD-1 COMPLETED at 14:00 ($100)
        batch_1 = [{
            "order_id": "ORD-1",
            "customer_id": "cust_1",
            "customer_email": "alice@example.com",
            "product_name": "Keyboard",
            "category": "Electronics",
            "unit_price_usd": 100.0,
            "quantity": 1,
            "total_amount_usd": 100.0,
            "payment_method": "paypal",
            "status": "COMPLETED",
            "order_timestamp": "2026-11-12 14:00:00",
            "order_date": "2026-11-12",
            "day_of_week": "Thursday"
        }]
        self.loader.load_transactional_batch(
            run_id="RUN-1", batch_hash="hash_batch_1",
            cleaned_records=batch_1, quarantined_records=[],
            raw_count=1, status="SUCCESS"
        )

        # Idempotency check: hash_batch_1 should now be registered
        self.assertTrue(self.loader.is_batch_already_processed("hash_batch_1"))
        self.assertFalse(self.loader.is_batch_already_processed("hash_batch_2"))

        # Batch 2:
        # - Stale out-of-order event for ORD-1 (PENDING at 12:00 -> must NOT overwrite 14:00 COMPLETED!)
        # - New order ORD-2 for same customer & category ($200) -> SQL marts must accumulate to $300!
        batch_2 = [
            {
                "order_id": "ORD-1",
                "customer_id": "cust_1",
                "customer_email": "alice@example.com",
                "product_name": "Keyboard",
                "category": "Electronics",
                "unit_price_usd": 100.0,
                "quantity": 1,
                "total_amount_usd": 100.0,
                "payment_method": "paypal",
                "status": "PENDING",
                "order_timestamp": "2026-11-12 12:00:00",
                "order_date": "2026-11-12",
                "day_of_week": "Thursday"
            },
            {
                "order_id": "ORD-2",
                "customer_id": "cust_1",
                "customer_email": "alice@example.com",
                "product_name": "Monitor",
                "category": "Electronics",
                "unit_price_usd": 200.0,
                "quantity": 1,
                "total_amount_usd": 200.0,
                "payment_method": "credit_card",
                "status": "COMPLETED",
                "order_timestamp": "2026-11-12 15:00:00",
                "order_date": "2026-11-12",
                "day_of_week": "Thursday"
            }
        ]
        self.loader.load_transactional_batch(
            run_id="RUN-2", batch_hash="hash_batch_2",
            cleaned_records=batch_2, quarantined_records=[],
            raw_count=2, status="SUCCESS"
        )

        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()

            # Verify stale event did NOT overwrite ORD-1 status
            cursor.execute("SELECT status FROM fact_orders WHERE order_id = 'ORD-1'")
            self.assertEqual(cursor.fetchone()[0], "COMPLETED")

            # Verify cumulative SQL aggregation in agg_category_sales ($100 + $200 = $300)
            cursor.execute("SELECT total_revenue_usd, total_orders FROM agg_category_sales WHERE category = 'Electronics'")
            rev, orders = cursor.fetchone()
            self.assertEqual(rev, 300.0)
            self.assertEqual(orders, 2)

            # Verify dim_customers was populated accurately from full warehouse history
            cursor.execute("SELECT total_orders, lifetime_spend_usd FROM dim_customers WHERE customer_id = 'cust_1'")
            cust_orders, ltv = cursor.fetchone()
            self.assertEqual(cust_orders, 2)
            self.assertEqual(ltv, 300.0)

    def test_data_quality_detects_flaws(self):
        bad_records = [
            {
                "order_id": "",
                "total_amount_usd": -50.0,
                "quantity": 0,
                "status": "INVALID_STATUS",
                "order_timestamp": "2026-11-10 14:00:00"
            }
        ]
        report = self.validator.validate(bad_records, strict=False)
        self.assertFalse(report["passed"])
        self.assertGreater(len(report["failed_checks"]), 0)


if __name__ == "__main__":
    unittest.main()
