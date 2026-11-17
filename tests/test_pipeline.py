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

    def test_extract_generates_valid_keys(self):
        records = self.extractor.generate_synthetic_batch(count=5, save_to_disk=False)
        self.assertGreaterEqual(len(records), 5)
        sample = records[0]
        required_keys = ["order_id", "customer_id", "product_name", "unit_price", "currency", "order_timestamp"]
        for key in required_keys:
            self.assertIn(key, sample)

    def test_transform_deduplication_and_currency(self):
        raw_test_data = [
            {
                "order_id": "ORD-001",
                "customer_id": "  CUST_999  ",
                "product_name": "Laptop",
                "category": "Electronics",
                "unit_price": "100.0",
                "quantity": 2,
                "currency": "EUR",
                "status": "completed",
                "order_timestamp": "2026-11-10 14:00:00"
            },
            {
                # Duplicate record with exact same order_id
                "order_id": "ORD-001",
                "customer_id": "CUST_999",
                "product_name": "Laptop",
                "category": "Electronics",
                "unit_price": "100.0",
                "quantity": 2,
                "currency": "EUR",
                "status": "completed",
                "order_timestamp": "2026-11-10 14:00:00"
            }
        ]

        cleaned = self.transformer.clean_and_normalize(raw_test_data)
        # Should be deduplicated to 1 record
        self.assertEqual(len(cleaned), 1)
        record = cleaned[0]
        self.assertEqual(record["customer_id"], "cust_999")
        self.assertEqual(record["status"], "COMPLETED")
        # 100 EUR * 1.08 = 108 USD per unit * 2 = 216 USD
        self.assertEqual(record["unit_price_usd"], 108.0)
        self.assertEqual(record["total_amount_usd"], 216.0)

    def test_data_quality_detects_flaws(self):
        bad_records = [
            {
                "order_id": "",  # Missing ID
                "total_amount_usd": -50.0,  # Negative
                "quantity": 0,
                "status": "INVALID_STATUS",
                "order_timestamp": "2026-11-10 14:00:00"
            }
        ]
        report = self.validator.validate(bad_records, strict=False)
        self.assertFalse(report["passed"])
        self.assertGreater(len(report["failed_checks"]), 0)

    def test_database_load_and_query(self):
        self.loader.init_warehouse_schema()
        dummy_record = [{
            "order_id": "ORD-TEST-1",
            "customer_id": "cust_1",
            "product_name": "Mechanical Keyboard",
            "category": "Electronics",
            "unit_price_usd": 120.0,
            "quantity": 1,
            "total_amount_usd": 120.0,
            "payment_method": "paypal",
            "status": "COMPLETED",
            "order_timestamp": "2026-11-12 10:00:00",
            "order_date": "2026-11-12",
            "day_of_week": "Thursday"
        }]

        self.loader.load_fact_orders(dummy_record)

        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT order_id, total_amount_usd FROM fact_orders WHERE order_id = 'ORD-TEST-1'")
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "ORD-TEST-1")
            self.assertEqual(row[1], 120.0)

if __name__ == "__main__":
    unittest.main()
