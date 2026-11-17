import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
from src.config import RAW_DATA_DIR

CATEGORIES = {
    "Electronics": ["Laptop", "Wireless Headphones", "Mechanical Keyboard", "Smart Watch"],
    "Apparel": ["Cotton T-Shirt", "Denim Jacket", "Running Shoes", "Winter Hoodie"],
    "Home & Kitchen": ["Espresso Maker", "Air Fryer", "Desk Lamp", "Ceramic Mug Set"],
    "Books": ["Designing Data-Intensive Apps", "Python Crash Course", "Clean Architecture"]
}

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "PKR"]
PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "crypto"]

class DataExtractor:
    """Handles raw data ingestion from files or synthetic streaming generators."""

    def __init__(self, raw_dir: Path = RAW_DATA_DIR):
        self.raw_dir = raw_dir

    def generate_synthetic_batch(self, count: int = 100, save_to_disk: bool = True) -> List[Dict[str, Any]]:
        """Generates realistic e-commerce transactions with real-world messiness."""
        records = []
        base_time = datetime.now() - timedelta(days=7)

        for i in range(1, count + 1):
            category = random.choice(list(CATEGORIES.keys()))
            product = random.choice(CATEGORIES[category])
            order_date = base_time + timedelta(
                days=random.randint(0, 7),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            # Introduce intentional real-world data issues for the transform step to resolve:
            # 1. Messy customer casing
            raw_customer = f"cust_{random.randint(100, 150)}"
            if random.random() < 0.2:
                raw_customer = raw_customer.upper()

            # 2. Variable currency
            currency = random.choice(CURRENCIES)

            # 3. String or float price
            base_price = round(random.uniform(15.0, 950.0), 2)
            quantity = random.randint(1, 5)

            # 4. Occasional missing/null email or inconsistent phone
            email = f"user{random.randint(100, 150)}@example.com" if random.random() > 0.05 else None

            record = {
                "order_id": f"ORD-{1000 + i}",
                "customer_id": raw_customer.strip(),
                "customer_email": email,
                "product_name": product,
                "category": category,
                "unit_price": base_price if random.random() > 0.3 else str(base_price),
                "quantity": quantity,
                "currency": currency,
                "payment_method": random.choice(PAYMENT_METHODS),
                "order_timestamp": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(["COMPLETED", "completed", "REFUNDED", "PENDING"])
            }
            records.append(record)

        # 5. Inject a few intentional duplicate records to test deduplication
        if count >= 10:
            dup_target = records[0].copy()
            records.append(dup_target)

        if save_to_disk:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.raw_dir / f"batch_orders_{timestamp_str}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            print(f"[EXTRACT] Ingested {len(records)} raw records to: {output_file.name}")

        return records

    def load_from_json(self, file_path: Path) -> List[Dict[str, Any]]:
        """Reads raw JSON data from a given path."""
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
