import hashlib
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple
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
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def compute_batch_hash(records: List[Dict[str, Any]]) -> str:
        """Computes a deterministic SHA-256 fingerprint for idempotency checks."""
        canonical = json.dumps(records, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def generate_synthetic_batch(
        self,
        count: int = 100,
        save_to_disk: bool = True,
        inject_anomalies: bool = True
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Generates realistic e-commerce transactions including common real-world
        production anomalies (dirty strings, mixed dates, schema drift, duplicates,
        and unknown currencies).
        """
        records = []
        base_time = datetime.now() - timedelta(days=7)
        batch_prefix = datetime.now().strftime("%H%M%S")

        for i in range(1, count + 1):
            category = random.choice(list(CATEGORIES.keys()))
            product = random.choice(CATEGORIES[category])
            order_date = base_time + timedelta(
                days=random.randint(0, 6),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            # Messy customer casing & trailing spaces
            raw_customer = f"cust_{random.randint(100, 150)}"
            if random.random() < 0.2:
                raw_customer = f"  {raw_customer.upper()}  "

            currency = random.choice(CURRENCIES)
            base_price = round(random.uniform(15.0, 950.0), 2)
            quantity = random.randint(1, 5)
            email = f"user{random.randint(100, 150)}@example.com" if random.random() > 0.05 else None

            # Real-world messy price formatting (e.g., "$1,250.50" or string floats)
            unit_price_val: Any = base_price
            if random.random() < 0.15:
                unit_price_val = f"${base_price:,.2f}"
            elif random.random() < 0.30:
                unit_price_val = str(base_price)

            # Real-world mixed timestamp formats (standard vs ISO-8601 UTC vs slash format)
            ts_roll = random.random()
            if ts_roll < 0.15:
                timestamp_str = order_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif ts_roll < 0.25:
                timestamp_str = order_date.strftime("%m/%d/%Y %H:%M:%S")
            else:
                timestamp_str = order_date.strftime("%Y-%m-%d %H:%M:%S")

            record: Dict[str, Any] = {
                "order_id": f"ORD-{batch_prefix}-{1000 + i}",
                "customer_id": raw_customer,
                "customer_email": email,
                "product_name": product,
                "category": category,
                "unit_price": unit_price_val,
                "quantity": quantity,
                "currency": currency,
                "payment_method": random.choice(PAYMENT_METHODS),
                "order_timestamp": timestamp_str,
                "status": random.choice(["COMPLETED", "completed", "REFUNDED", "PENDING"])
            }
            records.append(record)

        if inject_anomalies and count >= 10:
            # 1. Late-arriving update / duplicate with newer timestamp & updated status
            updated_dup = records[0].copy()
            updated_dup["status"] = "COMPLETED"
            updated_dup["order_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            records.append(updated_dup)

            # 2. Corrupted price record (should go to Quarantine / DLQ, NOT crash or vanish)
            bad_price = records[1].copy()
            bad_price["order_id"] = f"ORD-{batch_prefix}-BAD-PRICE"
            bad_price["unit_price"] = "N/A"
            records.append(bad_price)

            # 3. Unknown upstream currency (should be quarantined instead of defaulting to 1.0 USD)
            bad_currency = records[2].copy()
            bad_currency["order_id"] = f"ORD-{batch_prefix}-BAD-CURR"
            bad_currency["currency"] = "JPY"
            records.append(bad_currency)

            # 4. Unparseable timestamp (should be quarantined instead of faking datetime.now())
            bad_ts = records[3].copy()
            bad_ts["order_id"] = f"ORD-{batch_prefix}-BAD-TS"
            bad_ts["order_timestamp"] = "invalid-date-format"
            records.append(bad_ts)

            # 5. Upstream schema drift: unexpected new column added by upstream API
            records[4]["promo_code_v2"] = "BLACKFRIDAY20"

        batch_hash = self.compute_batch_hash(records)

        if save_to_disk:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.raw_dir / f"batch_orders_{timestamp_str}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            print(f"[EXTRACT] Ingested {len(records)} raw records to: {output_file.name}")

        return records, batch_hash

    def load_from_json(self, file_path: Path) -> Tuple[List[Dict[str, Any]], str]:
        """Reads raw JSON data from a given path and computes its SHA-256 fingerprint."""
        with open(file_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        batch_hash = self.compute_batch_hash(records)
        print(f"[EXTRACT] Loaded {len(records)} raw records from: {file_path.name}")
        return records, batch_hash
