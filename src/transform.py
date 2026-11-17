from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from src.config import EXCHANGE_RATES_TO_USD

class DataTransformer:
    """Cleans, normalizes, deduplicates, and aggregates raw transactional data."""

    def __init__(self, exchange_rates: Dict[str, float] = None):
        self.exchange_rates = exchange_rates or EXCHANGE_RATES_TO_USD

    def clean_and_normalize(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicates and cleans records, normalizing currencies and dates."""
        seen_order_ids = set()
        cleaned_records = []

        for record in raw_records:
            order_id = record.get("order_id")
            
            # 1. Deduplication
            if not order_id or order_id in seen_order_ids:
                continue
            seen_order_ids.add(order_id)

            # 2. Type casting & sanitization
            try:
                unit_price = float(record.get("unit_price", 0.0))
                quantity = int(record.get("quantity", 1))
            except (ValueError, TypeError):
                continue  # Skip corrupted unparseable numbers

            if unit_price < 0 or quantity <= 0:
                continue

            # 3. Currency Normalization to USD
            currency = str(record.get("currency", "USD")).upper()
            rate = self.exchange_rates.get(currency, 1.0)
            unit_price_usd = round(unit_price * rate, 2)
            total_amount_usd = round(unit_price_usd * quantity, 2)

            # 4. Standardizing strings & casing
            customer_id = str(record.get("customer_id", "")).strip().lower()
            customer_email = record.get("customer_email") or "unknown@domain.local"
            status = str(record.get("status", "PENDING")).strip().upper()
            category = str(record.get("category", "General")).strip()
            product_name = str(record.get("product_name", "Unknown Item")).strip()

            # 5. Timestamp parsing & date dimensions
            raw_ts = record.get("order_timestamp", "")
            try:
                dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                dt = datetime.now()

            order_date = dt.strftime("%Y-%m-%d")
            order_hour = dt.hour
            day_of_week = dt.strftime("%A")

            cleaned = {
                "order_id": order_id,
                "customer_id": customer_id,
                "customer_email": customer_email,
                "product_name": product_name,
                "category": category,
                "original_currency": currency,
                "unit_price_usd": unit_price_usd,
                "quantity": quantity,
                "total_amount_usd": total_amount_usd,
                "payment_method": record.get("payment_method", "credit_card"),
                "status": status,
                "order_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "order_date": order_date,
                "order_hour": order_hour,
                "day_of_week": day_of_week
            }
            cleaned_records.append(cleaned)

        print(f"[TRANSFORM] Cleaned {len(cleaned_records)} records from {len(raw_records)} raw rows.")
        return cleaned_records

    def build_category_aggregates(self, cleaned_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Computes summary metrics grouped by product category."""
        metrics = defaultdict(lambda: {"revenue": 0.0, "units": 0, "orders": 0})

        for r in cleaned_records:
            if r["status"] == "COMPLETED":
                cat = r["category"]
                metrics[cat]["revenue"] += r["total_amount_usd"]
                metrics[cat]["units"] += r["quantity"]
                metrics[cat]["orders"] += 1

        summary = []
        for category, data in metrics.items():
            summary.append({
                "category": category,
                "total_revenue_usd": round(data["revenue"], 2),
                "total_units_sold": data["units"],
                "total_orders": data["orders"],
                "avg_order_value_usd": round(data["revenue"] / data["orders"], 2) if data["orders"] > 0 else 0.0
            })
        return sorted(summary, key=lambda x: x["total_revenue_usd"], reverse=True)

    def build_daily_aggregates(self, cleaned_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Computes daily sales and order volume summary."""
        daily = defaultdict(lambda: {"revenue": 0.0, "completed": 0, "refunded": 0, "total": 0})

        for r in cleaned_records:
            d = r["order_date"]
            daily[d]["total"] += 1
            if r["status"] == "COMPLETED":
                daily[d]["revenue"] += r["total_amount_usd"]
                daily[d]["completed"] += 1
            elif r["status"] == "REFUNDED":
                daily[d]["refunded"] += 1

        summary = []
        for order_date, data in sorted(daily.items()):
            summary.append({
                "order_date": order_date,
                "daily_revenue_usd": round(data["revenue"], 2),
                "completed_orders": data["completed"],
                "refunded_orders": data["refunded"],
                "total_orders": data["total"]
            })
        return summary
