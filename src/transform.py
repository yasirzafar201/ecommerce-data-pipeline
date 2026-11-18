import re
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from src.config import (
    EXCHANGE_RATES_TO_USD,
    REQUIRED_SCHEMA_FIELDS,
    EXPECTED_SCHEMA_FIELDS,
)

SUPPORTED_TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
)


class DataTransformer:
    """
    Production-resilient transformation engine:
    - Enforces schema contracts & detects upstream schema drift
    - Routes malformed rows to Quarantine/DLQ (zero silent data loss)
    - Sanitizes dirty numeric formats (e.g. "$1,250.50")
    - Strictly validates currency exchange lookups
    - Parses multiple timestamp formats without faking current time
    - Performs latest-timestamp-wins deduplication for late-arriving updates
    """

    def __init__(self, exchange_rates: Optional[Dict[str, float]] = None):
        self.exchange_rates = exchange_rates or EXCHANGE_RATES_TO_USD

    @staticmethod
    def parse_price(raw_price: Any) -> Optional[float]:
        """Sanitizes formatted price strings like '$1,250.50' into float."""
        if isinstance(raw_price, (int, float)):
            return float(raw_price)
        if isinstance(raw_price, str):
            cleaned = re.sub(r"[$€£,\s]", "", raw_price.strip())
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    @staticmethod
    def parse_timestamp(raw_ts: Any) -> Optional[datetime]:
        """Parses standard, ISO-8601 UTC, and US slash timestamp formats."""
        if not raw_ts or not isinstance(raw_ts, str):
            return None
        raw_ts = raw_ts.strip()
        for fmt in SUPPORTED_TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(raw_ts, fmt)
            except ValueError:
                continue
        return None

    def clean_and_normalize(
        self,
        raw_records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        """
        Transforms raw records while capturing schema drift and quarantining
        any row that violates business or technical constraints.

        Returns:
            (cleaned_records, quarantined_records, transform_stats)
        """
        dedup_map: Dict[str, Tuple[datetime, Dict[str, Any]]] = {}
        quarantined: List[Dict[str, Any]] = []
        unexpected_fields: set = set()
        duplicates_merged = 0

        for idx, record in enumerate(raw_records):
            if not isinstance(record, dict):
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": "record_not_a_dict",
                    "raw_record": record
                })
                continue

            record_keys = set(record.keys())

            # 1. Schema Drift Detection: flag unexpected new columns (non-breaking warning)
            extra_keys = record_keys - EXPECTED_SCHEMA_FIELDS
            if extra_keys:
                unexpected_fields.update(extra_keys)

            # 2. Schema Contract Check: required fields must exist
            missing_keys = REQUIRED_SCHEMA_FIELDS - record_keys
            if missing_keys:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"missing_required_fields:{sorted(missing_keys)}",
                    "raw_record": record
                })
                continue

            order_id = str(record.get("order_id") or "").strip()
            if not order_id:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": "empty_order_id",
                    "raw_record": record
                })
                continue

            # 3. Price & Quantity Sanitization (no silent drops!)
            unit_price = self.parse_price(record.get("unit_price"))
            if unit_price is None or unit_price < 0:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"invalid_unit_price:{record.get('unit_price')}",
                    "raw_record": record
                })
                continue

            try:
                quantity = int(record.get("quantity"))
            except (ValueError, TypeError):
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"invalid_quantity:{record.get('quantity')}",
                    "raw_record": record
                })
                continue

            if quantity <= 0:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"non_positive_quantity:{quantity}",
                    "raw_record": record
                })
                continue

            # 4. Strict Currency Validation (prevent silent 1.0 USD distortion!)
            currency = str(record.get("currency") or "").strip().upper()
            if currency not in self.exchange_rates:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"unsupported_currency:{currency}",
                    "raw_record": record
                })
                continue

            # 5. Multi-Format Timestamp Parsing (never fake datetime.now() on bad dates!)
            dt = self.parse_timestamp(record.get("order_timestamp"))
            if dt is None:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": f"invalid_timestamp:{record.get('order_timestamp')}",
                    "raw_record": record
                })
                continue

            # 6. Currency Normalization & Enrichment
            rate = self.exchange_rates[currency]
            unit_price_usd = round(unit_price * rate, 2)
            total_amount_usd = round(unit_price_usd * quantity, 2)

            customer_id = str(record.get("customer_id") or "").strip().lower()
            if not customer_id:
                quarantined.append({
                    "row_index": idx,
                    "rejection_reason": "empty_customer_id",
                    "raw_record": record
                })
                continue

            customer_email = record.get("customer_email") or "unknown@domain.local"
            status = str(record.get("status") or "PENDING").strip().upper()
            category = str(record.get("category") or "General").strip()
            product_name = str(record.get("product_name") or "Unknown Item").strip()

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
                "payment_method": record.get("payment_method") or "credit_card",
                "status": status,
                "order_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "order_date": dt.strftime("%Y-%m-%d"),
                "order_hour": dt.hour,
                "day_of_week": dt.strftime("%A"),
            }

            # 7. Latest-Timestamp-Wins Deduplication (handles CDC / late updates)
            if order_id in dedup_map:
                duplicates_merged += 1
                existing_dt, _ = dedup_map[order_id]
                if dt >= existing_dt:
                    dedup_map[order_id] = (dt, cleaned)
            else:
                dedup_map[order_id] = (dt, cleaned)

        cleaned_records = [entry[1] for entry in dedup_map.values()]
        stats = {
            "raw_count": len(raw_records),
            "cleaned_count": len(cleaned_records),
            "quarantined_count": len(quarantined),
            "duplicates_merged": duplicates_merged,
            "schema_drift_unexpected_fields": sorted(unexpected_fields),
        }

        print(
            f"[TRANSFORM] Cleaned {len(cleaned_records)} records | "
            f"Quarantined {len(quarantined)} | Merged {duplicates_merged} duplicates"
        )
        if unexpected_fields:
            print(f"[SCHEMA DRIFT WARNING] Unexpected upstream fields detected: {sorted(unexpected_fields)}")

        return cleaned_records, quarantined, stats

    def build_category_aggregates(self, cleaned_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Computes in-memory summary metrics grouped by product category."""
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
                "avg_order_value_usd": round(data["revenue"] / data["orders"], 2) if data["orders"] > 0 else 0.0,
            })
        return sorted(summary, key=lambda x: x["total_revenue_usd"], reverse=True)

    def build_daily_aggregates(self, cleaned_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Computes in-memory daily sales and order volume summary."""
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
                "total_orders": data["total"],
            })
        return summary
