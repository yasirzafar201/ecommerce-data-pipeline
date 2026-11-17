from datetime import datetime
from typing import List, Dict, Any

class DataQualityError(Exception):
    """Raised when data quality thresholds are violated."""
    pass

class DataQualityValidator:
    """Automated data quality assertion framework for pipeline auditing."""

    def __init__(self, allowed_statuses: List[str] = None):
        self.allowed_statuses = allowed_statuses or ["COMPLETED", "REFUNDED", "PENDING", "CANCELLED"]

    def validate(self, records: List[Dict[str, Any]], strict: bool = False) -> Dict[str, Any]:
        """Runs validation checks on transformed dataset."""
        report = {
            "total_records": len(records),
            "passed": True,
            "failed_checks": [],
            "metrics": {
                "null_order_ids": 0,
                "duplicate_order_ids": 0,
                "negative_amounts": 0,
                "invalid_statuses": 0,
                "future_timestamps": 0
            }
        }

        seen_ids = set()
        now = datetime.now()

        for idx, r in enumerate(records):
            order_id = r.get("order_id")

            # Check 1: Null check on Primary Key
            if not order_id:
                report["metrics"]["null_order_ids"] += 1

            # Check 2: Duplicate check
            if order_id in seen_ids:
                report["metrics"]["duplicate_order_ids"] += 1
            else:
                seen_ids.add(order_id)

            # Check 3: Non-negative financial constraints
            if r.get("total_amount_usd", 0) < 0 or r.get("quantity", 0) <= 0:
                report["metrics"]["negative_amounts"] += 1

            # Check 4: Valid categorical values
            if r.get("status") not in self.allowed_statuses:
                report["metrics"]["invalid_statuses"] += 1

            # Check 5: Timestamp plausibility
            try:
                ts = datetime.strptime(r["order_timestamp"], "%Y-%m-%d %H:%M:%S")
                if ts > now:
                    report["metrics"]["future_timestamps"] += 1
            except (KeyError, ValueError):
                pass

        # Evaluate pass/fail conditions
        for metric, count in report["metrics"].items():
            if count > 0:
                report["failed_checks"].append(f"{metric}: {count} violations detected")

        if report["failed_checks"]:
            report["passed"] = False
            if strict:
                raise DataQualityError(f"Data Quality Gate Failed: {report['failed_checks']}")

        status_str = "PASSED" if report["passed"] else "WARNINGS/FAILED"
        print(f"[QUALITY] Data Quality Check Result: {status_str} ({len(report['failed_checks'])} issues)")
        return report
