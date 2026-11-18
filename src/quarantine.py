import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.config import QUARANTINE_DIR


class DeadLetterQueue:
    """
    Persists rejected/malformed records to a Quarantine Zone (Dead-Letter Queue)
    with explicit rejection reasons so engineers can inspect, fix, and replay them
    instead of suffering silent data loss.
    """

    def __init__(self, quarantine_dir: Path = QUARANTINE_DIR):
        self.quarantine_dir = quarantine_dir
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def write_quarantine_batch(
        self,
        run_id: str,
        quarantined_records: List[Dict[str, Any]]
    ) -> Optional[Path]:
        """Writes quarantined records with metadata to a JSON DLQ file."""
        if not quarantined_records:
            return None

        output_file = self.quarantine_dir / f"dlq_{run_id}.json"
        payload = {
            "run_id": run_id,
            "quarantined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rejected_count": len(quarantined_records),
            "records": quarantined_records
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)

        print(f"[QUARANTINE] Routed {len(quarantined_records)} malformed records to DLQ: {output_file.name}")
        return output_file
