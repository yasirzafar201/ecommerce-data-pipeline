import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from src.config import LOGS_DIR, DB_PATH
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.quality import DataQualityValidator
from src.quarantine import DeadLetterQueue
from src.load import DataLoader

# Configure logging
log_file = LOGS_DIR / "pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)


class ETLPipeline:
    """
    Production-Resilient End-to-End Orchestrator for E-Commerce Data Engineering Pipeline.

    Solves core real-world data engineering failure modes:
    1. Idempotency via SHA-256 batch checksums (safe re-runs)
    2. Dead-Letter Queue / Quarantine Zone (zero silent data loss)
    3. Upstream Schema Drift detection
    4. Out-of-order safe upserts & atomic ACID warehouse transactions
    5. Incremental full-warehouse SQL aggregations
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.validator = DataQualityValidator()
        self.dlq = DeadLetterQueue()
        self.loader = DataLoader(db_path=db_path)

    def run(
        self,
        record_count: int = 100,
        strict_quality: bool = False,
        input_file: Optional[Path] = None,
        force: bool = False,
        inject_anomalies: bool = True
    ) -> Dict[str, Any]:
        """Executes the full resilient pipeline and records execution metadata."""
        start_time = time.time()
        pipeline_run_id = f"RUN-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:17]}"

        logging.info("=" * 65)
        logging.info(f"Starting Production ETL Pipeline: {pipeline_run_id}")
        logging.info("=" * 65)

        self.loader.init_warehouse_schema()

        # 1. EXTRACT
        logging.info("[STEP 1/5] Extracting raw batch data...")
        if input_file is not None:
            raw_data, batch_hash = self.extractor.load_from_json(Path(input_file))
        else:
            raw_data, batch_hash = self.extractor.generate_synthetic_batch(
                count=record_count,
                save_to_disk=True,
                inject_anomalies=inject_anomalies
            )

        # 2. IDEMPOTENCY GATE
        logging.info("[STEP 2/5] Verifying batch idempotency (SHA-256 fingerprint)...")
        if not force and self.loader.is_batch_already_processed(batch_hash):
            elapsed = round(time.time() - start_time, 3)
            logging.warning(f"Batch {batch_hash[:12]}... was already processed. Skipping duplicate load.")
            return {
                "run_id": pipeline_run_id,
                "status": "SKIPPED_DUPLICATE_BATCH",
                "batch_hash": batch_hash[:12],
                "raw_records": len(raw_data),
                "processed_records": 0,
                "quarantined_records": 0,
                "duplicates_merged": 0,
                "schema_drift_fields": [],
                "duration_seconds": elapsed,
                "warehouse_db": str(self.db_path)
            }

        # 3. TRANSFORM, SCHEMA DRIFT CHECK & QUARANTINE ROUTING
        logging.info("[STEP 3/5] Transforming records, detecting schema drift, routing bad rows to DLQ...")
        cleaned_records, quarantined_records, transform_stats = self.transformer.clean_and_normalize(raw_data)

        dlq_file = None
        if quarantined_records:
            dlq_file = self.dlq.write_quarantine_batch(pipeline_run_id, quarantined_records)
            logging.warning(f"Quarantined {len(quarantined_records)} malformed records -> {dlq_file}")

        if transform_stats["schema_drift_unexpected_fields"]:
            logging.warning(
                f"Schema Drift detected! New upstream columns: "
                f"{transform_stats['schema_drift_unexpected_fields']}"
            )

        # 4. DATA QUALITY AUDIT
        logging.info("[STEP 4/5] Running automated data quality gate...")
        quality_report = self.validator.validate(cleaned_records, strict=strict_quality)

        run_status = "SUCCESS"
        if not quality_report["passed"] or quarantined_records:
            run_status = "COMPLETED_WITH_WARNINGS"

        # 5. ATOMIC TRANSACTIONAL WAREHOUSE LOAD & SQL MART REBUILD
        logging.info("[STEP 5/5] Executing atomic ACID load & rebuilding cumulative SQL marts...")
        self.loader.load_transactional_batch(
            run_id=pipeline_run_id,
            batch_hash=batch_hash,
            cleaned_records=cleaned_records,
            quarantined_records=quarantined_records,
            raw_count=len(raw_data),
            status=run_status
        )
        self.loader.export_warehouse_marts_to_csv()

        elapsed = round(time.time() - start_time, 3)
        summary = {
            "run_id": pipeline_run_id,
            "status": run_status,
            "batch_hash": batch_hash[:12],
            "raw_records": len(raw_data),
            "processed_records": len(cleaned_records),
            "quarantined_records": len(quarantined_records),
            "duplicates_merged": transform_stats["duplicates_merged"],
            "schema_drift_fields": transform_stats["schema_drift_unexpected_fields"],
            "dlq_file": str(dlq_file) if dlq_file else "None",
            "duration_seconds": elapsed,
            "warehouse_db": str(self.db_path)
        }

        logging.info("-" * 65)
        logging.info(
            f"Pipeline Completed in {elapsed}s | Loaded {len(cleaned_records)} | "
            f"Quarantined {len(quarantined_records)} | Status: {run_status}"
        )
        logging.info("=" * 65)
        return summary
