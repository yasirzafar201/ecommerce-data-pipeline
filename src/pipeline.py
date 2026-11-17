import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from src.config import LOGS_DIR, DB_PATH
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.quality import DataQualityValidator
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
    """End-to-End Orchestrator for E-Commerce Data Engineering Pipeline."""

    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.validator = DataQualityValidator()
        self.loader = DataLoader()

    def run(self, record_count: int = 100, strict_quality: bool = False) -> Dict[str, Any]:
        """Executes the full pipeline and records execution metadata."""
        start_time = time.time()
        pipeline_run_id = f"RUN-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        logging.info("=" * 60)
        logging.info(f"Starting ETL Pipeline: {pipeline_run_id}")
        logging.info("=" * 60)

        # 1. EXTRACT
        logging.info("[STEP 1/4] Extracting raw batch data...")
        raw_data = self.extractor.generate_synthetic_batch(count=record_count, save_to_disk=True)

        # 2. TRANSFORM
        logging.info("[STEP 2/4] Transforming and normalizing records...")
        cleaned_records = self.transformer.clean_and_normalize(raw_data)
        category_agg = self.transformer.build_category_aggregates(cleaned_records)
        daily_agg = self.transformer.build_daily_aggregates(cleaned_records)

        # 3. DATA QUALITY AUDIT
        logging.info("[STEP 3/4] Running automated data quality gate...")
        quality_report = self.validator.validate(cleaned_records, strict=strict_quality)

        # 4. LOAD & EXPORT
        logging.info("[STEP 4/4] Loading into Analytical Warehouse and exporting CSVs...")
        self.loader.init_warehouse_schema()
        self.loader.load_fact_orders(cleaned_records)
        self.loader.load_aggregates(category_agg, daily_agg)
        self.loader.export_marts_to_csv(cleaned_records, category_agg, daily_agg)

        elapsed = round(time.time() - start_time, 2)
        summary = {
            "run_id": pipeline_run_id,
            "status": "SUCCESS" if quality_report["passed"] else "COMPLETED_WITH_WARNINGS",
            "raw_records": len(raw_data),
            "processed_records": len(cleaned_records),
            "categories_summarized": len(category_agg),
            "days_analyzed": len(daily_agg),
            "duration_seconds": elapsed,
            "warehouse_db": str(DB_PATH)
        }

        logging.info("-" * 60)
        logging.info(f"Pipeline Completed in {elapsed}s | Processed {len(cleaned_records)} records")
        logging.info("=" * 60)
        return summary
