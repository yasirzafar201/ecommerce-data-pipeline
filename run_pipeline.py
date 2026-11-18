#!/usr/bin/env python3
"""CLI Entry point for the Production-Resilient E-Commerce Data Pipeline."""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.pipeline import ETLPipeline


def main():
    parser = argparse.ArgumentParser(
        description="E-Commerce Sales Data Engineering ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--records",
        type=int,
        default=150,
        help="Number of synthetic transactional records to generate & process"
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Path to an existing raw JSON batch file (supports idempotent replay)"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail execution immediately if any data quality check fails"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass SHA-256 batch idempotency check and reprocess an existing batch"
    )
    parser.add_argument(
        "--no-anomalies",
        action="store_true",
        help="Disable synthetic real-world anomaly injection when generating batches"
    )

    args = parser.parse_args()

    pipeline = ETLPipeline()
    result = pipeline.run(
        record_count=args.records,
        strict_quality=args.strict,
        input_file=args.input_file,
        force=args.force,
        inject_anomalies=not args.no_anomalies
    )

    print("\n" + "=" * 55)
    print("           PIPELINE EXECUTION SUMMARY")
    print("=" * 55)
    for k, v in result.items():
        print(f" {k.replace('_', ' ').title():<25}: {v}")
    print("=" * 55)


if __name__ == "__main__":
    main()
