#!/usr/bin/env python3
"""CLI Entry point for the E-Commerce Data Pipeline."""

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
        "--strict",
        action="store_true",
        help="Fail execution immediately if any data quality check fails"
    )

    args = parser.parse_args()

    pipeline = ETLPipeline()
    result = pipeline.run(record_count=args.records, strict_quality=args.strict)

    print("\n" + "=" * 45)
    print("      PIPELINE EXECUTION SUMMARY")
    print("=" * 45)
    for k, v in result.items():
        print(f" {k.replace('_', ' ').title():<25}: {v}")
    print("=" * 45)

if __name__ == "__main__":
    main()
