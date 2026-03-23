"""Example: run a revenue analysis against the Olist Brazilian E-Commerce dataset.

Usage
-----
Ensure you have set ANTHROPIC_API_KEY in your environment or .env file, and
that the Olist CSVs are in data/raw/olist/ (see README for download instructions).

    cd projects/autonomous-analytics-agent
    python examples/run_retail_analysis.py

The report will be written to output/olist_revenue_analysis.html.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Resolve the project root so this script works when run from any directory.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)

QUESTION = (
    "What product categories are driving the most revenue, "
    "and how has this changed over the past 12 months?"
)

# Core tables needed for revenue analysis (geolocation excluded — 1M rows,
# not needed for category/revenue questions).
CORE_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
    "product_category_name_translation.csv",
]


def main() -> None:
    from analytics_agent.config import get_settings
    from analytics_agent.pipeline.runner import PipelineRunner

    settings = get_settings()

    data_dir = PROJECT_ROOT / "data" / "raw" / "olist"
    data_paths = [data_dir / f for f in CORE_FILES]

    missing = [p for p in data_paths if not p.exists()]
    if missing:
        print("Missing data files:")
        for p in missing:
            print(f"  {p}")
        print("\nSee README.md for download instructions.")
        sys.exit(1)

    output_path = PROJECT_ROOT / "output" / "olist_revenue_analysis.html"

    print(f"Question : {QUESTION}")
    print(f"Files    : {len(data_paths)} CSVs")
    print(f"Output   : {output_path}")
    print()

    runner = PipelineRunner(settings=settings)
    report = runner.run(
        data_paths=data_paths,
        business_question=QUESTION,
        title="Olist Revenue Analysis — Category Drivers",
        output_path=output_path,
    )

    print(f"\nDone! Report written to: {output_path}")
    print(f"Execution time : {report.execution_time_ms / 1000:.1f}s")
    print(f"Queries run    : {len(report.query_results)}")
    successful_charts = sum(1 for c in report.rendered_charts if c.success)
    print(f"Charts rendered: {successful_charts}")
    if report.errors:
        print(f"Warnings       : {len(report.errors)}")
        for err in report.errors:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
