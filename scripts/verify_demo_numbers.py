"""Reproduce and verify the headline numbers in the Olist revenue demo.

The portfolio site quotes specific figures from the e-commerce demo — Health &
Beauty at $1.23M, a top-5 total of $5.27M, and so on. This script recomputes
those figures directly from the raw Kaggle Olist dataset with an explicit query,
so anyone can confirm the published numbers in one command instead of taking
them on trust.

The Olist data is not committed (it is ~40MB, gitignored under ``data/raw/``).
Download it first (see the README), then run::

    python scripts/verify_demo_numbers.py
    python scripts/verify_demo_numbers.py --data-dir data/raw/olist_revenue

Exit code 0 = every published figure reproduced within tolerance; 1 = a
mismatch or the data is missing.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

_DEFAULT_DATA_DIR = Path("data/raw/olist_revenue")
_RTOL = 0.005  # 0.5% — the site rounds to two significant figures ($1.23M).

_REQUIRED_TABLES = (
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_products_dataset",
    "product_category_name_translation",
)

# Revenue = SUM(order_items.price) for delivered orders, by English category.
# This is the exact grain the demo report uses (item price, not payment value,
# which would double-count via the payments table).
_CATEGORY_REVENUE_SQL = """
SELECT tr.product_category_name_english AS category,
       SUM(oi.price) AS revenue,
       COUNT(*)      AS items
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o  ON oi.order_id = o.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
LEFT JOIN product_category_name_translation tr
       ON p.product_category_name = tr.product_category_name
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY revenue DESC
"""


@dataclass(frozen=True)
class Expectation:
    """A published figure to verify."""

    label: str
    actual_getter: str  # description of what we compute
    expected: float


# The figures as published on the portfolio site.
_EXPECTATIONS = (
    Expectation("Health & Beauty revenue", "top category revenue", 1_233_131.72),
    Expectation("Health & Beauty items sold", "top category item count", 9_465),
    Expectation("Watches & Gifts revenue", "2nd category revenue", 1_166_176.98),
    Expectation("Bed/Bath/Table revenue", "3rd category revenue", 1_023_434.76),
    Expectation("Top-5 categories total", "sum of top-5 revenues", 5_266_320.62),
)


def _load(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for table in _REQUIRED_TABLES:
        csv = data_dir / f"{table}.csv"
        if not csv.exists():
            raise FileNotFoundError(csv)
        safe = str(csv).replace("\\", "/")
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_csv_auto('{safe}')")
    return con


def _compute(con: duckdb.DuckDBPyConnection) -> dict[str, float]:
    rows = con.execute(_CATEGORY_REVENUE_SQL).fetchall()
    top5_total = sum(r[1] for r in rows[:5])
    return {
        "Health & Beauty revenue": float(rows[0][1]),
        "Health & Beauty items sold": float(rows[0][2]),
        "Watches & Gifts revenue": float(rows[1][1]),
        "Bed/Bath/Table revenue": float(rows[2][1]),
        "Top-5 categories total": float(top5_total),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=_DEFAULT_DATA_DIR)
    args = parser.parse_args()

    try:
        con = _load(args.data_dir)
    except FileNotFoundError as exc:
        print(f"Olist data not found: {exc}")
        print(
            "Download the Brazilian E-Commerce (Olist) dataset from Kaggle into "
            f"{args.data_dir}/ first — see the README."
        )
        return 1

    actual = _compute(con)

    all_ok = True
    print(f"{'Figure':<30} {'expected':>16} {'actual':>16}   result")
    print("-" * 74)
    for exp in _EXPECTATIONS:
        got = actual[exp.label]
        tol = max(_RTOL * abs(exp.expected), 0.5)
        ok = abs(got - exp.expected) <= tol
        all_ok = all_ok and ok
        print(
            f"{exp.label:<30} {exp.expected:>16,.2f} {got:>16,.2f}   "
            f"{'PASS' if ok else 'FAIL'}"
        )

    print("-" * 74)
    print("ALL FIGURES REPRODUCED" if all_ok else "MISMATCH — see FAIL rows above")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
