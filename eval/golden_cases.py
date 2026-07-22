"""Golden evaluation cases — questions, reference SQL, and expected numbers.

Each case pairs a business question with:
- ``reference_sql``: a reference query, authored independently of the agent,
  returning one row whose columns are the expected metric names. This is the
  *authoritative* correct answer.
- ``expected``: the expected values those columns must take, verified against
  the data. The test
  suite runs ``reference_sql`` against ``golden_dataset.csv`` and asserts it
  reproduces ``expected`` — so the ground truth is reproducible, not trusted.

The agent is graded by whether the numbers it produces for the question match
these expected values within tolerance. See ``eval.scorer``.

Dataset (``golden_dataset.csv``, 12 delivered orders) by category:
    electronics: 500 + 300 + 200 + 400 = 1400   (top category)
    furniture:   600 + 100 + 300       = 1000
    clothing:    250 + 150 + 100       =  500
    books:        80 + 120             =  200
    total = 3100 over 12 orders, avg order value = 258.33
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DATASET_PATH = Path(__file__).parent / "golden_dataset.csv"
DATASET_TABLE = "golden_dataset"


@dataclass(frozen=True)
class ExpectedMetric:
    """A single expected numeric answer with a relative tolerance."""

    name: str
    value: float
    rtol: float = 0.01


@dataclass(frozen=True)
class GoldenCase:
    """A business question with its reference SQL and expected metrics."""

    case_id: str
    question: str
    reference_sql: str
    expected: tuple[ExpectedMetric, ...] = field(default_factory=tuple)


GOLDEN_CASES: tuple[GoldenCase, ...] = (
    GoldenCase(
        case_id="total_and_average",
        question="What is total revenue, how many orders, and the average order value?",
        reference_sql=(
            "SELECT "
            "  SUM(revenue) AS total_revenue, "
            "  COUNT(*) AS order_count, "
            "  ROUND(AVG(revenue), 2) AS avg_order_value "
            f"FROM {DATASET_TABLE}"
        ),
        expected=(
            ExpectedMetric("total_revenue", 3100.0),
            ExpectedMetric("order_count", 12.0),
            ExpectedMetric("avg_order_value", 258.33),
        ),
    ),
    GoldenCase(
        case_id="category_breakdown",
        question="What is total revenue by product category?",
        reference_sql=(
            "SELECT "
            "  SUM(revenue) FILTER (WHERE category = 'electronics') AS electronics, "
            "  SUM(revenue) FILTER (WHERE category = 'furniture') AS furniture, "
            "  SUM(revenue) FILTER (WHERE category = 'clothing') AS clothing, "
            "  SUM(revenue) FILTER (WHERE category = 'books') AS books "
            f"FROM {DATASET_TABLE}"
        ),
        expected=(
            ExpectedMetric("electronics", 1400.0),
            ExpectedMetric("furniture", 1000.0),
            ExpectedMetric("clothing", 500.0),
            ExpectedMetric("books", 200.0),
        ),
    ),
    GoldenCase(
        case_id="top_category",
        question="Which category has the highest revenue, and how much?",
        reference_sql=(
            "SELECT SUM(revenue) AS top_category_revenue "
            f"FROM {DATASET_TABLE} "
            "GROUP BY category ORDER BY top_category_revenue DESC LIMIT 1"
        ),
        expected=(ExpectedMetric("top_category_revenue", 1400.0),),
    ),
)
