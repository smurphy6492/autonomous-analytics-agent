"""Deterministic scoring of agent output against golden expected numbers.

No LLM and no API key: given the numeric values an agent produced for a golden
case, check that every expected metric appears among them within tolerance, and
report an accuracy. This is what turns "the agent wrote a report" into "the
agent got N of M reference numbers right".
"""

from __future__ import annotations

from dataclasses import dataclass

from analytics_agent.db.engine import DuckDBEngine
from analytics_agent.models.query_plan import QueryResult
from eval.golden_cases import DATASET_PATH, DATASET_TABLE, ExpectedMetric, GoldenCase


@dataclass(frozen=True)
class MetricScore:
    """Whether one expected metric was matched by the agent's numbers."""

    name: str
    expected: float
    matched: bool
    closest: float | None


@dataclass(frozen=True)
class CaseScore:
    """The scored result of one golden case."""

    case_id: str
    metrics: tuple[MetricScore, ...]

    @property
    def matched_count(self) -> int:
        return sum(1 for m in self.metrics if m.matched)

    @property
    def total_count(self) -> int:
        return len(self.metrics)

    @property
    def accuracy(self) -> float:
        """Fraction of expected metrics the agent reproduced (0.0-1.0)."""
        if not self.metrics:
            return 1.0
        return self.matched_count / self.total_count

    @property
    def passed(self) -> bool:
        return self.matched_count == self.total_count


def collect_numeric_values(query_results: dict[str, QueryResult]) -> list[float]:
    """Flatten every numeric value from all successful query results."""
    values: list[float] = []
    for result in query_results.values():
        if not result.success or not result.data:
            continue
        for row in result.data:
            for val in row.values():
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    values.append(float(val))
    return values


def _matches(
    expected: ExpectedMetric, values: list[float]
) -> tuple[bool, float | None]:
    """Return (matched, closest_value) for *expected* against *values*."""
    if not values:
        return False, None
    closest = min(values, key=lambda v: abs(v - expected.value))
    tol = max(expected.rtol * abs(expected.value), 1e-9)
    return abs(closest - expected.value) <= tol, closest


def score_case(case: GoldenCase, agent_values: list[float]) -> CaseScore:
    """Score the agent's numeric output for one golden case."""
    metrics: list[MetricScore] = []
    for metric in case.expected:
        matched, closest = _matches(metric, agent_values)
        metrics.append(
            MetricScore(
                name=metric.name,
                expected=metric.value,
                matched=matched,
                closest=closest,
            )
        )
    return CaseScore(case_id=case.case_id, metrics=tuple(metrics))


def overall_accuracy(scores: list[CaseScore]) -> float:
    """Fraction of all expected metrics matched across every case (0.0-1.0)."""
    total = sum(s.total_count for s in scores)
    matched = sum(s.matched_count for s in scores)
    return matched / total if total else 1.0


def compute_reference(case: GoldenCase) -> dict[str, float]:
    """Run *case*'s reference SQL against the golden dataset.

    Returns the single reference row as a name → value mapping. Used to prove
    the expected numbers are reproducible from the data, not just
    asserted. Raises if the reference SQL returns anything but one row.
    """
    with DuckDBEngine() as engine:
        engine.load_csv(DATASET_PATH, DATASET_TABLE)
        rows = engine.execute(case.reference_sql)
    if len(rows) != 1:
        raise ValueError(
            f"Reference SQL for '{case.case_id}' must return exactly one row, "
            f"got {len(rows)}."
        )
    return {k: float(v) for k, v in rows[0].items() if v is not None}
