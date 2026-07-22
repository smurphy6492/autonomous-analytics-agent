"""Tests for the golden-answer eval — CI-safe, no API key required.

These prove two things without ever calling the LLM:
1. The expected numbers are reproducible from the committed dataset (the
   reference query actually produces them).
2. The scorer correctly grades agent output against those numbers.
"""

from __future__ import annotations

from analytics_agent.models.query_plan import QueryResult
from eval.golden_cases import GOLDEN_CASES
from eval.scorer import (
    collect_numeric_values,
    compute_reference,
    overall_accuracy,
    score_case,
)


def _result(data: list[dict]) -> dict[str, QueryResult]:
    return {
        "q": QueryResult(
            query_id="q",
            sql="SELECT 1",
            success=True,
            data=data,
            row_count=len(data),
            attempts=1,
        )
    }


# ------------------------------------------------------------------
# Ground truth is reproducible: reference SQL reproduces expected numbers
# ------------------------------------------------------------------


class TestReferenceReproducible:
    def test_every_case_reference_sql_matches_expected(self) -> None:
        # The authoritative check: running each case's reference SQL against the
        # committed dataset reproduces its expected values.
        for case in GOLDEN_CASES:
            ref = compute_reference(case)
            for metric in case.expected:
                assert metric.name in ref, (
                    f"{case.case_id}: reference SQL produced no column "
                    f"'{metric.name}' (got {sorted(ref)})"
                )
                tol = max(metric.rtol * abs(metric.value), 1e-9)
                assert abs(ref[metric.name] - metric.value) <= tol, (
                    f"{case.case_id}: {metric.name} = {ref[metric.name]}, "
                    f"expected {metric.value}"
                )

    def test_case_ids_unique(self) -> None:
        ids = [c.case_id for c in GOLDEN_CASES]
        assert len(ids) == len(set(ids))


# ------------------------------------------------------------------
# Scorer grades agent output correctly
# ------------------------------------------------------------------


class TestScorer:
    def test_perfect_agent_scores_100(self) -> None:
        case = next(c for c in GOLDEN_CASES if c.case_id == "category_breakdown")
        # An agent that produced exactly the right category revenues.
        agent_values = collect_numeric_values(
            _result(
                [
                    {"category": "electronics", "revenue": 1400.0},
                    {"category": "furniture", "revenue": 1000.0},
                    {"category": "clothing", "revenue": 500.0},
                    {"category": "books", "revenue": 200.0},
                ]
            )
        )
        score = score_case(case, agent_values)
        assert score.passed
        assert score.accuracy == 1.0

    def test_wrong_agent_flags_missing_metric(self) -> None:
        case = next(c for c in GOLDEN_CASES if c.case_id == "total_and_average")
        # Total revenue double-counted (a join fan-out signature): 6200 not 3100.
        agent_values = collect_numeric_values(
            _result([{"total_revenue": 6200.0, "order_count": 12, "aov": 258.33}])
        )
        score = score_case(case, agent_values)
        assert not score.passed
        missed = {m.name for m in score.metrics if not m.matched}
        assert "total_revenue" in missed
        # The correct order_count and avg still match.
        assert "order_count" not in missed

    def test_overall_accuracy_aggregates_cases(self) -> None:
        case = next(c for c in GOLDEN_CASES if c.case_id == "top_category")
        good = score_case(case, [1400.0])
        bad = score_case(case, [9999.0])
        assert overall_accuracy([good, bad]) == 0.5

    def test_empty_agent_values_scores_zero(self) -> None:
        case = next(c for c in GOLDEN_CASES if c.case_id == "category_breakdown")
        score = score_case(case, [])
        assert score.accuracy == 0.0
