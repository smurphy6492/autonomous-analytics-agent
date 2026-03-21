"""Unit tests for QueryPlan, PlannedQuery, and QueryResult models."""

import pytest
from pydantic import ValidationError

from analytics_agent.models.query_plan import PlannedQuery, QueryPlan, QueryResult


def make_planned_query(query_id: str = "q1") -> PlannedQuery:
    return PlannedQuery(
        query_id=query_id,
        purpose="Summarise revenue by category",
        required_tables=["orders", "products"],
        required_columns=["category", "revenue"],
        aggregation_grain="by_category",
        expected_output_type="breakdown",
    )


class TestPlannedQuery:
    def test_valid(self) -> None:
        q = make_planned_query()
        assert q.query_id == "q1"
        assert q.expected_output_type == "breakdown"

    def test_invalid_output_type(self) -> None:
        with pytest.raises(ValidationError):
            PlannedQuery(
                query_id="q1",
                purpose="test",
                required_tables=[],
                required_columns=[],
                aggregation_grain="daily",
                expected_output_type="unknown_type",  # type: ignore[arg-type]
            )


class TestQueryPlan:
    def test_valid(self) -> None:
        plan = QueryPlan(
            analysis_approach="Analyse revenue trends.",
            queries=[make_planned_query("q1"), make_planned_query("q2")],
        )
        assert len(plan.queries) == 2

    def test_too_many_queries(self) -> None:
        with pytest.raises(ValidationError):
            QueryPlan(
                analysis_approach="Over-analysed.",
                queries=[make_planned_query(f"q{i}") for i in range(5)],
            )

    def test_no_queries(self) -> None:
        with pytest.raises(ValidationError):
            QueryPlan(analysis_approach="Nothing.", queries=[])


class TestQueryResult:
    def test_successful_result(self) -> None:
        result = QueryResult(
            query_id="q1",
            sql="SELECT 1",
            success=True,
            data=[{"total": 100}],
            row_count=1,
        )
        assert result.success is True
        assert result.error is None
        assert result.attempts == 1

    def test_failed_result(self) -> None:
        result = QueryResult(
            query_id="q1",
            sql="SELECT bad",
            success=False,
            error="Column not found",
            attempts=3,
        )
        assert result.success is False
        assert result.data is None
        assert result.row_count == 0

    def test_attempts_minimum(self) -> None:
        with pytest.raises(ValidationError):
            QueryResult(query_id="q1", sql="SELECT 1", success=True, attempts=0)
