"""Unit tests for OrchestratorAgent."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from analytics_agent.agents.base import AgentError, BaseAgent
from analytics_agent.agents.orchestrator import (
    OrchestratorAgent,
    _build_plan_prompt,
    _build_synthesis_prompt,
)
from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    TableProfile,
)
from analytics_agent.models.query_plan import (
    PlannedQuery,
    QueryPlan,
    QueryPlanRequest,
    QueryResult,
)
from analytics_agent.models.report import (
    AnalysisSynthesis,
    DataTableSpec,
    KeyMetric,
    SynthesisRequest,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> DataProfile:
    return DataProfile(
        tables=[
            TableProfile(
                name="orders",
                row_count=1000,
                columns=[
                    ColumnProfile(
                        name="order_id",
                        dtype="BIGINT",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=1000,
                        cardinality="high",
                        sample_values=["1", "2", "3"],
                        is_date=False,
                        is_numeric=True,
                        min_value="1",
                        max_value="1000",
                    ),
                    ColumnProfile(
                        name="order_date",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=365,
                        cardinality="high",
                        sample_values=["2023-01-01"],
                        is_date=True,
                        is_numeric=False,
                        min_value="2023-01-01",
                        max_value="2023-12-31",
                    ),
                    ColumnProfile(
                        name="revenue",
                        dtype="DOUBLE",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=500,
                        cardinality="high",
                        sample_values=["100.0"],
                        is_date=False,
                        is_numeric=True,
                        min_value="10.0",
                        max_value="5000.0",
                    ),
                    ColumnProfile(
                        name="category",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=5,
                        cardinality="low",
                        sample_values=["electronics", "clothing"],
                        is_date=False,
                        is_numeric=False,
                        min_value=None,
                        max_value=None,
                    ),
                ],
            )
        ],
        relationships=[],
        suggested_grain="order_id",
        data_quality_issues=[],
    )


@pytest.fixture
def query_plan() -> QueryPlan:
    return QueryPlan(
        analysis_approach="Analyse revenue over time and by category.",
        queries=[
            PlannedQuery(
                query_id="monthly_revenue",
                purpose="Monthly revenue trend",
                required_tables=["orders"],
                required_columns=["order_date", "revenue"],
                aggregation_grain="monthly",
                expected_output_type="time_series",
            ),
            PlannedQuery(
                query_id="revenue_by_category",
                purpose="Revenue breakdown by product category",
                required_tables=["orders"],
                required_columns=["category", "revenue"],
                aggregation_grain="by_category",
                expected_output_type="breakdown",
            ),
        ],
    )


@pytest.fixture
def query_results() -> dict[str, QueryResult]:
    return {
        "monthly_revenue": QueryResult(
            query_id="monthly_revenue",
            sql="SELECT month, SUM(revenue) AS total FROM orders GROUP BY month",
            success=True,
            data=[
                {"month": "2023-01", "total": 10000.0},
                {"month": "2023-02", "total": 12000.0},
            ],
            row_count=2,
            attempts=1,
        ),
        "revenue_by_category": QueryResult(
            query_id="revenue_by_category",
            sql="SELECT category, SUM(revenue) AS total FROM orders GROUP BY category",
            success=True,
            data=[
                {"category": "electronics", "total": 15000.0},
                {"category": "clothing", "total": 7000.0},
            ],
            row_count=2,
            attempts=1,
        ),
    }


@pytest.fixture
def synthesis_response() -> AnalysisSynthesis:
    return AnalysisSynthesis(
        executive_summary=(
            "Total revenue for 2023 was $22,000. Electronics led all categories "
            "with $15,000 (68% of total). Revenue grew 20% from Jan to Feb."
        ),
        key_metrics=[
            KeyMetric(label="Total Revenue", value="$22,000"),
            KeyMetric(label="Top Category", value="Electronics", context="$15,000"),
        ],
        chart_specs=[
            ChartSpec(
                chart_id="monthly_revenue_trend",
                chart_type=ChartType.LINE,
                title="Monthly Revenue Trend",
                data_source="monthly_revenue",
                x_column="month",
                y_column="total",
            ),
            ChartSpec(
                chart_id="revenue_by_category",
                chart_type=ChartType.BAR,
                title="Revenue by Category",
                data_source="revenue_by_category",
                x_column="category",
                y_column="total",
            ),
        ],
        data_tables=[
            DataTableSpec(
                table_id="category_summary",
                title="Revenue by Category",
                data_source="revenue_by_category",
                max_rows=10,
            )
        ],
    )


def _make_base_mock_plan(plan: QueryPlan) -> MagicMock:
    base = MagicMock(spec=BaseAgent)
    base.call_structured.return_value = plan
    return base


def _make_base_mock_synthesis(synthesis: AnalysisSynthesis) -> MagicMock:
    base = MagicMock(spec=BaseAgent)
    base.call_structured.return_value = synthesis
    return base


# ---------------------------------------------------------------------------
# _build_plan_prompt — unit tests
# ---------------------------------------------------------------------------


class TestBuildPlanPrompt:
    def test_includes_business_question(self, profile: DataProfile) -> None:
        req = QueryPlanRequest(
            business_question="What is the monthly revenue trend?",
            data_profile=profile,
        )
        prompt = _build_plan_prompt(req)
        assert "monthly revenue trend" in prompt

    def test_includes_table_name(self, profile: DataProfile) -> None:
        req = QueryPlanRequest(
            business_question="How does revenue vary?",
            data_profile=profile,
        )
        assert "orders" in _build_plan_prompt(req)

    def test_includes_column_names(self, profile: DataProfile) -> None:
        req = QueryPlanRequest(
            business_question="Revenue analysis",
            data_profile=profile,
        )
        prompt = _build_plan_prompt(req)
        assert "revenue" in prompt
        assert "category" in prompt

    def test_includes_cardinality_labels(self, profile: DataProfile) -> None:
        req = QueryPlanRequest(
            business_question="Category analysis",
            data_profile=profile,
        )
        prompt = _build_plan_prompt(req)
        assert "cardinality=low" in prompt or "cardinality=high" in prompt

    def test_includes_suggested_grain(self, profile: DataProfile) -> None:
        req = QueryPlanRequest(
            business_question="Grain test",
            data_profile=profile,
        )
        assert "order_id" in _build_plan_prompt(req)

    def test_data_quality_issues_included_when_present(self) -> None:
        profile = DataProfile(
            tables=[
                TableProfile(
                    name="t",
                    row_count=10,
                    columns=[
                        ColumnProfile(
                            name="id",
                            dtype="INT",
                            null_count=0,
                            null_pct=0.0,
                            unique_count=10,
                            cardinality="low",
                            sample_values=[],
                            is_date=False,
                            is_numeric=True,
                        )
                    ],
                )
            ],
            relationships=[],
            suggested_grain="id",
            data_quality_issues=["High nulls on key column 'revenue'"],
        )
        req = QueryPlanRequest(business_question="Q", data_profile=profile)
        assert "High nulls" in _build_plan_prompt(req)


# ---------------------------------------------------------------------------
# _build_synthesis_prompt — unit tests
# ---------------------------------------------------------------------------


class TestBuildSynthesisPrompt:
    def test_includes_business_question(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        req = SynthesisRequest(
            business_question="What drives revenue?",
            query_plan=query_plan,
            query_results=query_results,
        )
        assert "drives revenue" in _build_synthesis_prompt(req)

    def test_includes_analysis_approach(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        req = SynthesisRequest(
            business_question="Q",
            query_plan=query_plan,
            query_results=query_results,
        )
        assert "revenue over time" in _build_synthesis_prompt(req)

    def test_includes_query_ids(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        req = SynthesisRequest(
            business_question="Q",
            query_plan=query_plan,
            query_results=query_results,
        )
        prompt = _build_synthesis_prompt(req)
        assert "monthly_revenue" in prompt
        assert "revenue_by_category" in prompt

    def test_includes_row_data(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        req = SynthesisRequest(
            business_question="Q",
            query_plan=query_plan,
            query_results=query_results,
        )
        prompt = _build_synthesis_prompt(req)
        assert "electronics" in prompt
        assert "10000" in prompt

    def test_failed_query_noted(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        query_results["monthly_revenue"] = QueryResult(
            query_id="monthly_revenue",
            sql="",
            success=False,
            error="Syntax error",
            attempts=3,
        )
        req = SynthesisRequest(
            business_question="Q",
            query_plan=query_plan,
            query_results=query_results,
        )
        prompt = _build_synthesis_prompt(req)
        assert "FAILED" in prompt
        assert "Syntax error" in prompt


# ---------------------------------------------------------------------------
# OrchestratorAgent.plan_queries
# ---------------------------------------------------------------------------


class TestPlanQueries:
    def test_returns_query_plan(
        self, profile: DataProfile, query_plan: QueryPlan
    ) -> None:
        base = _make_base_mock_plan(query_plan)
        agent = OrchestratorAgent(base=base)
        result = agent.plan_queries(
            QueryPlanRequest(
                business_question="Revenue analysis",
                data_profile=profile,
            )
        )
        assert isinstance(result, QueryPlan)

    def test_plan_has_queries(
        self, profile: DataProfile, query_plan: QueryPlan
    ) -> None:
        base = _make_base_mock_plan(query_plan)
        agent = OrchestratorAgent(base=base)
        result = agent.plan_queries(
            QueryPlanRequest(
                business_question="Revenue analysis",
                data_profile=profile,
            )
        )
        assert len(result.queries) == 2

    def test_calls_claude_once(
        self, profile: DataProfile, query_plan: QueryPlan
    ) -> None:
        base = _make_base_mock_plan(query_plan)
        agent = OrchestratorAgent(base=base)
        agent.plan_queries(
            QueryPlanRequest(
                business_question="Test",
                data_profile=profile,
            )
        )
        assert base.call_structured.call_count == 1

    def test_uses_plan_system_prompt(
        self, profile: DataProfile, query_plan: QueryPlan
    ) -> None:
        base = _make_base_mock_plan(query_plan)
        agent = OrchestratorAgent(base=base)
        agent.plan_queries(
            QueryPlanRequest(business_question="Q", data_profile=profile)
        )
        system_prompt: str = base.call_structured.call_args.args[0]
        assert "query" in system_prompt.lower()
        assert "business question" in system_prompt.lower()

    def test_agent_error_propagates(self, profile: DataProfile) -> None:
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = AgentError("API down")
        agent = OrchestratorAgent(base=base)
        with pytest.raises(AgentError, match="API down"):
            agent.plan_queries(
                QueryPlanRequest(business_question="Q", data_profile=profile)
            )


# ---------------------------------------------------------------------------
# OrchestratorAgent.synthesize
# ---------------------------------------------------------------------------


class TestSynthesize:
    def test_returns_analysis_synthesis(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
        synthesis_response: AnalysisSynthesis,
    ) -> None:
        base = _make_base_mock_synthesis(synthesis_response)
        agent = OrchestratorAgent(base=base)
        result = agent.synthesize(
            SynthesisRequest(
                business_question="Revenue analysis",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        assert isinstance(result, AnalysisSynthesis)

    def test_executive_summary_present(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
        synthesis_response: AnalysisSynthesis,
    ) -> None:
        base = _make_base_mock_synthesis(synthesis_response)
        agent = OrchestratorAgent(base=base)
        result = agent.synthesize(
            SynthesisRequest(
                business_question="Q",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        assert len(result.executive_summary) > 0

    def test_key_metrics_count(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
        synthesis_response: AnalysisSynthesis,
    ) -> None:
        base = _make_base_mock_synthesis(synthesis_response)
        agent = OrchestratorAgent(base=base)
        result = agent.synthesize(
            SynthesisRequest(
                business_question="Q",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        assert len(result.key_metrics) == 2

    def test_chart_specs_reference_valid_data_sources(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
        synthesis_response: AnalysisSynthesis,
    ) -> None:
        base = _make_base_mock_synthesis(synthesis_response)
        agent = OrchestratorAgent(base=base)
        result = agent.synthesize(
            SynthesisRequest(
                business_question="Q",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        valid_sources = set(query_results.keys())
        for spec in result.chart_specs:
            assert spec.data_source in valid_sources, (
                f"Chart '{spec.chart_id}' references unknown data_source "
                f"'{spec.data_source}'"
            )

    def test_calls_claude_once(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
        synthesis_response: AnalysisSynthesis,
    ) -> None:
        base = _make_base_mock_synthesis(synthesis_response)
        agent = OrchestratorAgent(base=base)
        agent.synthesize(
            SynthesisRequest(
                business_question="Q",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        assert base.call_structured.call_count == 1

    def test_agent_error_propagates(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = AgentError("timeout")
        agent = OrchestratorAgent(base=base)
        with pytest.raises(AgentError, match="timeout"):
            agent.synthesize(
                SynthesisRequest(
                    business_question="Q",
                    query_plan=query_plan,
                    query_results=query_results,
                )
            )


# ---------------------------------------------------------------------------
# Integration tests — real Claude API
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestOrchestratorIntegration:
    def test_plan_queries_with_real_api(self, profile: DataProfile) -> None:
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        agent = OrchestratorAgent(base=base)

        plan = agent.plan_queries(
            QueryPlanRequest(
                business_question=(
                    "What is the monthly revenue trend and which product "
                    "categories drive the most revenue?"
                ),
                data_profile=profile,
            )
        )
        assert isinstance(plan, QueryPlan)
        assert 1 <= len(plan.queries) <= 4
        for q in plan.queries:
            assert q.query_id
            assert q.purpose
            assert q.required_tables

    def test_synthesize_with_real_api(
        self,
        query_plan: QueryPlan,
        query_results: dict[str, QueryResult],
    ) -> None:
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        agent = OrchestratorAgent(base=base)

        synthesis = agent.synthesize(
            SynthesisRequest(
                business_question="What drives revenue?",
                query_plan=query_plan,
                query_results=query_results,
            )
        )
        assert isinstance(synthesis, AnalysisSynthesis)
        assert len(synthesis.executive_summary) > 20
