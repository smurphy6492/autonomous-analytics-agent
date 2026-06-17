"""Cross-agent handoff invariant tests.

The pipeline relies on two key references lining up across agents:

1. Every ChartSpec.data_source (produced by the Orchestrator's synthesis call)
   must name a query_id that exists in the query_results dict (produced by the
   SQL Analyst).
2. The query_results keys must be exactly the query_ids from the QueryPlan
   (produced by the Orchestrator's planning call). The runner enforces this by
   keying each result on ``planned.query_id`` (runner.py:201).

These tests pin both invariants so a future change that breaks the handoff
(e.g. keying results on something other than the plan's query_id) fails loudly.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from analytics_agent.config import Settings
from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.profile import DataProfile, TableProfile
from analytics_agent.models.query_plan import (
    PlannedQuery,
    QueryPlan,
    QueryResult,
    SQLRequest,
)
from analytics_agent.models.report import AnalysisSynthesis
from analytics_agent.pipeline.context import PipelineContext
from analytics_agent.pipeline.runner import PipelineRunner


def _make_runner() -> PipelineRunner:
    settings = MagicMock(spec=Settings)
    settings.anthropic_api_key = "sk-test"
    settings.model = "claude-sonnet-4-6"
    settings.cache_dir = None
    with patch("analytics_agent.pipeline.runner.anthropic.Anthropic"):
        return PipelineRunner(settings=settings)


def _minimal_profile() -> DataProfile:
    return DataProfile(
        tables=[TableProfile(name="orders", row_count=10, columns=[])],
        suggested_grain="order_id",
    )


def _planned(query_id: str) -> PlannedQuery:
    return PlannedQuery(
        query_id=query_id,
        purpose=f"purpose for {query_id}",
        required_tables=["orders"],
        required_columns=["revenue"],
        aggregation_grain="monthly",
        expected_output_type="summary_table",
    )


# ---------------------------------------------------------------------------
# Invariant 2 — query_results keys must match the plan's query_ids
# ---------------------------------------------------------------------------


class TestResultKeysMatchPlan:
    def test_step_execute_keys_results_by_plan_query_ids(self) -> None:
        runner = _make_runner()
        # Replace the SQL analyst with a stub that echoes the planned query_id.
        runner._sql_analyst = MagicMock()
        runner._sql_analyst.execute_query.side_effect = lambda req: QueryResult(
            query_id=req.planned_query.query_id,
            sql="SELECT 1",
            success=True,
            data=[],
            row_count=0,
            attempts=1,
        )

        plan = QueryPlan(
            analysis_approach="Two analyses.",
            queries=[_planned("monthly_revenue"), _planned("revenue_by_category")],
        )
        ctx = PipelineContext(
            data_paths=[],
            table_names=[],
            business_question="q",
            output_path=Path("output/test.html"),
        )
        ctx.profile = _minimal_profile()
        ctx.query_plan = plan

        runner._step_execute(ctx)

        assert set(ctx.query_results.keys()) == {q.query_id for q in plan.queries}

    def test_step_execute_builds_request_per_planned_query(self) -> None:
        runner = _make_runner()
        runner._sql_analyst = MagicMock()
        runner._sql_analyst.execute_query.side_effect = lambda req: QueryResult(
            query_id=req.planned_query.query_id,
            sql="SELECT 1",
            success=True,
            attempts=1,
        )
        plan = QueryPlan(
            analysis_approach="One analysis.", queries=[_planned("only_q")]
        )
        ctx = PipelineContext(
            data_paths=[],
            table_names=[],
            business_question="q",
            output_path=Path("output/test.html"),
        )
        ctx.profile = _minimal_profile()
        ctx.query_plan = plan

        runner._step_execute(ctx)

        # Every call received a SQLRequest carrying a planned query from the plan.
        plan_ids = {q.query_id for q in plan.queries}
        for call in runner._sql_analyst.execute_query.call_args_list:
            request = call.args[0]
            assert isinstance(request, SQLRequest)
            assert request.planned_query.query_id in plan_ids


# ---------------------------------------------------------------------------
# Invariant 1 — every chart data_source must resolve to a query result
# ---------------------------------------------------------------------------


def _coherent_results() -> dict[str, QueryResult]:
    return {
        "monthly_revenue": QueryResult(
            query_id="monthly_revenue",
            sql="SELECT month, SUM(revenue) AS total FROM orders GROUP BY month",
            success=True,
            data=[{"month": "2023-01", "total": 10000.0}],
            row_count=1,
            attempts=1,
        ),
        "revenue_by_category": QueryResult(
            query_id="revenue_by_category",
            sql="SELECT category, SUM(revenue) AS total FROM orders GROUP BY category",
            success=True,
            data=[{"category": "electronics", "total": 15000.0}],
            row_count=1,
            attempts=1,
        ),
    }


class TestChartDataSourcesResolve:
    def test_all_chart_data_sources_resolve_to_results(self) -> None:
        query_results = _coherent_results()
        synthesis = AnalysisSynthesis(
            executive_summary="Revenue analysis.",
            chart_specs=[
                ChartSpec(
                    chart_id="trend",
                    chart_type=ChartType.LINE,
                    title="Monthly Revenue",
                    data_source="monthly_revenue",
                    x_column="month",
                    y_column="total",
                ),
                ChartSpec(
                    chart_id="by_cat",
                    chart_type=ChartType.BAR,
                    title="Revenue by Category",
                    data_source="revenue_by_category",
                    x_column="category",
                    y_column="total",
                ),
            ],
        )

        unresolved = [
            spec.data_source
            for spec in synthesis.chart_specs
            if spec.data_source not in query_results
        ]
        assert unresolved == []

    def test_orphan_chart_data_source_is_detectable(self) -> None:
        query_results = _coherent_results()
        synthesis = AnalysisSynthesis(
            executive_summary="Revenue analysis with an orphan chart.",
            chart_specs=[
                ChartSpec(
                    chart_id="orphan",
                    chart_type=ChartType.BAR,
                    title="No backing query",
                    data_source="does_not_exist",
                    x_column="category",
                    y_column="total",
                ),
            ],
        )

        unresolved = {
            spec.data_source
            for spec in synthesis.chart_specs
            if spec.data_source not in query_results
        }
        assert unresolved == {"does_not_exist"}

    def test_plan_results_and_charts_line_up_end_to_end(self) -> None:
        # A consistent plan → results → synthesis chain: result keys equal the
        # plan's query_ids, and every chart resolves to one of those results.
        plan = QueryPlan(
            analysis_approach="Revenue over time and by category.",
            queries=[_planned("monthly_revenue"), _planned("revenue_by_category")],
        )
        query_results = _coherent_results()
        synthesis = AnalysisSynthesis(
            executive_summary="Revenue analysis.",
            chart_specs=[
                ChartSpec(
                    chart_id="trend",
                    chart_type=ChartType.LINE,
                    title="Monthly Revenue",
                    data_source="monthly_revenue",
                    x_column="month",
                    y_column="total",
                ),
            ],
        )

        assert set(query_results.keys()) == {q.query_id for q in plan.queries}
        for spec in synthesis.chart_specs:
            assert spec.data_source in query_results
