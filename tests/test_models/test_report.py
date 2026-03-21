"""Unit tests for AnalysisSynthesis, AnalysisReport, and related report models."""

from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.query_plan import PlannedQuery, QueryPlan, QueryResult
from analytics_agent.models.report import (
    AgentCallLog,
    AnalysisReport,
    AnalysisSynthesis,
    KeyMetric,
    RenderedChart,
)


def make_query_plan() -> QueryPlan:
    return QueryPlan(
        analysis_approach="Analyse by category.",
        queries=[
            PlannedQuery(
                query_id="q1",
                purpose="Revenue by category",
                required_tables=["orders"],
                required_columns=["category", "revenue"],
                aggregation_grain="by_category",
                expected_output_type="breakdown",
            )
        ],
    )


class TestAnalysisSynthesis:
    def test_valid_minimal(self) -> None:
        s = AnalysisSynthesis(
            executive_summary="Electronics drives 55% of revenue.",
        )
        assert s.key_metrics == []
        assert s.chart_specs == []
        assert s.data_tables == []

    def test_with_metrics_and_charts(self) -> None:
        s = AnalysisSynthesis(
            executive_summary="Revenue grew 12% YoY.",
            key_metrics=[KeyMetric(label="Total Revenue", value="$1.2M")],
            chart_specs=[
                ChartSpec(
                    chart_id="rev",
                    chart_type=ChartType.BAR,
                    title="Revenue",
                    data_source="q1",
                )
            ],
        )
        assert len(s.key_metrics) == 1
        assert len(s.chart_specs) == 1

    def test_key_metric_with_context(self) -> None:
        m = KeyMetric(label="AOV", value="$85", context="+5% vs prior month")
        assert m.context == "+5% vs prior month"


class TestRenderedChart:
    def test_success(self) -> None:
        chart = RenderedChart(
            chart_id="rev",
            title="Revenue",
            html="<div>...</div>",
            success=True,
        )
        assert chart.error is None

    def test_failure(self) -> None:
        chart = RenderedChart(
            chart_id="rev",
            title="Revenue",
            html="",
            success=False,
            error="Column missing",
        )
        assert chart.success is False


class TestAnalysisReport:
    def test_valid_report(self) -> None:
        qp = make_query_plan()
        report = AnalysisReport(
            title="E-commerce Analysis",
            business_question="What drives revenue?",
            executive_summary="Electronics is the top category.",
            key_metrics=[KeyMetric(label="Revenue", value="$1M")],
            rendered_charts=[],
            data_tables=[],
            query_plan=qp,
            query_results={
                "q1": QueryResult(
                    query_id="q1",
                    sql="SELECT 1",
                    success=True,
                    data=[{"category": "electronics", "revenue": 1000}],
                    row_count=1,
                )
            },
            data_sources=["orders"],
            analysis_approach="Group by category.",
        )
        assert report.title == "E-commerce Analysis"
        assert report.errors == []
        assert report.agent_calls == []
        assert isinstance(report.generated_at.year, int)

    def test_agent_call_log(self) -> None:
        log = AgentCallLog(
            agent_name="orchestrator",
            call_type="plan_queries",
            model="claude-sonnet-4-6",
            input_tokens=500,
            output_tokens=200,
            duration_ms=1200,
        )
        assert log.success is True
        assert log.error is None
