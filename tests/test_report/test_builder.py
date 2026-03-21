"""Unit tests for ReportBuilder."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from analytics_agent.models.query_plan import PlannedQuery, QueryPlan, QueryResult
from analytics_agent.models.report import (
    AnalysisReport,
    DataTableSpec,
    KeyMetric,
    RenderedChart,
)
from analytics_agent.report.builder import (
    ReportBuilder,
    _format_duration,
    _rows_to_html,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_query_plan() -> QueryPlan:
    return QueryPlan(
        analysis_approach="Analyse revenue by category and over time.",
        queries=[
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


def _make_query_results() -> dict[str, QueryResult]:
    return {
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


def _make_report(
    *,
    charts: list[RenderedChart] | None = None,
    data_tables: list[DataTableSpec] | None = None,
    errors: list[str] | None = None,
) -> AnalysisReport:
    return AnalysisReport(
        title="Test Report",
        business_question="What is the revenue breakdown by category?",
        generated_at=datetime(2024, 1, 15, 12, 0, 0),
        executive_summary=(
            "Electronics led all categories with $15,000 in total revenue."
        ),
        key_metrics=[
            KeyMetric(label="Total Revenue", value="$22,000"),
            KeyMetric(label="Top Category", value="Electronics", context="$15K"),
        ],
        rendered_charts=charts or [],
        data_tables=data_tables or [],
        query_plan=_make_query_plan(),
        query_results=_make_query_results(),
        data_sources=["orders"],
        analysis_approach="Analyse revenue by category and over time.",
        execution_time_ms=5200,
        errors=errors or [],
    )


# ---------------------------------------------------------------------------
# ReportBuilder.render — structural tests
# ---------------------------------------------------------------------------


class TestReportBuilderRender:
    def test_returns_string(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert isinstance(html, str)

    def test_is_valid_html_start(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html

    def test_contains_report_title(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "Test Report" in html

    def test_contains_business_question(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "revenue breakdown by category" in html

    def test_contains_executive_summary(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "Electronics led all categories" in html

    def test_contains_key_metric_value(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "$22,000" in html

    def test_contains_key_metric_label(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "Total Revenue" in html

    def test_contains_data_source(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "orders" in html

    def test_contains_analysis_approach(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "Analyse revenue" in html

    def test_contains_sql_query(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "SELECT category" in html

    def test_contains_generated_date(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "2024-01-15" in html

    def test_contains_execution_time(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        # 5200ms = 5.2s
        assert "5.2s" in html

    def test_plotly_js_included(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report())
        assert "plotly" in html.lower()

    def test_no_errors_section_when_none(self) -> None:
        builder = ReportBuilder()
        html = builder.render(_make_report(errors=[]))
        assert "Pipeline warnings" not in html

    def test_errors_section_shown_when_present(self) -> None:
        builder = ReportBuilder()
        report = _make_report(errors=["Query 'q1' failed after 3 attempts"])
        html = builder.render(report)
        assert "Pipeline warnings" in html
        assert "Query 'q1' failed" in html


# ---------------------------------------------------------------------------
# Charts rendering
# ---------------------------------------------------------------------------


class TestReportBuilderCharts:
    def test_successful_chart_html_embedded(self) -> None:
        chart = RenderedChart(
            chart_id="test_chart",
            title="Test Chart",
            html='<div id="test_chart">CHART_CONTENT</div>',
            success=True,
        )
        builder = ReportBuilder()
        html = builder.render(_make_report(charts=[chart]))
        assert "CHART_CONTENT" in html

    def test_failed_chart_not_embedded(self) -> None:
        chart = RenderedChart(
            chart_id="broken_chart",
            title="Broken Chart",
            html="",
            success=False,
            error="Missing column",
        )
        builder = ReportBuilder()
        html = builder.render(_make_report(charts=[chart]))
        assert "CHART_CONTENT" not in html

    def test_chart_title_shown(self) -> None:
        chart = RenderedChart(
            chart_id="t",
            title="My Amazing Chart",
            html='<div id="t">data</div>',
            success=True,
        )
        builder = ReportBuilder()
        html = builder.render(_make_report(charts=[chart]))
        assert "My Amazing Chart" in html

    def test_multiple_charts_all_embedded(self) -> None:
        charts = [
            RenderedChart(
                chart_id=f"chart_{i}",
                title=f"Chart {i}",
                html=f'<div id="chart_{i}">CONTENT_{i}</div>',
                success=True,
            )
            for i in range(3)
        ]
        builder = ReportBuilder()
        html = builder.render(_make_report(charts=charts))
        for i in range(3):
            assert f"CONTENT_{i}" in html


# ---------------------------------------------------------------------------
# Data tables rendering
# ---------------------------------------------------------------------------


class TestReportBuilderDataTables:
    def test_data_table_rendered(self) -> None:
        spec = DataTableSpec(
            table_id="cat_table",
            title="Revenue by Category",
            data_source="revenue_by_category",
            max_rows=10,
        )
        builder = ReportBuilder()
        html = builder.render(_make_report(data_tables=[spec]))
        assert "Revenue by Category" in html
        assert "electronics" in html

    def test_data_table_respects_max_rows(self) -> None:
        # Add a result with many rows but cap at 1.
        spec = DataTableSpec(
            table_id="cat_table",
            title="Revenue",
            data_source="revenue_by_category",
            max_rows=1,
        )
        builder = ReportBuilder()
        html = builder.render(_make_report(data_tables=[spec]))
        # Only 1 row should appear; "clothing" is the 2nd row, should not be there.
        assert "electronics" in html
        assert "clothing" not in html

    def test_skips_table_for_failed_query(self) -> None:
        results = _make_query_results()
        results["revenue_by_category"] = QueryResult(
            query_id="revenue_by_category",
            sql="SELECT 1",
            success=False,
            error="DuckDB error",
            attempts=3,
        )
        report = AnalysisReport(
            title="T",
            business_question="Q",
            generated_at=datetime(2024, 1, 1),
            executive_summary="Summary.",
            key_metrics=[],
            rendered_charts=[],
            data_tables=[
                DataTableSpec(
                    table_id="cat",
                    title="Revenue",
                    data_source="revenue_by_category",
                )
            ],
            query_plan=_make_query_plan(),
            query_results=results,
            data_sources=["orders"],
            analysis_approach="Approach.",
        )
        builder = ReportBuilder()
        html = builder.render(report)
        # Table should not be rendered when query failed.
        assert "electronics" not in html


# ---------------------------------------------------------------------------
# ReportBuilder.write — file output
# ---------------------------------------------------------------------------


class TestReportBuilderWrite:
    def test_write_creates_file(self, tmp_path: Path) -> None:
        output = tmp_path / "report.html"
        builder = ReportBuilder()
        builder.write(_make_report(), output)
        assert output.exists()

    def test_written_file_is_html(self, tmp_path: Path) -> None:
        output = tmp_path / "report.html"
        builder = ReportBuilder()
        builder.write(_make_report(), output)
        content = output.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content

    def test_write_creates_parent_dirs(self, tmp_path: Path) -> None:
        output = tmp_path / "nested" / "deep" / "report.html"
        builder = ReportBuilder()
        builder.write(_make_report(), output)
        assert output.exists()


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_milliseconds(self) -> None:
        assert _format_duration(500) == "500ms"

    def test_seconds(self) -> None:
        assert _format_duration(5200) == "5.2s"

    def test_minutes(self) -> None:
        assert _format_duration(90000) == "1m 30s"

    def test_exactly_one_second(self) -> None:
        assert _format_duration(1000) == "1.0s"


class TestRowsToHtml:
    def test_returns_table_tag(self) -> None:
        spec = DataTableSpec(table_id="t", title="T", data_source="q")
        rows = [{"a": "1", "b": "2"}]
        html = _rows_to_html(rows, spec)
        assert "<table" in html

    def test_header_from_row_keys(self) -> None:
        spec = DataTableSpec(table_id="t", title="T", data_source="q")
        rows = [{"category": "electronics", "revenue": 100}]
        html = _rows_to_html(rows, spec)
        assert "<th>category</th>" in html
        assert "<th>revenue</th>" in html

    def test_header_filtered_by_columns(self) -> None:
        spec = DataTableSpec(
            table_id="t", title="T", data_source="q", columns=["category"]
        )
        rows = [{"category": "electronics", "revenue": 100}]
        html = _rows_to_html(rows, spec)
        assert "category" in html
        assert "revenue" not in html

    def test_empty_rows_returns_placeholder(self) -> None:
        spec = DataTableSpec(table_id="t", title="T", data_source="q")
        html = _rows_to_html([], spec)
        assert "No data" in html

    def test_html_escaping(self) -> None:
        spec = DataTableSpec(table_id="t", title="T", data_source="q")
        rows = [{"name": "<script>alert('xss')</script>"}]
        html = _rows_to_html(rows, spec)
        assert "<script>" not in html
        assert "&lt;script&gt;" in html
