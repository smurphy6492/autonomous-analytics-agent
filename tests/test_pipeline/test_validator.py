"""Tests for analytics_agent.pipeline.validator."""

from __future__ import annotations

from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    TableProfile,
)
from analytics_agent.models.query_plan import PlannedQuery, QueryResult
from analytics_agent.pipeline.validator import (
    validate_chart_html,
    validate_join_fanout,
    validate_query_result,
    validate_summary_numbers,
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_result(
    query_id: str = "test_query",
    row_count: int = 5,
    data: list[dict] | None = None,
    success: bool = True,
) -> QueryResult:
    if data is None:
        data = [{"revenue": float(i * 100)} for i in range(row_count)]
    return QueryResult(
        query_id=query_id,
        sql="SELECT 1",
        success=success,
        data=data,
        row_count=row_count,
        attempts=1,
    )


def _make_spec(
    chart_id: str = "test_chart",
    chart_type: ChartType = ChartType.HORIZONTAL_BAR,
) -> ChartSpec:
    return ChartSpec(
        chart_id=chart_id,
        chart_type=chart_type,
        title="Test Chart",
        data_source="test_query",
        x_column="revenue",
        y_column="category",
        show_legend=False,
    )


# ------------------------------------------------------------------
# validate_query_result
# ------------------------------------------------------------------


class TestValidateQueryResult:
    def test_zero_rows_warns(self) -> None:
        result = _make_result(row_count=0, data=[])
        warnings = validate_query_result(result)
        assert any("0 rows" in w for w in warnings)

    def test_zero_rows_includes_query_id(self) -> None:
        result = _make_result(query_id="monthly_trend", row_count=0, data=[])
        warnings = validate_query_result(result)
        assert any("monthly_trend" in w for w in warnings)

    def test_healthy_result_no_warnings(self) -> None:
        data = [
            {"category": "A", "revenue": 1200000.0},
            {"category": "B", "revenue": 950000.0},
            {"category": "C", "revenue": 800000.0},
            {"category": "D", "revenue": 600000.0},
            {"category": "E", "revenue": 450000.0},
        ]
        result = _make_result(row_count=5, data=data)
        assert validate_query_result(result) == []

    def test_zero_variance_column_warns(self) -> None:
        data = [{"revenue": 5.0, "category": "A"} for _ in range(6)]
        result = _make_result(row_count=6, data=data)
        warnings = validate_query_result(result)
        assert any("zero variance" in w and "revenue" in w for w in warnings)

    def test_sequential_index_column_warns(self) -> None:
        # Values exactly equal to row indices — classic bdata-gone-wrong symptom.
        data = [{"revenue": float(i), "category": f"cat_{i}"} for i in range(10)]
        result = _make_result(row_count=10, data=data)
        warnings = validate_query_result(result)
        assert any("sequential" in w and "revenue" in w for w in warnings)

    def test_monotonic_real_data_not_flagged(self) -> None:
        # Monotonically increasing revenue is valid (sorted result set).
        data = [
            {"revenue": 100.0},
            {"revenue": 200.0},
            {"revenue": 450.0},
            {"revenue": 900.0},
            {"revenue": 1800.0},
        ]
        result = _make_result(row_count=5, data=data)
        # High correlation here, but not ≥ 0.99 because spacing is geometric.
        # This test ensures we don't false-positive on legitimate sorted data.
        warnings = validate_query_result(result)
        sequential_warns = [w for w in warnings if "sequential" in w]
        assert len(sequential_warns) == 0

    def test_none_data_no_crash(self) -> None:
        result = QueryResult(
            query_id="q",
            sql="SELECT 1",
            success=True,
            data=None,
            row_count=0,
            attempts=1,
        )
        # Should not raise.
        warnings = validate_query_result(result)
        assert isinstance(warnings, list)

    def test_bool_columns_not_flagged_as_sequential(self) -> None:
        # bool is a subclass of int — [False, True, False, True] should not warn.
        data = [
            {"active": b, "revenue": float(i * 50)}
            for i, b in enumerate([False, True, False, True, False])
        ]
        result = _make_result(row_count=5, data=data)
        warnings = validate_query_result(result)
        # "active" column should not trigger sequential-index warning.
        assert not any("active" in w and "sequential" in w for w in warnings)

    def test_single_row_no_crash(self) -> None:
        result = _make_result(row_count=1, data=[{"revenue": 100.0}])
        assert validate_query_result(result) == []


# ------------------------------------------------------------------
# validate_chart_html
# ------------------------------------------------------------------


class TestValidateChartHtml:
    """Only the bdata serialization backstop lives here now; axis-mapping checks
    moved to renderer.figure_axis_warnings (tested in test_viz_agent)."""

    def test_bdata_present_warns(self) -> None:
        html = (
            '<script>Plotly.newPlot("c",[{"x":{"dtype":"f8","bdata":"AAAA"},'
            '"y":["A","B"],"type":"bar"}],{});</script>'
        )
        spec = _make_spec()
        warnings = validate_chart_html(html, spec)
        assert any("bdata" in w for w in warnings)

    def test_bdata_present_includes_chart_id(self) -> None:
        html = '<div>{"bdata":"AAAA"}</div>'
        spec = _make_spec(chart_id="revenue_bar")
        warnings = validate_chart_html(html, spec)
        assert any("revenue_bar" in w for w in warnings)

    def test_clean_html_no_warning(self) -> None:
        html = (
            '<div id="test"></div><script>Plotly.newPlot("test",'
            '[{"x":[1233131.72,1166176.98],"y":["health_beauty","watches_gifts"],'
            '"type":"bar","orientation":"h"}],{});</script>'
        )
        spec = _make_spec()
        assert validate_chart_html(html, spec) == []

    def test_empty_html_no_crash(self) -> None:
        spec = _make_spec()
        assert validate_chart_html("", spec) == []


# ------------------------------------------------------------------
# validate_join_fanout
# ------------------------------------------------------------------


def _make_profile(row_counts: dict[str, int]) -> DataProfile:
    """Build a minimal DataProfile with the given table row counts."""
    tables = [
        TableProfile(
            name=name,
            row_count=count,
            columns=[
                ColumnProfile(
                    name="id",
                    dtype="int64",
                    null_count=0,
                    null_pct=0.0,
                    unique_count=count,
                    cardinality="high",
                )
            ],
        )
        for name, count in row_counts.items()
    ]
    return DataProfile(tables=tables, suggested_grain="id")


def _make_planned(tables: list[str]) -> PlannedQuery:
    return PlannedQuery(
        query_id="breakdown",
        purpose="revenue by category",
        required_tables=tables,
        required_columns=["category", "revenue"],
        aggregation_grain="by_category",
        expected_output_type="breakdown",
    )


class TestValidateJoinFanout:
    def test_single_table_never_flags(self) -> None:
        # Even a huge result from one table cannot be join fan-out.
        result = _make_result(query_id="breakdown", row_count=999999)
        planned = _make_planned(["orders"])
        profile = _make_profile({"orders": 100})
        assert validate_join_fanout(result, planned, profile) == []

    def test_grouped_result_within_source_size_no_warning(self) -> None:
        # 8 category rows from a join of tables with 100k / 32k rows: fine.
        result = _make_result(query_id="breakdown", row_count=8)
        planned = _make_planned(["order_items", "orders"])
        profile = _make_profile({"order_items": 112650, "orders": 99441})
        assert validate_join_fanout(result, planned, profile) == []

    def test_result_larger_than_largest_source_warns(self) -> None:
        # A grouped query returning more rows than any base table = fan-out.
        result = _make_result(query_id="breakdown", row_count=250000)
        planned = _make_planned(["order_items", "payments"])
        profile = _make_profile({"order_items": 112650, "payments": 103886})
        warnings = validate_join_fanout(result, planned, profile)
        assert any("fanned" in w and "breakdown" in w for w in warnings)

    def test_failed_result_skipped(self) -> None:
        result = _make_result(query_id="breakdown", row_count=0, success=False)
        planned = _make_planned(["a", "b"])
        profile = _make_profile({"a": 10, "b": 10})
        assert validate_join_fanout(result, planned, profile) == []

    def test_missing_profile_tables_no_crash(self) -> None:
        result = _make_result(query_id="breakdown", row_count=500)
        planned = _make_planned(["unknown1", "unknown2"])
        profile = _make_profile({"other": 100})
        assert validate_join_fanout(result, planned, profile) == []


# ------------------------------------------------------------------
# validate_summary_numbers
# ------------------------------------------------------------------


def _results_with_values(values: list[dict]) -> dict[str, QueryResult]:
    return {
        "q1": QueryResult(
            query_id="q1",
            sql="SELECT 1",
            success=True,
            data=values,
            row_count=len(values),
            attempts=1,
        )
    }


class TestValidateSummaryNumbers:
    def test_supported_currency_figure_no_warning(self) -> None:
        # "$1.23M" must match a raw 1,233,131.72 within tolerance.
        results = _results_with_values(
            [{"category": "health_beauty", "revenue": 1233131.72}]
        )
        summary = "Health & Beauty leads with $1.23M in revenue."
        assert validate_summary_numbers(summary, results) == []

    def test_supported_percentage_no_warning(self) -> None:
        results = _results_with_values([{"share": 11.69}, {"share": 8.53}])
        summary = "Share rose from 8.53% to 11.69% over the period."
        assert validate_summary_numbers(summary, results) == []

    def test_fabricated_figure_warns(self) -> None:
        results = _results_with_values([{"revenue": 1233131.72}])
        summary = "Revenue reached $9.99M, an all-time high."
        warnings = validate_summary_numbers(summary, results)
        assert any("9.99M" in w for w in warnings)

    def test_small_structural_integers_ignored(self) -> None:
        # "top 5" and "12 months" are language, not data — must not warn.
        results = _results_with_values([{"revenue": 1233131.72}])
        summary = "The top 5 categories over the past 12 months drove growth."
        assert validate_summary_numbers(summary, results) == []

    def test_large_plain_number_checked(self) -> None:
        # "9,465 items" is a headline figure and IS supported.
        results = _results_with_values([{"items": 9465, "revenue": 1233131.72}])
        summary = "Health & Beauty sold 9,465 items."
        assert validate_summary_numbers(summary, results) == []

    def test_empty_summary_no_warning(self) -> None:
        results = _results_with_values([{"revenue": 100.0}])
        assert validate_summary_numbers("", results) == []
