"""Tests for analytics_agent.pipeline.validator."""

from __future__ import annotations

import pytest

from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.query_plan import QueryResult
from analytics_agent.pipeline.validator import validate_chart_html, validate_query_result


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
        data = [{"active": b, "revenue": float(i * 50)} for i, b in enumerate([False, True, False, True, False])]
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
    _CLEAN_HTML = (
        '<div id="test"></div><script>Plotly.newPlot("test",'
        '[{"x":[1233131.72,1166176.98,1023434.76],"y":["health_beauty","watches_gifts","bed_bath"],'
        '"type":"bar","orientation":"h"}],{});</script>'
    )

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

    def test_clean_html_no_bdata_warning(self) -> None:
        spec = _make_spec()
        warnings = validate_chart_html(self._CLEAN_HTML, spec)
        assert not any("bdata" in w for w in warnings)

    def test_healthy_x_values_no_sequential_warning(self) -> None:
        spec = _make_spec()
        warnings = validate_chart_html(self._CLEAN_HTML, spec)
        assert not any("sequential" in w for w in warnings)

    def test_sequential_x_axis_warns(self) -> None:
        # x=[0,1,2,3,4,5,6,7,8,9] — classic row-index symptom.
        html = (
            '<script>Plotly.newPlot("c",[{"x":[0,1,2,3,4,5,6,7,8,9],'
            '"y":["A","B","C","D","E","F","G","H","I","J"],'
            '"type":"bar","orientation":"h"}],{});</script>'
        )
        spec = _make_spec()
        warnings = validate_chart_html(html, spec)
        assert any("sequential" in w and "'x'" in w for w in warnings)

    def test_sequential_y_axis_warns(self) -> None:
        html = (
            '<script>Plotly.newPlot("c",[{"y":[0,1,2,3,4,5,6,7,8,9],'
            '"x":["A","B","C","D","E","F","G","H","I","J"],'
            '"type":"bar"}],{});</script>'
        )
        spec = _make_spec(chart_type=ChartType.BAR)
        warnings = validate_chart_html(html, spec)
        assert any("sequential" in w and "'y'" in w for w in warnings)

    def test_identical_values_warns(self) -> None:
        html = (
            '<script>Plotly.newPlot("c",[{"x":[5,5,5,5,5,5],'
            '"y":["A","B","C","D","E","F"],"type":"bar"}],{});</script>'
        )
        spec = _make_spec()
        warnings = validate_chart_html(html, spec)
        assert any("identical" in w for w in warnings)

    def test_empty_html_no_crash(self) -> None:
        spec = _make_spec()
        assert validate_chart_html("", spec) == []

    def test_real_revenue_values_no_warning(self) -> None:
        # Representative values from the Olist dataset.
        html = (
            '<script>Plotly.newPlot("c",[{"x":[1233131.72,1166176.98,1023434.76,'
            '954852.55,888724.61,711927.69,615628.69,610204.1,578966.65,471286.48],'
            '"y":["health_beauty","watches_gifts","bed_bath_table","sports_leisure",'
            '"computers_accessories","furniture_decor","housewares","cool_stuff",'
            '"auto","toys"],"type":"bar","orientation":"h"}],{});</script>'
        )
        spec = _make_spec()
        warnings = validate_chart_html(html, spec)
        assert warnings == []
