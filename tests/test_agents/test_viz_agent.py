"""Unit tests for VizAgent and the renderer module."""

from __future__ import annotations

from typing import Any

import pytest

from analytics_agent.agents.viz_agent import VizAgent
from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.report import RenderedChart
from analytics_agent.viz.renderer import RenderError, render_chart

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------


def _time_series_data() -> list[dict[str, Any]]:
    return [
        {"month": "2023-01", "total": 10000.0},
        {"month": "2023-02", "total": 12000.0},
        {"month": "2023-03", "total": 9500.0},
    ]


def _category_data() -> list[dict[str, Any]]:
    return [
        {"category": "electronics", "revenue": 15000.0},
        {"category": "clothing", "revenue": 7000.0},
        {"category": "furniture", "revenue": 4500.0},
    ]


def _pie_data() -> list[dict[str, Any]]:
    return [
        {"payment_type": "credit_card", "count": 450},
        {"payment_type": "boleto", "count": 300},
        {"payment_type": "debit_card", "count": 150},
    ]


def _scatter_data() -> list[dict[str, Any]]:
    return [
        {"price": 100.0, "freight": 10.0, "category": "electronics"},
        {"price": 50.0, "freight": 5.0, "category": "clothing"},
        {"price": 200.0, "freight": 20.0, "category": "furniture"},
    ]


def _heatmap_data() -> list[dict[str, Any]]:
    return [
        {"state": "CA", "month": "2023-01", "orders": 100},
        {"state": "CA", "month": "2023-02", "orders": 120},
        {"state": "NY", "month": "2023-01", "orders": 80},
        {"state": "NY", "month": "2023-02", "orders": 90},
    ]


# ---------------------------------------------------------------------------
# render_chart — line chart
# ---------------------------------------------------------------------------


class TestRenderLineChart:
    def test_returns_html_string(self) -> None:
        spec = ChartSpec(
            chart_id="test_line",
            chart_type=ChartType.LINE,
            title="Revenue Over Time",
            data_source="monthly_revenue",
            x_column="month",
            y_column="total",
        )
        html = render_chart(spec, _time_series_data())
        assert isinstance(html, str)
        assert len(html) > 0

    def test_contains_plotly_div(self) -> None:
        spec = ChartSpec(
            chart_id="test_line",
            chart_type=ChartType.LINE,
            title="Revenue",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        html = render_chart(spec, _time_series_data())
        assert "<div" in html

    def test_div_id_matches_chart_id(self) -> None:
        spec = ChartSpec(
            chart_id="my_line_chart",
            chart_type=ChartType.LINE,
            title="Revenue",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        html = render_chart(spec, _time_series_data())
        assert "my_line_chart" in html

    def test_no_full_html_boilerplate(self) -> None:
        """to_html(full_html=False) should not include <html> tags."""
        spec = ChartSpec(
            chart_id="test_line",
            chart_type=ChartType.LINE,
            title="Revenue",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        html = render_chart(spec, _time_series_data())
        assert "<html" not in html.lower()


# ---------------------------------------------------------------------------
# render_chart — bar chart
# ---------------------------------------------------------------------------


class TestRenderBarChart:
    def test_bar_returns_html(self) -> None:
        spec = ChartSpec(
            chart_id="test_bar",
            chart_type=ChartType.BAR,
            title="Revenue by Category",
            data_source="q",
            x_column="category",
            y_column="revenue",
        )
        html = render_chart(spec, _category_data())
        assert "<div" in html

    def test_horizontal_bar_returns_html(self) -> None:
        spec = ChartSpec(
            chart_id="test_hbar",
            chart_type=ChartType.HORIZONTAL_BAR,
            title="Revenue by Category (H)",
            data_source="q",
            x_column="category",
            y_column="revenue",
        )
        html = render_chart(spec, _category_data())
        assert "<div" in html


# ---------------------------------------------------------------------------
# render_chart — pie chart
# ---------------------------------------------------------------------------


class TestRenderPieChart:
    def test_pie_returns_html(self) -> None:
        spec = ChartSpec(
            chart_id="test_pie",
            chart_type=ChartType.PIE,
            title="Payment Method Distribution",
            data_source="q",
            values_column="count",
            names_column="payment_type",
        )
        html = render_chart(spec, _pie_data())
        assert "<div" in html


# ---------------------------------------------------------------------------
# render_chart — scatter chart
# ---------------------------------------------------------------------------


class TestRenderScatterChart:
    def test_scatter_returns_html(self) -> None:
        spec = ChartSpec(
            chart_id="test_scatter",
            chart_type=ChartType.SCATTER,
            title="Price vs Freight",
            data_source="q",
            x_column="price",
            y_column="freight",
            color_column="category",
        )
        html = render_chart(spec, _scatter_data())
        assert "<div" in html


# ---------------------------------------------------------------------------
# render_chart — heatmap
# ---------------------------------------------------------------------------


class TestRenderHeatmap:
    def test_heatmap_returns_html(self) -> None:
        spec = ChartSpec(
            chart_id="test_heatmap",
            chart_type=ChartType.HEATMAP,
            title="Orders by State and Month",
            data_source="q",
            x_column="month",
            y_column="state",
            color_column="orders",
        )
        html = render_chart(spec, _heatmap_data())
        assert "<div" in html


# ---------------------------------------------------------------------------
# render_chart — error cases
# ---------------------------------------------------------------------------


class TestRenderErrors:
    def test_empty_data_raises_render_error(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        with pytest.raises(RenderError, match="empty"):
            render_chart(spec, [])

    def test_missing_x_column_raises_render_error(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="nonexistent_col",
            y_column="total",
        )
        with pytest.raises(RenderError, match="nonexistent_col"):
            render_chart(spec, _time_series_data())

    def test_missing_y_column_raises_render_error(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.BAR,
            title="T",
            data_source="q",
            x_column="category",
            y_column="bad_column",
        )
        with pytest.raises(RenderError, match="bad_column"):
            render_chart(spec, _category_data())

    def test_missing_pie_values_column_raises_render_error(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.PIE,
            title="T",
            data_source="q",
            values_column="missing_values",
            names_column="payment_type",
        )
        with pytest.raises(RenderError, match="missing_values"):
            render_chart(spec, _pie_data())

    def test_heatmap_without_value_col_raises_render_error(self) -> None:
        """Heatmap with no color_column and no obvious numeric column."""
        spec = ChartSpec(
            chart_id="test_heatmap",
            chart_type=ChartType.HEATMAP,
            title="T",
            data_source="q",
            x_column="a",
            y_column="b",
            # No color_column; data has only string columns.
        )
        data = [{"a": "x", "b": "y"}]
        with pytest.raises(RenderError, match="value column"):
            render_chart(spec, data)


# ---------------------------------------------------------------------------
# VizAgent — happy path
# ---------------------------------------------------------------------------


class TestVizAgentSuccess:
    def test_returns_rendered_chart(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="test_line",
            chart_type=ChartType.LINE,
            title="Revenue Over Time",
            data_source="monthly_revenue",
            x_column="month",
            y_column="total",
        )
        result = agent.render(spec, _time_series_data())
        assert isinstance(result, RenderedChart)

    def test_success_true(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="test_bar",
            chart_type=ChartType.BAR,
            title="Revenue by Category",
            data_source="q",
            x_column="category",
            y_column="revenue",
        )
        result = agent.render(spec, _category_data())
        assert result.success is True

    def test_html_contains_div(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="test_bar",
            chart_type=ChartType.BAR,
            title="Revenue by Category",
            data_source="q",
            x_column="category",
            y_column="revenue",
        )
        result = agent.render(spec, _category_data())
        assert "<div" in result.html

    def test_chart_id_preserved(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="my_unique_id",
            chart_type=ChartType.PIE,
            title="Pie",
            data_source="q",
            values_column="count",
            names_column="payment_type",
        )
        result = agent.render(spec, _pie_data())
        assert result.chart_id == "my_unique_id"

    def test_title_preserved(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="t",
            chart_type=ChartType.LINE,
            title="My Chart Title",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        result = agent.render(spec, _time_series_data())
        assert result.title == "My Chart Title"

    def test_error_is_none_on_success(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="t",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        result = agent.render(spec, _time_series_data())
        assert result.error is None


# ---------------------------------------------------------------------------
# VizAgent — failure / graceful degradation
# ---------------------------------------------------------------------------


class TestVizAgentFailure:
    def test_empty_data_returns_failed_chart(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="fail",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="month",
            y_column="total",
        )
        result = agent.render(spec, [])
        assert result.success is False
        assert result.error is not None

    def test_missing_column_returns_failed_chart(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="fail",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="ghost_column",
            y_column="total",
        )
        result = agent.render(spec, _time_series_data())
        assert result.success is False

    def test_failed_html_is_empty_string(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="fail",
            chart_type=ChartType.BAR,
            title="T",
            data_source="q",
            x_column="bad",
            y_column="also_bad",
        )
        result = agent.render(spec, _category_data())
        assert result.html == ""

    def test_failed_chart_id_still_set(self) -> None:
        agent = VizAgent()
        spec = ChartSpec(
            chart_id="known_id",
            chart_type=ChartType.LINE,
            title="T",
            data_source="q",
            x_column="missing",
            y_column="total",
        )
        result = agent.render(spec, _time_series_data())
        assert result.chart_id == "known_id"
