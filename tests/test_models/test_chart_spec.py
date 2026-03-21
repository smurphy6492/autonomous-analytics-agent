"""Unit tests for ChartSpec and ChartType models."""

import pytest
from pydantic import ValidationError

from analytics_agent.models.chart_spec import ChartSpec, ChartType


class TestChartType:
    def test_all_values(self) -> None:
        assert ChartType.LINE == "line"
        assert ChartType.BAR == "bar"
        assert ChartType.HORIZONTAL_BAR == "horizontal_bar"
        assert ChartType.PIE == "pie"
        assert ChartType.SCATTER == "scatter"
        assert ChartType.HEATMAP == "heatmap"

    def test_from_string(self) -> None:
        assert ChartType("line") is ChartType.LINE
        assert ChartType("pie") is ChartType.PIE

    def test_invalid_value(self) -> None:
        with pytest.raises(ValueError):
            ChartType("donut")


class TestChartSpec:
    def test_valid_line_chart(self) -> None:
        spec = ChartSpec(
            chart_id="revenue_over_time",
            chart_type=ChartType.LINE,
            title="Monthly Revenue Trend",
            data_source="monthly_revenue",
            x_column="month",
            y_column="total_revenue",
            x_label="Month",
            y_label="Revenue (R$)",
            y_format=",.0f",
        )
        assert spec.chart_id == "revenue_over_time"
        assert spec.chart_type == ChartType.LINE
        assert spec.show_legend is True
        assert spec.height == 400
        assert spec.color_palette == "plotly"

    def test_valid_bar_chart(self) -> None:
        spec = ChartSpec(
            chart_id="revenue_by_category",
            chart_type=ChartType.BAR,
            title="Revenue by Product Category",
            data_source="category_breakdown",
            x_column="category",
            y_column="revenue",
            color_column="category",
        )
        assert spec.color_column == "category"

    def test_valid_pie_chart(self) -> None:
        spec = ChartSpec(
            chart_id="payment_method_share",
            chart_type=ChartType.PIE,
            title="Payment Method Distribution",
            data_source="payment_breakdown",
            values_column="payment_count",
            names_column="payment_type",
        )
        assert spec.values_column == "payment_count"
        assert spec.names_column == "payment_type"
        assert spec.x_column is None

    def test_valid_scatter_chart(self) -> None:
        spec = ChartSpec(
            chart_id="price_vs_freight",
            chart_type=ChartType.SCATTER,
            title="Price vs Freight",
            data_source="order_items",
            x_column="price",
            y_column="freight_value",
            size_column="quantity",
        )
        assert spec.size_column == "quantity"

    def test_default_height(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.BAR,
            title="Test",
            data_source="q1",
        )
        assert spec.height == 400

    def test_custom_height(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type=ChartType.BAR,
            title="Test",
            data_source="q1",
            height=600,
        )
        assert spec.height == 600

    def test_height_too_small(self) -> None:
        with pytest.raises(ValidationError):
            ChartSpec(
                chart_id="test",
                chart_type=ChartType.BAR,
                title="Test",
                data_source="q1",
                height=50,
            )

    def test_height_too_large(self) -> None:
        with pytest.raises(ValidationError):
            ChartSpec(
                chart_id="test",
                chart_type=ChartType.BAR,
                title="Test",
                data_source="q1",
                height=9999,
            )

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ChartSpec(
                chart_id="test",
                chart_type=ChartType.BAR,
                title="Test",
                data_source="q1",
                unknown_field="oops",  # type: ignore[call-arg]
            )

    def test_chart_type_from_string(self) -> None:
        spec = ChartSpec(
            chart_id="test",
            chart_type="bar",  # type: ignore[arg-type]  # str coerced to enum
            title="Test",
            data_source="q1",
        )
        assert spec.chart_type is ChartType.BAR

    def test_serialise_roundtrip(self) -> None:
        spec = ChartSpec(
            chart_id="rev",
            chart_type=ChartType.LINE,
            title="Revenue",
            data_source="q1",
            x_column="month",
            y_column="revenue",
        )
        dumped = spec.model_dump()
        restored = ChartSpec.model_validate(dumped)
        assert restored == spec
