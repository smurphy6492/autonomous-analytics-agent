"""Chart specification models — data contract between the Orchestrator and Viz Agent."""

from enum import StrEnum

from pydantic import BaseModel, Field


class ChartType(StrEnum):
    """Supported Plotly chart types."""

    LINE = "line"
    BAR = "bar"
    HORIZONTAL_BAR = "horizontal_bar"
    PIE = "pie"
    SCATTER = "scatter"
    HEATMAP = "heatmap"


class ChartSpec(BaseModel):
    """Specification for a chart to be rendered by the Viz Agent.

    The Orchestrator produces these; the Viz Agent consumes them.
    All column references must match actual columns in the named data_source
    query result.
    """

    chart_id: str  # Unique identifier, e.g. "revenue_over_time"
    chart_type: ChartType
    title: str

    # Data mapping
    data_source: str  # query_id from QueryResult that provides the data
    x_column: str | None = None  # x-axis column (not needed for pie)
    y_column: str | None = None  # y-axis column
    color_column: str | None = None  # column for color grouping
    size_column: str | None = None  # column for bubble size (scatter only)
    values_column: str | None = None  # column for slice values (pie only)
    names_column: str | None = None  # column for slice labels (pie only)

    # Display options
    x_label: str | None = None
    y_label: str | None = None
    show_legend: bool = True
    height: int = Field(default=400, ge=100, le=2000)
    max_rows: int | None = None  # truncate data to top-N rows before rendering

    # Bar chart options
    bar_mode: str | None = None  # "group" for side-by-side bars, "stack" for stacked (default)
    bar_norm: str | None = None  # "percent" for 100% stacked bars, None for default

    # Formatting
    x_format: str | None = None  # e.g. "%Y-%m-%d" for dates
    y_format: str | None = None  # e.g. ",.0f" for numbers with commas
    color_palette: str = "plotly"  # Plotly named color sequence

    model_config = {"extra": "forbid"}
