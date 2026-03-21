"""Plotly chart renderer — deterministic rendering from ChartSpec + data."""

from __future__ import annotations

import logging
from typing import Any

import plotly.express as px  # type: ignore[import-untyped]
import plotly.graph_objects as go  # type: ignore[import-untyped]

from analytics_agent.models.chart_spec import ChartSpec, ChartType

logger = logging.getLogger(__name__)

# Colour sequence names accepted by Plotly Express.
_VALID_COLOR_SEQUENCES = {
    "plotly",
    "D3",
    "G10",
    "T10",
    "Alphabet",
    "Dark24",
    "Light24",
    "Set1",
    "Pastel1",
    "Set2",
    "Pastel2",
    "Set3",
    "Antique",
    "Bold",
    "Pastel",
    "Prism",
    "Safe",
    "Vivid",
}


class RenderError(Exception):
    """Raised when a chart cannot be rendered from the given spec + data."""


def render_chart(spec: ChartSpec, data: list[dict[str, Any]]) -> str:
    """Render a Plotly chart to an HTML div string.

    This function is entirely deterministic — no LLM call is made.  It
    dispatches on ``spec.chart_type`` and builds the appropriate
    ``plotly.express`` or ``plotly.graph_objects`` figure.

    Args:
        spec: The chart specification produced by the Orchestrator.
        data: Rows from the query result (list of dicts).

    Returns:
        An HTML string containing the Plotly ``<div>`` (and inline JS) that
        can be embedded directly in a report.

    Raises:
        RenderError: If the data is empty, required columns are missing, or
            the chart type is unsupported.
    """
    if not data:
        raise RenderError(f"Chart '{spec.chart_id}': data is empty — cannot render.")

    _validate_columns(spec, data[0])

    color_seq = _resolve_color_sequence(spec.color_palette)

    fig: go.Figure

    match spec.chart_type:
        case ChartType.LINE:
            fig = _render_line(spec, data, color_seq)
        case ChartType.BAR:
            fig = _render_bar(spec, data, color_seq, horizontal=False)
        case ChartType.HORIZONTAL_BAR:
            fig = _render_bar(spec, data, color_seq, horizontal=True)
        case ChartType.PIE:
            fig = _render_pie(spec, data, color_seq)
        case ChartType.SCATTER:
            fig = _render_scatter(spec, data, color_seq)
        case ChartType.HEATMAP:
            fig = _render_heatmap(spec, data)
        case _:
            raise RenderError(f"Unsupported chart type: {spec.chart_type!r}")

    _apply_layout(fig, spec)

    result: str = fig.to_html(
        full_html=False,
        include_plotlyjs=False,  # Plotly JS loaded once in the report template.
        div_id=spec.chart_id,
    )
    return result


# ------------------------------------------------------------------
# Per-type renderers
# ------------------------------------------------------------------


def _render_line(
    spec: ChartSpec,
    data: list[dict[str, Any]],
    color_seq: list[str] | None,
) -> go.Figure:
    kwargs: dict[str, Any] = {
        "data_frame": data,
        "x": spec.x_column,
        "y": spec.y_column,
        "title": spec.title,
        "height": spec.height,
    }
    if spec.color_column:
        kwargs["color"] = spec.color_column
    if spec.x_label:
        kwargs["labels"] = {spec.x_column: spec.x_label}
    if spec.y_label:
        kwargs.setdefault("labels", {})[spec.y_column] = spec.y_label
    if color_seq:
        kwargs["color_discrete_sequence"] = color_seq
    return px.line(**kwargs)


def _render_bar(
    spec: ChartSpec,
    data: list[dict[str, Any]],
    color_seq: list[str] | None,
    *,
    horizontal: bool,
) -> go.Figure:
    x_col = spec.y_column if horizontal else spec.x_column
    y_col = spec.x_column if horizontal else spec.y_column
    kwargs: dict[str, Any] = {
        "data_frame": data,
        "x": x_col,
        "y": y_col,
        "title": spec.title,
        "height": spec.height,
    }
    if spec.color_column:
        kwargs["color"] = spec.color_column
    if horizontal:
        kwargs["orientation"] = "h"
    if color_seq:
        kwargs["color_discrete_sequence"] = color_seq
    return px.bar(**kwargs)


def _render_pie(
    spec: ChartSpec,
    data: list[dict[str, Any]],
    color_seq: list[str] | None,
) -> go.Figure:
    kwargs: dict[str, Any] = {
        "data_frame": data,
        "values": spec.values_column,
        "names": spec.names_column,
        "title": spec.title,
        "height": spec.height,
    }
    if color_seq:
        kwargs["color_discrete_sequence"] = color_seq
    return px.pie(**kwargs)


def _render_scatter(
    spec: ChartSpec,
    data: list[dict[str, Any]],
    color_seq: list[str] | None,
) -> go.Figure:
    kwargs: dict[str, Any] = {
        "data_frame": data,
        "x": spec.x_column,
        "y": spec.y_column,
        "title": spec.title,
        "height": spec.height,
    }
    if spec.color_column:
        kwargs["color"] = spec.color_column
    if spec.size_column:
        kwargs["size"] = spec.size_column
    if color_seq:
        kwargs["color_discrete_sequence"] = color_seq
    return px.scatter(**kwargs)


def _render_heatmap(
    spec: ChartSpec,
    data: list[dict[str, Any]],
) -> go.Figure:
    """Render a heatmap using go.Heatmap.

    Expects data with x_column (row labels), y_column (column labels), and
    a numeric ``value`` column (or the first numeric column found).
    """
    # Find the value column: prefer color_column, then first numeric column.
    value_col: str | None = spec.color_column
    if value_col is None:
        for key, val in data[0].items():
            if key not in (spec.x_column, spec.y_column) and isinstance(
                val, (int, float)
            ):
                value_col = key
                break
    if value_col is None:
        raise RenderError(
            f"Heatmap '{spec.chart_id}': cannot determine value column. "
            "Set color_column to the numeric column to use."
        )

    x_col = spec.x_column or ""
    y_col = spec.y_column or ""
    x_vals = sorted({str(row[x_col]) for row in data})
    y_vals = sorted({str(row[y_col]) for row in data})
    z: list[list[float | None]] = []
    lookup: dict[tuple[str, str], float | None] = {
        (str(row[x_col]), str(row[y_col])): float(row[value_col])
        for row in data
    }
    for y in y_vals:
        z.append([lookup.get((x, y)) for x in x_vals])

    fig = go.Figure(
        data=go.Heatmap(z=z, x=x_vals, y=y_vals, colorscale="Blues"),
        layout=go.Layout(title=spec.title, height=spec.height),
    )
    return fig


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _validate_columns(spec: ChartSpec, sample_row: dict[str, Any]) -> None:
    """Raise RenderError if required columns are missing from sample_row."""
    available = set(sample_row.keys())
    required: list[str] = []

    match spec.chart_type:
        case ChartType.PIE:
            if spec.values_column:
                required.append(spec.values_column)
            if spec.names_column:
                required.append(spec.names_column)
        case _:
            if spec.x_column:
                required.append(spec.x_column)
            if spec.y_column:
                required.append(spec.y_column)

    if spec.color_column:
        required.append(spec.color_column)
    if spec.size_column:
        required.append(spec.size_column)

    missing = [c for c in required if c not in available]
    if missing:
        raise RenderError(
            f"Chart '{spec.chart_id}': missing column(s) {missing}. "
            f"Available columns: {sorted(available)}"
        )


def _resolve_color_sequence(palette: str) -> list[str] | None:
    """Return a Plotly colour sequence list for the given palette name.

    Returns ``None`` if the palette name isn't recognised (Plotly will use
    its default palette).
    """
    import plotly.express.colors as px_colors  # type: ignore[import-untyped]

    if palette == "plotly":
        return None  # Use Plotly's default

    try:
        seq = getattr(px_colors.qualitative, palette, None)
        if seq is not None:
            return list(seq)
    except Exception:
        pass

    logger.warning("Unknown color palette %r — using Plotly default.", palette)
    return None


def _apply_layout(fig: go.Figure, spec: ChartSpec) -> None:
    """Apply shared layout settings from the spec to the figure."""
    layout_updates: dict[str, Any] = {
        "showlegend": spec.show_legend,
    }
    if spec.x_label:
        layout_updates["xaxis_title"] = spec.x_label
    if spec.y_label:
        layout_updates["yaxis_title"] = spec.y_label
    fig.update_layout(**layout_updates)
