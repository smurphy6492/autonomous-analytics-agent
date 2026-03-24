"""Python Viz Agent — renders ChartSpec + data into HTML Plotly figures."""

from __future__ import annotations

import logging
from typing import Any

from analytics_agent.models.chart_spec import ChartSpec
from analytics_agent.models.report import RenderedChart
from analytics_agent.viz.renderer import RenderError, render_chart

logger = logging.getLogger(__name__)


class VizAgent:
    """Renders Plotly charts from ChartSpec objects.

    The Viz Agent is intentionally deterministic — it does **not** call the
    Claude API.  The Orchestrator has already produced structured chart specs;
    this agent simply translates them into HTML ``<div>`` elements using
    Plotly Express / Graph Objects.

    If rendering fails (e.g. a column is missing), it returns a
    :class:`~analytics_agent.models.report.RenderedChart` with
    ``success=False`` so the pipeline can continue without that chart.
    """

    def render(
        self,
        spec: ChartSpec,
        data: list[dict[str, Any]],
    ) -> RenderedChart:
        """Render a single chart to an HTML div.

        Args:
            spec: The chart specification from the Orchestrator.
            data: Rows from the query result that backs this chart.

        Returns:
            A :class:`~analytics_agent.models.report.RenderedChart` with the
            rendered HTML on success, or an error message on failure.
        """
        if spec.max_rows is not None:
            data = data[: spec.max_rows]
        logger.info(
            "Rendering chart '%s' (%s) from %d rows",
            spec.chart_id,
            spec.chart_type,
            len(data),
        )
        try:
            html = render_chart(spec, data)
            logger.info("Chart '%s' rendered successfully.", spec.chart_id)
            return RenderedChart(
                chart_id=spec.chart_id,
                title=spec.title,
                html=html,
                success=True,
            )
        except RenderError as exc:
            logger.warning("Chart '%s' failed to render: %s", spec.chart_id, exc)
            return RenderedChart(
                chart_id=spec.chart_id,
                title=spec.title,
                html="",
                success=False,
                error=str(exc),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Unexpected error rendering chart '%s': %s", spec.chart_id, exc
            )
            return RenderedChart(
                chart_id=spec.chart_id,
                title=spec.title,
                html="",
                success=False,
                error=f"Unexpected error: {exc}",
            )
