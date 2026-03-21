"""HTML Report Builder — assembles the final report from pipeline context."""

from __future__ import annotations

import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from analytics_agent.models.query_plan import QueryResult
from analytics_agent.models.report import AnalysisReport, DataTableSpec

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent / "templates"


class ReportBuilder:
    """Renders an :class:`~analytics_agent.models.report.AnalysisReport` to HTML.

    Uses a Jinja2 template located in the ``templates/`` directory alongside
    this module.  The output is a fully self-contained HTML file — all CSS is
    embedded in the ``<style>`` block and Plotly JS is loaded from CDN.

    Usage::

        builder = ReportBuilder()
        html = builder.render(report)
        Path("output/report.html").write_text(html, encoding="utf-8")

    Or use :meth:`write` to write directly to a file::

        builder.write(report, Path("output/report.html"))
    """

    def __init__(self) -> None:
        self._env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            autoescape=select_autoescape(["html"]),
        )

    def render(self, report: AnalysisReport) -> str:
        """Render *report* to an HTML string.

        Args:
            report: The fully assembled pipeline output.

        Returns:
            A self-contained HTML document as a string.
        """
        template = self._env.get_template("report.html.jinja2")

        # Render data tables to HTML strings so the template can embed them.
        rendered_tables = _render_data_tables(report)

        # Pull only successful charts for display.
        charts = [c for c in report.rendered_charts if c.success]
        skipped_charts = [c for c in report.rendered_charts if not c.success]
        if skipped_charts:
            logger.warning(
                "Skipping %d failed chart(s): %s",
                len(skipped_charts),
                [c.chart_id for c in skipped_charts],
            )

        # Collect SQL queries for the methodology section.
        queries_for_template = [
            {
                "purpose": report.query_plan.queries[i].purpose
                if i < len(report.query_plan.queries)
                else qid,
                "sql": result.sql,
            }
            for i, (qid, result) in enumerate(report.query_results.items())
            if result.sql
        ]

        ctx = {
            "report_title": report.title,
            "business_question": report.business_question,
            "generated_at": report.generated_at.strftime("%Y-%m-%d %H:%M UTC"),
            "executive_summary": report.executive_summary,
            "key_metrics": report.key_metrics,
            "charts": charts,
            "data_tables": rendered_tables,
            "data_sources": report.data_sources,
            "analysis_approach": report.analysis_approach,
            "queries": queries_for_template,
            "execution_time": _format_duration(report.execution_time_ms),
            "errors": report.errors,
        }

        html = template.render(**ctx)
        logger.info("Report rendered: %d chars", len(html))
        return html

    def write(self, report: AnalysisReport, output_path: Path) -> None:
        """Render *report* and write to *output_path*.

        Creates parent directories if they do not exist.

        Args:
            report: The fully assembled pipeline output.
            output_path: Destination file path (e.g. ``output/report.html``).
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        html = self.render(report)
        output_path.write_text(html, encoding="utf-8")
        logger.info("Report written to %s (%d bytes)", output_path, len(html))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _render_data_tables(report: AnalysisReport) -> list[dict[str, str]]:
    """Convert :class:`~analytics_agent.models.report.DataTableSpec` objects to HTML.

    Each spec is resolved against ``report.query_results`` to get the actual
    row data, then rendered as an HTML ``<table>``.
    """
    rendered: list[dict[str, str]] = []
    for spec in report.data_tables:
        result: QueryResult | None = report.query_results.get(spec.data_source)
        if result is None or not result.success or not result.data:
            logger.warning(
                "Data table '%s': no data from query '%s' — skipping.",
                spec.table_id,
                spec.data_source,
            )
            continue
        rows = result.data[: spec.max_rows]
        html = _rows_to_html(rows, spec)
        rendered.append({"title": spec.title, "html": html})
    return rendered


def _rows_to_html(rows: list[dict], spec: DataTableSpec) -> str:  # type: ignore[type-arg]
    """Convert a list of row dicts to an HTML table string."""
    if not rows:
        return "<p><em>No data available.</em></p>"

    columns = spec.columns or list(rows[0].keys())
    # Filter to only columns that actually exist in the data.
    columns = [c for c in columns if c in rows[0]]

    header = "".join(f"<th>{_escape(c)}</th>" for c in columns)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{_escape(str(row.get(c, '')))}</td>" for c in columns)
        body_rows.append(f"<tr>{cells}</tr>")

    return (
        '<table class="data-table">'
        f"<thead><tr>{header}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def _escape(text: str) -> str:
    """HTML-escape a string (minimal, since Jinja2 autoescapes template output)."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _format_duration(ms: int) -> str:
    """Format milliseconds as a human-readable duration string."""
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m {secs}s"
