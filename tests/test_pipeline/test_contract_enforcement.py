"""Contract-violation tests for the pipeline runner.

Two concerns:
1. When a chart spec points at a data_source that is missing or failed, the
   runner must skip that chart and record an error — never render against
   absent data (runner._step_render_charts, runner.py:328-340).
2. The synthesis-layer models forbid unknown fields: an injected extra field
   must raise ValidationError rather than be silently accepted.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from analytics_agent.config import Settings
from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.query_plan import QueryResult
from analytics_agent.models.report import AnalysisSynthesis
from analytics_agent.pipeline.context import PipelineContext
from analytics_agent.pipeline.runner import PipelineRunner


def _make_runner() -> PipelineRunner:
    """Build a PipelineRunner without a real Anthropic client.

    Only the deterministic VizAgent and the step methods are exercised here,
    so the patched-out client is never called.
    """
    settings = MagicMock(spec=Settings)
    settings.anthropic_api_key = "sk-test"
    settings.model = "claude-sonnet-4-6"
    settings.cache_dir = None
    with patch("analytics_agent.pipeline.runner.anthropic.Anthropic"):
        return PipelineRunner(settings=settings)


def _ctx_with_synthesis(
    synthesis: AnalysisSynthesis,
    query_results: dict[str, QueryResult],
) -> PipelineContext:
    ctx = PipelineContext(
        data_paths=[],
        table_names=[],
        business_question="What is the revenue trend?",
        output_path=Path("output/test.html"),
    )
    ctx.synthesis = synthesis
    ctx.query_results = query_results
    return ctx


# ---------------------------------------------------------------------------
# Missing / failed data_source — the chart must be skipped, not rendered
# ---------------------------------------------------------------------------


class TestMissingDataSourceSkipped:
    def test_missing_data_source_chart_is_skipped_and_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        synthesis = AnalysisSynthesis(
            executive_summary="Revenue grew over the period.",
            chart_specs=[
                ChartSpec(
                    chart_id="good_chart",
                    chart_type=ChartType.LINE,
                    title="Revenue Over Time",
                    data_source="good_q",
                    x_column="month",
                    y_column="total",
                ),
                ChartSpec(
                    chart_id="orphan_chart",
                    chart_type=ChartType.BAR,
                    title="Chart with no backing query",
                    data_source="missing_q",  # not in query_results
                    x_column="category",
                    y_column="total",
                ),
            ],
        )
        query_results = {
            "good_q": QueryResult(
                query_id="good_q",
                sql="SELECT month, SUM(revenue) AS total FROM orders GROUP BY month",
                success=True,
                data=[
                    {"month": "2023-01", "total": 10000.0},
                    {"month": "2023-02", "total": 12000.0},
                ],
                row_count=2,
                attempts=1,
            ),
        }
        ctx = _ctx_with_synthesis(synthesis, query_results)

        with caplog.at_level(logging.WARNING):
            _make_runner()._step_render_charts(ctx)

        # The orphan chart was skipped: only the good chart was rendered.
        rendered_ids = [c.chart_id for c in ctx.rendered_charts]
        assert rendered_ids == ["good_chart"]
        # The skip was recorded as a pipeline error and a warning.
        assert any("orphan_chart" in e and "missing_q" in e for e in ctx.errors)
        assert any("orphan_chart" in r.message for r in caplog.records)

    def test_failed_data_source_chart_is_skipped(self) -> None:
        # The source query exists but did not succeed — still skip the chart.
        synthesis = AnalysisSynthesis(
            executive_summary="One query failed.",
            chart_specs=[
                ChartSpec(
                    chart_id="from_failed_q",
                    chart_type=ChartType.BAR,
                    title="Backed by a failed query",
                    data_source="failed_q",
                    x_column="category",
                    y_column="total",
                ),
            ],
        )
        query_results = {
            "failed_q": QueryResult(
                query_id="failed_q",
                sql="SELECT ...",
                success=False,
                error="syntax error",
                attempts=3,
            ),
        }
        ctx = _ctx_with_synthesis(synthesis, query_results)

        _make_runner()._step_render_charts(ctx)

        assert ctx.rendered_charts == []
        assert any("from_failed_q" in e for e in ctx.errors)


# ---------------------------------------------------------------------------
# extra="forbid" — an injected unknown field must be rejected
# ---------------------------------------------------------------------------


class TestExtraFieldRejected:
    def test_synthesis_rejects_unknown_field(self) -> None:
        payload = {
            "executive_summary": "Revenue grew.",
            "key_metrics": [],
            "chart_specs": [],
            "data_tables": [],
            "injected_field": "should not be accepted",
        }
        with pytest.raises(ValidationError):
            AnalysisSynthesis.model_validate(payload)

    def test_chart_spec_rejects_unknown_field(self) -> None:
        payload = {
            "chart_id": "c1",
            "chart_type": "line",
            "title": "T",
            "data_source": "q",
            "x_column": "month",
            "y_column": "total",
            "injected_field": "should not be accepted",
        }
        with pytest.raises(ValidationError):
            ChartSpec.model_validate(payload)
