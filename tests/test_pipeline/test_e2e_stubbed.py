"""End-to-end pipeline test with a stubbed LLM — runs in CI without an API key.

Every other test exercises a single agent or helper. This one runs the *whole*
pipeline — profile → plan → execute → synthesize → validate → render → report —
against a real DuckDB and a real CSV, with only the Claude API calls stubbed.
The SQL the stub returns is executed for real, so the DuckDB path, the
validators, the Plotly renderer, and the HTML builder all run end-to-end.

This is what proves "a question in produces a report out" actually holds,
rather than just that individual functions typecheck.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from analytics_agent.config import Settings
from analytics_agent.models.report import AnalysisReport
from analytics_agent.pipeline.runner import PipelineRunner

# --- Canned LLM responses, keyed by a phrase unique to each agent's prompt. ---

_PROFILE_JSON = """
{
  "tables": [{
    "name": "orders",
    "row_count": 10,
    "columns": [
      {"name": "order_id", "dtype": "BIGINT", "null_count": 0, "null_pct": 0.0,
       "unique_count": 10, "cardinality": "low", "sample_values": ["1", "2"],
       "is_date": false, "is_numeric": true},
      {"name": "order_date", "dtype": "DATE", "null_count": 0, "null_pct": 0.0,
       "unique_count": 10, "cardinality": "low", "sample_values": ["2023-01-01"],
       "is_date": true, "is_numeric": false},
      {"name": "revenue", "dtype": "DOUBLE", "null_count": 0, "null_pct": 0.0,
       "unique_count": 10, "cardinality": "low", "sample_values": ["100.0"],
       "is_date": false, "is_numeric": true},
      {"name": "category", "dtype": "VARCHAR", "null_count": 0, "null_pct": 0.0,
       "unique_count": 3, "cardinality": "low", "sample_values": ["electronics"],
       "is_date": false, "is_numeric": false}
    ]
  }],
  "relationships": [],
  "suggested_grain": "order_id",
  "data_quality_issues": []
}
"""

_PLAN_JSON = """
{
  "analysis_approach": "Aggregate revenue by category.",
  "queries": [{
    "query_id": "revenue_by_category",
    "purpose": "Total revenue by category",
    "required_tables": ["orders"],
    "required_columns": ["category", "revenue"],
    "aggregation_grain": "by_category",
    "expected_output_type": "breakdown"
  }]
}
"""

# Real SQL — DuckDB executes this against the loaded CSV for genuine rows.
_SQL_JSON = (
    '{"sql": "SELECT category, SUM(revenue) AS total_revenue FROM orders '
    'GROUP BY category ORDER BY total_revenue DESC"}'
)

_SYNTHESIS_JSON = """
{
  "executive_summary": "Electronics leads total revenue, then furniture and clothing.",
  "key_metrics": [{"label": "Top category", "value": "electronics"}],
  "chart_specs": [{
    "chart_id": "rev_by_cat",
    "chart_type": "bar",
    "title": "Revenue by Category",
    "data_source": "revenue_by_category",
    "x_column": "category",
    "y_column": "total_revenue"
  }],
  "data_tables": [{
    "table_id": "rev_tbl",
    "title": "Revenue by Category",
    "data_source": "revenue_by_category"
  }]
}
"""


def _route_response(system_prompt: str) -> str:
    """Return the canned response for whichever agent is calling."""
    if "data profiler agent" in system_prompt:
        return _PROFILE_JSON
    if "decide what SQL analyses" in system_prompt:
        return _PLAN_JSON
    if "SQL analyst agent" in system_prompt:
        return _SQL_JSON
    if "synthesize SQL query" in system_prompt:
        return _SYNTHESIS_JSON
    if "quality assurance reviewer" in system_prompt:  # coverage
        return "PASS"
    if "data quality reviewer" in system_prompt:  # metric sanity
        return "PASS"
    raise AssertionError(f"Unexpected system prompt: {system_prompt[:80]!r}")


def _fake_anthropic_factory() -> MagicMock:
    """Build a fake Anthropic client that routes on the system prompt."""
    client = MagicMock()

    def _create(**kwargs: object) -> MagicMock:
        system = str(kwargs.get("system", ""))
        block = MagicMock()
        block.type = "text"
        block.text = _route_response(system)
        message = MagicMock()
        message.content = [block]
        return message

    client.messages.create.side_effect = _create
    return client


@pytest.fixture
def orders_csv(tmp_path: Path) -> Path:
    path = tmp_path / "orders.csv"
    path.write_text(
        "order_id,order_date,revenue,category\n"
        "1,2023-01-01,500.00,electronics\n"
        "2,2023-01-02,600.00,furniture\n"
        "3,2023-01-03,250.00,clothing\n"
        "4,2023-02-01,300.00,electronics\n"
        "5,2023-02-10,100.00,furniture\n"
        "6,2023-03-05,200.00,electronics\n"
        "7,2023-03-12,150.00,clothing\n"
        "8,2023-03-25,300.00,furniture\n",
        encoding="utf-8",
    )
    return path


def _make_runner() -> PipelineRunner:
    settings = MagicMock(spec=Settings)
    settings.anthropic_api_key = "sk-test"
    settings.model = "claude-sonnet-4-6"
    settings.cache_dir = None
    with patch(
        "analytics_agent.pipeline.runner.anthropic.Anthropic",
        return_value=_fake_anthropic_factory(),
    ):
        return PipelineRunner(settings=settings)


def test_full_pipeline_produces_report(orders_csv: Path, tmp_path: Path) -> None:
    runner = _make_runner()
    output_path = tmp_path / "report.html"

    report = runner.run(
        data_paths=[orders_csv],
        business_question="What is total revenue by product category?",
        title="E2E Test",
        output_path=output_path,
    )

    # A structurally complete report came out the far end.
    assert isinstance(report, AnalysisReport)
    assert report.executive_summary
    assert report.query_plan.queries[0].query_id == "revenue_by_category"

    # The stubbed SQL actually executed against DuckDB and returned real rows.
    result = report.query_results["revenue_by_category"]
    assert result.success
    assert result.row_count == 3  # electronics, furniture, clothing

    # Real aggregation: electronics = 500 + 300 + 200 = 1000 (the top row).
    top_row = result.data[0]
    assert top_row["category"] == "electronics"
    assert top_row["total_revenue"] == pytest.approx(1000.0)

    # A chart rendered and the HTML file was written.
    assert len(report.rendered_charts) == 1
    assert report.rendered_charts[0].success
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").strip() != ""


def test_full_pipeline_no_stub_gaps(orders_csv: Path, tmp_path: Path) -> None:
    # Every LLM call the pipeline makes must be one the stub knows how to answer;
    # an unrouted prompt raises in _route_response. This guards against a new
    # agent call slipping into the pipeline untested.
    runner = _make_runner()
    report = runner.run(
        data_paths=[orders_csv],
        business_question="What is total revenue by product category?",
        title="E2E Coverage",
        output_path=tmp_path / "r.html",
    )
    assert report.query_results["revenue_by_category"].success
