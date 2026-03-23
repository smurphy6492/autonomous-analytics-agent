"""Tests for PipelineRunner orchestration logic."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from analytics_agent.pipeline.runner import _slugify

# ---------------------------------------------------------------------------
# _slugify helper
# ---------------------------------------------------------------------------


def test_slugify_basic() -> None:
    assert _slugify("Revenue by Category") == "revenue_by_category"


def test_slugify_removes_special_chars() -> None:
    result = _slugify("Hello, World! (2024)")
    assert "," not in result
    assert "!" not in result
    assert "(" not in result


def test_slugify_truncates_to_40_chars() -> None:
    long_title = "A" * 100
    result = _slugify(long_title)
    assert len(result) <= 40


def test_slugify_collapses_whitespace() -> None:
    assert _slugify("hello   world") == "hello_world"


def test_slugify_handles_hyphens() -> None:
    result = _slugify("hello-world")
    assert result == "hello_world"


# ---------------------------------------------------------------------------
# PipelineRunner.run() — input validation
# ---------------------------------------------------------------------------


def test_run_raises_on_empty_data_paths() -> None:
    from analytics_agent.config import Settings
    from analytics_agent.pipeline.runner import PipelineRunner

    mock_settings = MagicMock(spec=Settings)
    mock_settings.anthropic_api_key = "sk-test"
    mock_settings.model = "claude-sonnet-4-6"
    mock_settings.cache_dir = None

    with patch("analytics_agent.pipeline.runner.anthropic.Anthropic"):
        runner = PipelineRunner(settings=mock_settings)

    with pytest.raises(ValueError, match="data_paths must not be empty"):
        runner.run(data_paths=[], business_question="Any question")


def test_run_raises_on_missing_file(tmp_path: Path) -> None:
    from analytics_agent.config import Settings
    from analytics_agent.pipeline.runner import PipelineRunner

    mock_settings = MagicMock(spec=Settings)
    mock_settings.anthropic_api_key = "sk-test"
    mock_settings.model = "claude-sonnet-4-6"
    mock_settings.cache_dir = None

    with patch("analytics_agent.pipeline.runner.anthropic.Anthropic"):
        runner = PipelineRunner(settings=mock_settings)

    missing = tmp_path / "nonexistent.csv"
    with pytest.raises(ValueError, match="not found"):
        runner.run(data_paths=[missing], business_question="Any question")


# ---------------------------------------------------------------------------
# PipelineContext
# ---------------------------------------------------------------------------


def test_context_elapsed_ms_returns_zero_when_times_not_set() -> None:
    from analytics_agent.pipeline.context import PipelineContext

    ctx = PipelineContext(
        data_paths=[],
        table_names=[],
        business_question="test",
        output_path=Path("output/test.html"),
    )
    assert ctx.elapsed_ms() == 0


def test_context_elapsed_ms_calculates_delta() -> None:
    from datetime import UTC, datetime, timedelta

    from analytics_agent.pipeline.context import PipelineContext

    ctx = PipelineContext(
        data_paths=[],
        table_names=[],
        business_question="test",
        output_path=Path("output/test.html"),
    )
    now = datetime.now(UTC)
    ctx.start_time = now
    ctx.end_time = now + timedelta(seconds=2)
    assert ctx.elapsed_ms() == 2000


def test_context_record_error_appends() -> None:
    from analytics_agent.pipeline.context import PipelineContext

    ctx = PipelineContext(
        data_paths=[],
        table_names=[],
        business_question="test",
        output_path=Path("output/test.html"),
    )
    ctx.record_error("first error")
    ctx.record_error("second error")
    assert ctx.errors == ["first error", "second error"]
