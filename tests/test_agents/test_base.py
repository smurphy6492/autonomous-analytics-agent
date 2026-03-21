"""Unit tests for BaseAgent — retry logic, caching, and structured output."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import anthropic
import pytest

from analytics_agent.agents.base import (
    AgentError,
    BaseAgent,
    _extract_json,
    _make_cache_key,
)
from analytics_agent.models.chart_spec import ChartSpec, ChartType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_agent(
    client: MagicMock | None = None,
    cache_dir: Path | None = None,
) -> BaseAgent:
    if client is None:
        client = MagicMock(spec=anthropic.Anthropic)
    return BaseAgent(client=client, cache_dir=cache_dir)


def _make_text_response(text: str) -> MagicMock:
    """Build a mock messages.create() return value with a single text block."""
    block = MagicMock()
    block.type = "text"
    block.text = text
    message = MagicMock()
    message.content = [block]
    return message


# ---------------------------------------------------------------------------
# _make_cache_key
# ---------------------------------------------------------------------------


class TestMakeCacheKey:
    def test_same_inputs_same_key(self) -> None:
        k1 = _make_cache_key("model", "sys", "user")
        k2 = _make_cache_key("model", "sys", "user")
        assert k1 == k2

    def test_different_inputs_different_key(self) -> None:
        k1 = _make_cache_key("model", "sys", "user-A")
        k2 = _make_cache_key("model", "sys", "user-B")
        assert k1 != k2

    def test_key_is_hex_string(self) -> None:
        key = _make_cache_key("m", "s", "u")
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


# ---------------------------------------------------------------------------
# _extract_json
# ---------------------------------------------------------------------------


class TestExtractJson:
    def test_plain_json_object(self) -> None:
        data = _extract_json('{"a": 1}')
        assert data == {"a": 1}

    def test_plain_json_with_whitespace(self) -> None:
        data = _extract_json('  {"a": 1}  ')
        assert data == {"a": 1}

    def test_markdown_json_fence(self) -> None:
        text = '```json\n{"a": 1}\n```'
        data = _extract_json(text)
        assert data == {"a": 1}

    def test_plain_markdown_fence(self) -> None:
        text = '```\n{"a": 1}\n```'
        data = _extract_json(text)
        assert data == {"a": 1}

    def test_json_embedded_in_prose(self) -> None:
        text = 'Here is the result: {"key": "value"} — done.'
        data = _extract_json(text)
        assert data == {"key": "value"}

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            _extract_json("not valid json at all")


# ---------------------------------------------------------------------------
# BaseAgent.call — happy path
# ---------------------------------------------------------------------------


class TestBaseAgentCall:
    def test_returns_text_response(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response("hello")
        agent = _make_agent(client)

        result = agent.call("sys", "usr")

        assert result == "hello"
        client.messages.create.assert_called_once()

    def test_passes_correct_params_to_api(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response("ok")
        agent = BaseAgent(client=client, model="claude-haiku-4-5", cache_dir=None)

        agent.call("my system", "my user")

        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["model"] == "claude-haiku-4-5"
        assert call_kwargs["system"] == "my system"
        assert call_kwargs["messages"][0]["content"] == "my user"


# ---------------------------------------------------------------------------
# BaseAgent — retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_succeeds_after_one_rate_limit(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = [
            anthropic.RateLimitError(
                message="rate limited",
                response=MagicMock(headers={}),
                body={},
            ),
            _make_text_response("ok"),
        ]
        agent = _make_agent(client)

        with patch("analytics_agent.agents.base.time.sleep"):
            result = agent.call("sys", "usr")

        assert result == "ok"
        assert client.messages.create.call_count == 2

    def test_succeeds_after_two_server_errors(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        server_err = anthropic.APIStatusError(
            message="server error",
            response=MagicMock(status_code=500, headers={}),
            body={},
        )
        client.messages.create.side_effect = [
            server_err,
            server_err,
            _make_text_response("done"),
        ]
        agent = _make_agent(client)

        with patch("analytics_agent.agents.base.time.sleep"):
            result = agent.call("sys", "usr")

        assert result == "done"
        assert client.messages.create.call_count == 3

    def test_raises_after_all_retries_exhausted(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = anthropic.RateLimitError(
            message="rate limited",
            response=MagicMock(headers={}),
            body={},
        )
        agent = BaseAgent(client=client, max_retries=3, cache_dir=None)

        with (
            patch("analytics_agent.agents.base.time.sleep"),
            pytest.raises(AgentError, match="All 3 attempts failed"),
        ):
            agent.call("sys", "usr")

        assert client.messages.create.call_count == 3

    def test_non_retryable_4xx_raises_immediately(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = anthropic.APIStatusError(
            message="bad request",
            response=MagicMock(status_code=400, headers={}),
            body={},
        )
        agent = _make_agent(client)

        with pytest.raises(AgentError, match="Non-retryable"):
            agent.call("sys", "usr")

        # Should not retry — only one call made.
        assert client.messages.create.call_count == 1

    def test_connection_error_is_retried(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = [
            anthropic.APIConnectionError(request=MagicMock()),
            _make_text_response("connected"),
        ]
        agent = _make_agent(client)

        with patch("analytics_agent.agents.base.time.sleep"):
            result = agent.call("sys", "usr")

        assert result == "connected"


# ---------------------------------------------------------------------------
# BaseAgent — caching
# ---------------------------------------------------------------------------


class TestCaching:
    def test_second_call_uses_cache(self, tmp_path: Path) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response("cached response")
        agent = BaseAgent(client=client, cache_dir=tmp_path)

        r1 = agent.call("sys", "usr")
        r2 = agent.call("sys", "usr")

        assert r1 == "cached response"
        assert r2 == "cached response"
        # API should only have been called once
        assert client.messages.create.call_count == 1

    def test_different_prompts_get_different_cache_entries(
        self, tmp_path: Path
    ) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = [
            _make_text_response("response-A"),
            _make_text_response("response-B"),
        ]
        agent = BaseAgent(client=client, cache_dir=tmp_path)

        r1 = agent.call("sys", "usr-A")
        r2 = agent.call("sys", "usr-B")

        assert r1 == "response-A"
        assert r2 == "response-B"
        assert client.messages.create.call_count == 2

    def test_no_cache_when_cache_dir_is_none(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response("result")
        agent = BaseAgent(client=client, cache_dir=None)

        agent.call("sys", "usr")
        agent.call("sys", "usr")

        # Both calls should hit the API since caching is disabled
        assert client.messages.create.call_count == 2

    def test_cache_dir_is_created_if_missing(self, tmp_path: Path) -> None:
        new_cache = tmp_path / "new_cache_dir"
        assert not new_cache.exists()
        BaseAgent(client=MagicMock(spec=anthropic.Anthropic), cache_dir=new_cache)
        assert new_cache.exists()


# ---------------------------------------------------------------------------
# BaseAgent.call_structured
# ---------------------------------------------------------------------------


class TestCallStructured:
    def _valid_chart_json(self) -> str:
        return json.dumps(
            {
                "chart_id": "test_chart",
                "chart_type": "bar",
                "title": "Test Chart",
                "data_source": "query_1",
                "x_column": "category",
                "y_column": "revenue",
            }
        )

    def test_parses_valid_json_response(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response(
            self._valid_chart_json()
        )
        agent = _make_agent(client)

        result = agent.call_structured("sys", "usr", ChartSpec)

        assert isinstance(result, ChartSpec)
        assert result.chart_id == "test_chart"
        assert result.chart_type == ChartType.BAR

    def test_raises_agent_error_on_invalid_json(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response("not json at all!")
        agent = _make_agent(client)

        with pytest.raises(AgentError, match="Failed to parse response"):
            agent.call_structured("sys", "usr", ChartSpec)

    def test_raises_agent_error_on_schema_mismatch(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        # Missing required fields
        client.messages.create.return_value = _make_text_response('{"chart_id": "x"}')
        agent = _make_agent(client)

        with pytest.raises(AgentError, match="Failed to parse response"):
            agent.call_structured("sys", "usr", ChartSpec)

    def test_json_schema_appended_to_system_prompt(self) -> None:
        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.return_value = _make_text_response(
            self._valid_chart_json()
        )
        agent = _make_agent(client)

        agent.call_structured("my system", "usr", ChartSpec)

        call_kwargs = client.messages.create.call_args.kwargs
        system_used = call_kwargs["system"]
        assert "my system" in system_used
        assert "JSON" in system_used or "json" in system_used
