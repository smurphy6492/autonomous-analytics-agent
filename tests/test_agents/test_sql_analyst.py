"""Unit tests for SQLAnalystAgent."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from analytics_agent.agents.base import AgentError, BaseAgent
from analytics_agent.agents.sql_analyst import (
    SQLAnalystAgent,
    _build_sql_prompt,
    _format_schema,
    _SQLOutput,
)
from analytics_agent.db.engine import DuckDBEngine
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    TableProfile,
)
from analytics_agent.models.query_plan import PlannedQuery, SQLRequest

# ---------------------------------------------------------------------------
# Module-level path helpers
# ---------------------------------------------------------------------------

_SAMPLE_CSV = (
    Path(__file__).parent.parent.parent / "data" / "sample" / "sample_orders.csv"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> DataProfile:
    """Minimal DataProfile with a single 'orders' table."""
    return DataProfile(
        tables=[
            TableProfile(
                name="orders",
                row_count=10,
                columns=[
                    ColumnProfile(
                        name="order_id",
                        dtype="BIGINT",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=10,
                        cardinality="low",
                        sample_values=["1", "2", "3"],
                        is_date=False,
                        is_numeric=True,
                        min_value="1",
                        max_value="10",
                    ),
                    ColumnProfile(
                        name="order_date",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=10,
                        cardinality="low",
                        sample_values=["2023-01-01"],
                        is_date=True,
                        is_numeric=False,
                        min_value="2023-01-01",
                        max_value="2023-06-30",
                    ),
                    ColumnProfile(
                        name="revenue",
                        dtype="DOUBLE",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=10,
                        cardinality="low",
                        sample_values=["100.0"],
                        is_date=False,
                        is_numeric=True,
                        min_value="75.0",
                        max_value="495.0",
                    ),
                    ColumnProfile(
                        name="category",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=3,
                        cardinality="low",
                        sample_values=["electronics", "clothing", "furniture"],
                        is_date=False,
                        is_numeric=False,
                        min_value=None,
                        max_value=None,
                    ),
                    ColumnProfile(
                        name="customer_state",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=3,
                        cardinality="low",
                        sample_values=["CA", "NY", "TX"],
                        is_date=False,
                        is_numeric=False,
                        min_value=None,
                        max_value=None,
                    ),
                ],
            )
        ],
        relationships=[],
        suggested_grain="order_id",
        data_quality_issues=[],
    )


@pytest.fixture
def planned_query() -> PlannedQuery:
    return PlannedQuery(
        query_id="revenue_by_category",
        purpose="Total revenue grouped by product category",
        required_tables=["orders"],
        required_columns=["category", "revenue"],
        aggregation_grain="by_category",
        expected_output_type="breakdown",
    )


@pytest.fixture
def engine() -> DuckDBEngine:
    """Engine with sample_orders.csv pre-loaded as 'orders'."""
    eng = DuckDBEngine()
    eng.load_csv(_SAMPLE_CSV, "orders")
    return eng


def _make_base_mock(sql: str) -> MagicMock:
    """Return a mock BaseAgent whose call_structured always returns the given SQL."""
    base = MagicMock(spec=BaseAgent)
    base.call_structured.return_value = _SQLOutput(sql=sql)
    return base


# ---------------------------------------------------------------------------
# _build_sql_prompt — unit tests
# ---------------------------------------------------------------------------


class TestBuildSqlPrompt:
    def test_includes_query_purpose(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        assert "Total revenue grouped by product category" in _build_sql_prompt(req)

    def test_includes_output_type(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        assert "breakdown" in _build_sql_prompt(req)

    def test_includes_aggregation_grain(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        assert "by_category" in _build_sql_prompt(req)

    def test_includes_table_and_column_names(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        prompt = _build_sql_prompt(req)
        assert "orders" in prompt
        assert "category" in prompt
        assert "revenue" in prompt

    def test_includes_previous_error_on_retry(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(
            planned_query=planned_query,
            data_profile=profile,
            previous_error="Column 'nonexistent' not found",
        )
        prompt = _build_sql_prompt(req)
        assert "PREVIOUS ATTEMPT FAILED" in prompt
        assert "nonexistent" in prompt

    def test_no_previous_error_section_on_first_attempt(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        assert "PREVIOUS ATTEMPT" not in _build_sql_prompt(req)

    def test_required_columns_listed(
        self, planned_query: PlannedQuery, profile: DataProfile
    ) -> None:
        req = SQLRequest(planned_query=planned_query, data_profile=profile)
        prompt = _build_sql_prompt(req)
        assert "category" in prompt
        assert "revenue" in prompt


# ---------------------------------------------------------------------------
# _format_schema — unit tests
# ---------------------------------------------------------------------------


class TestFormatSchema:
    def test_formats_known_table(self, profile: DataProfile) -> None:
        block = _format_schema(profile, ["orders"])
        assert "orders" in block
        assert "revenue" in block
        assert "category" in block

    def test_handles_unknown_table(self, profile: DataProfile) -> None:
        block = _format_schema(profile, ["ghost_table"])
        assert "not found" in block.lower()

    def test_includes_dtype(self, profile: DataProfile) -> None:
        block = _format_schema(profile, ["orders"])
        assert "DOUBLE" in block or "VARCHAR" in block

    def test_includes_sample_values(self, profile: DataProfile) -> None:
        block = _format_schema(profile, ["orders"])
        assert "electronics" in block


# ---------------------------------------------------------------------------
# SQLAnalystAgent.execute_query — happy path
# ---------------------------------------------------------------------------


class TestExecuteQuerySuccess:
    def test_returns_successful_result(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = _make_base_mock(
            "SELECT category, SUM(revenue) AS total FROM orders GROUP BY category"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.success is True

    def test_attempts_is_one_on_first_success(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = _make_base_mock(
            "SELECT category, SUM(revenue) AS total FROM orders GROUP BY category"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.attempts == 1

    def test_query_id_preserved(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = _make_base_mock("SELECT COUNT(*) AS n FROM orders")
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.query_id == "revenue_by_category"

    def test_row_count_correct(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = _make_base_mock(
            "SELECT category, SUM(revenue) AS total FROM orders GROUP BY category"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.row_count == 3  # 3 distinct categories in sample_orders.csv

    def test_data_contains_rows(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = _make_base_mock(
            "SELECT category, SUM(revenue) AS total "
            "FROM orders GROUP BY category ORDER BY total DESC"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.data is not None
        categories = {r["category"] for r in result.data}
        assert categories == {"electronics", "clothing", "furniture"}

    def test_sql_stored_in_result(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        expected_sql = "SELECT COUNT(*) AS n FROM orders"
        base = _make_base_mock(expected_sql)
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.sql == expected_sql


# ---------------------------------------------------------------------------
# SQLAnalystAgent.execute_query — retry loop
# ---------------------------------------------------------------------------


class TestExecuteQueryRetry:
    def test_retries_on_bad_sql_and_succeeds(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """Bad SQL on attempt 1 → good SQL on attempt 2 → success."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = [
            _SQLOutput(sql="SELECT nonexistent_col FROM orders"),  # attempt 1
            _SQLOutput(  # attempt 2: succeeds
                sql=(
                    "SELECT category, SUM(revenue) AS total"
                    " FROM orders GROUP BY category"
                )
            ),
        ]
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.success is True
        assert result.attempts == 2

    def test_previous_error_appears_in_retry_prompt(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """The DuckDB error from attempt N must be present in attempt N+1's prompt."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = [
            _SQLOutput(sql="SELECT nonexistent_col FROM orders"),
            _SQLOutput(
                sql=(
                    "SELECT category, SUM(revenue) AS total"
                    " FROM orders GROUP BY category"
                )
            ),
        ]
        agent = SQLAnalystAgent(base=base, engine=engine)
        agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )

        # Second call's user_prompt (second positional arg) must reference the error.
        second_call = base.call_structured.call_args_list[1]
        user_prompt: str = second_call.args[1]
        assert "PREVIOUS ATTEMPT FAILED" in user_prompt

    def test_fails_after_max_retries(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """All 3 attempts return bad SQL → success=False, attempts=3."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.return_value = _SQLOutput(
            sql="SELECT nonexistent_col FROM orders"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.success is False
        assert result.attempts == 3
        assert result.error is not None

    def test_claude_called_exactly_max_attempts_times(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """call_structured must be invoked once per attempt, no more."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.return_value = _SQLOutput(
            sql="SELECT nonexistent_col FROM orders"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert base.call_structured.call_count == 3

    def test_succeeds_on_third_attempt(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = [
            _SQLOutput(sql="SELECT bad_col FROM orders"),  # attempt 1
            _SQLOutput(sql="SELECT also_bad FROM orders"),  # attempt 2
            _SQLOutput(  # attempt 3
                sql=(
                    "SELECT category, SUM(revenue) AS total"
                    " FROM orders GROUP BY category"
                )
            ),
        ]
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.success is True
        assert result.attempts == 3

    def test_each_retry_carries_previous_error(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """Verify the error chain: attempt 2 gets error from 1, attempt 3 from 2."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.return_value = _SQLOutput(
            sql="SELECT nonexistent_col FROM orders"
        )
        agent = SQLAnalystAgent(base=base, engine=engine)
        agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )

        # Attempt 1 has no previous error (previous_error=None initially).
        first_call = base.call_structured.call_args_list[0]
        assert "PREVIOUS ATTEMPT" not in first_call.args[1]

        # Attempts 2 and 3 must have a previous error in the prompt.
        for call in base.call_structured.call_args_list[1:]:
            assert "PREVIOUS ATTEMPT FAILED" in call.args[1]


# ---------------------------------------------------------------------------
# SQLAnalystAgent — agent API error handling
# ---------------------------------------------------------------------------


class TestExecuteQueryAgentError:
    def test_agent_error_returns_failed_result_immediately(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        """An AgentError (Claude API failure) should not be retried."""
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = AgentError("API unavailable")
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.success is False
        assert "API unavailable" in (result.error or "")
        # Should have stopped after the first attempt.
        assert base.call_structured.call_count == 1

    def test_agent_error_sets_correct_query_id(
        self,
        planned_query: PlannedQuery,
        profile: DataProfile,
        engine: DuckDBEngine,
    ) -> None:
        base = MagicMock(spec=BaseAgent)
        base.call_structured.side_effect = AgentError("timeout")
        agent = SQLAnalystAgent(base=base, engine=engine)
        result = agent.execute_query(
            SQLRequest(planned_query=planned_query, data_profile=profile)
        )
        assert result.query_id == "revenue_by_category"


# ---------------------------------------------------------------------------
# Integration test — requires ANTHROPIC_API_KEY
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSQLAnalystIntegration:
    def test_generates_and_executes_valid_sql(self, profile: DataProfile) -> None:
        """Real Claude API call: generated SQL must execute without errors."""
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        eng = DuckDBEngine()
        eng.load_csv(_SAMPLE_CSV, "orders")
        agent = SQLAnalystAgent(base=base, engine=eng)

        pq = PlannedQuery(
            query_id="revenue_by_category",
            purpose="Total revenue grouped by product category, ordered descending",
            required_tables=["orders"],
            required_columns=["category", "revenue"],
            aggregation_grain="by_category",
            expected_output_type="breakdown",
        )
        result = agent.execute_query(SQLRequest(planned_query=pq, data_profile=profile))
        assert result.success is True
        assert result.row_count > 0
        assert result.data is not None
        assert result.attempts >= 1

    def test_retry_loop_with_initially_invalid_sql(self, profile: DataProfile) -> None:
        """Seed with a previous error to force Claude into the correction path."""
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        eng = DuckDBEngine()
        eng.load_csv(_SAMPLE_CSV, "orders")
        agent = SQLAnalystAgent(base=base, engine=eng)

        pq = PlannedQuery(
            query_id="test_correction",
            purpose="Count of orders by customer state",
            required_tables=["orders"],
            required_columns=["customer_state"],
            aggregation_grain="by_state",
            expected_output_type="breakdown",
        )
        # Seed with a plausible prior error to confirm Claude corrects it.
        result = agent.execute_query(
            SQLRequest(
                planned_query=pq,
                data_profile=profile,
                previous_error="Column 'state' not found. Use 'customer_state'.",
            )
        )
        assert result.success is True
