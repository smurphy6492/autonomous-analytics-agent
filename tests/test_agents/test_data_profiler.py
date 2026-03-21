"""Unit tests for DataProfilerAgent."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from analytics_agent.agents.base import BaseAgent
from analytics_agent.agents.data_profiler import (
    DataProfilerAgent,
    _build_profile_prompt,
)
from analytics_agent.db.engine import DuckDBEngine
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    ProfileRequest,
    TableProfile,
)

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
def engine() -> DuckDBEngine:
    """Fresh in-memory DuckDB engine per test."""
    return DuckDBEngine()


@pytest.fixture
def minimal_profile() -> DataProfile:
    """Pre-built DataProfile returned by the mocked Claude call."""
    return DataProfile(
        tables=[
            TableProfile(
                name="orders",
                row_count=3,
                columns=[
                    ColumnProfile(
                        name="order_id",
                        dtype="INTEGER",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=3,
                        cardinality="low",
                        sample_values=["1", "2", "3"],
                        is_date=False,
                        is_numeric=True,
                        min_value="1",
                        max_value="3",
                    ),
                    ColumnProfile(
                        name="order_date",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=3,
                        cardinality="low",
                        sample_values=["2023-01-01", "2023-01-02", "2023-01-03"],
                        is_date=True,
                        is_numeric=False,
                        min_value="2023-01-01",
                        max_value="2023-01-03",
                    ),
                    ColumnProfile(
                        name="revenue",
                        dtype="DOUBLE",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=3,
                        cardinality="low",
                        sample_values=["100.0", "200.0", "150.0"],
                        is_date=False,
                        is_numeric=True,
                        min_value="100.0",
                        max_value="200.0",
                    ),
                    ColumnProfile(
                        name="category",
                        dtype="VARCHAR",
                        null_count=0,
                        null_pct=0.0,
                        unique_count=2,
                        cardinality="low",
                        sample_values=["electronics", "clothing"],
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
def mock_base(minimal_profile: DataProfile) -> MagicMock:
    """Mock BaseAgent whose call_structured returns the minimal profile."""
    base = MagicMock(spec=BaseAgent)
    base.call_structured.return_value = minimal_profile
    return base


@pytest.fixture
def profiler(mock_base: MagicMock, engine: DuckDBEngine) -> DataProfilerAgent:
    return DataProfilerAgent(base=mock_base, engine=engine)


# ---------------------------------------------------------------------------
# _collect_table_stats — programmatic stats, no LLM
# ---------------------------------------------------------------------------


class TestCollectTableStats:
    def test_returns_correct_row_count(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        assert stats["row_count"] == 3

    def test_column_names_match_csv_headers(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        col_names = [c["name"] for c in stats["columns"]]
        assert "order_id" in col_names
        assert "revenue" in col_names
        assert "category" in col_names

    def test_null_count_zero_for_complete_csv(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        for col in stats["columns"]:
            assert col["null_count"] == 0

    def test_unique_count_for_categorical_column(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        cat_col = next(c for c in stats["columns"] if c["name"] == "category")
        # conftest sample_csv has 2 categories: electronics + clothing
        assert cat_col["unique_count"] == 2

    def test_sample_values_populated(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        rev_col = next(c for c in stats["columns"] if c["name"] == "revenue")
        assert len(rev_col["sample_values"]) >= 1

    def test_min_max_for_numeric_column(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        rev_col = next(c for c in stats["columns"] if c["name"] == "revenue")
        assert rev_col["min_value"] is not None
        assert rev_col["max_value"] is not None


class TestCollectTableStatsWithNulls:
    """Verify null detection on a CSV that has explicit null values."""

    @pytest.fixture
    def csv_with_nulls(self, tmp_path: Path) -> Path:
        data = (
            "order_id,revenue,category\n"
            "1,100.00,electronics\n"
            "2,,clothing\n"  # revenue null
            "3,150.00,\n"  # category null
        )
        p = tmp_path / "nulls.csv"
        p.write_text(data)
        return p

    def test_detects_revenue_null(
        self, profiler: DataProfilerAgent, csv_with_nulls: Path
    ) -> None:
        profiler._engine.load_csv(csv_with_nulls, "nulls")
        stats = profiler._collect_table_stats("nulls")
        rev_col = next(c for c in stats["columns"] if c["name"] == "revenue")
        assert rev_col["null_count"] == 1

    def test_detects_category_null(
        self, profiler: DataProfilerAgent, csv_with_nulls: Path
    ) -> None:
        profiler._engine.load_csv(csv_with_nulls, "nulls")
        stats = profiler._collect_table_stats("nulls")
        cat_col = next(c for c in stats["columns"] if c["name"] == "category")
        assert cat_col["null_count"] == 1


# ---------------------------------------------------------------------------
# _build_profile_prompt — unit tests for the prompt builder
# ---------------------------------------------------------------------------


class TestBuildProfilePrompt:
    def test_includes_table_name(self) -> None:
        raw = [{"table_name": "my_orders", "row_count": 5, "columns": []}]
        assert "my_orders" in _build_profile_prompt(raw)

    def test_formats_row_count_with_commas(self) -> None:
        raw = [{"table_name": "t", "row_count": 9999, "columns": []}]
        assert "9,999" in _build_profile_prompt(raw)

    def test_includes_column_name_and_type(self) -> None:
        raw = [
            {
                "table_name": "t",
                "row_count": 2,
                "columns": [
                    {
                        "name": "my_col",
                        "type": "INTEGER",
                        "null_count": 0,
                        "unique_count": 2,
                        "sample_values": ["1", "2"],
                        "min_value": "1",
                        "max_value": "2",
                    }
                ],
            }
        ]
        prompt = _build_profile_prompt(raw)
        assert "my_col" in prompt
        assert "INTEGER" in prompt

    def test_includes_null_count(self) -> None:
        raw = [
            {
                "table_name": "t",
                "row_count": 5,
                "columns": [
                    {
                        "name": "c",
                        "type": "VARCHAR",
                        "null_count": 2,
                        "unique_count": 3,
                        "sample_values": [],
                        "min_value": None,
                        "max_value": None,
                    }
                ],
            }
        ]
        prompt = _build_profile_prompt(raw)
        assert "null_count=2" in prompt

    def test_includes_quality_issues(self) -> None:
        raw = [{"table_name": "t", "row_count": 1, "columns": []}]
        prompt = _build_profile_prompt(raw, ["file not found: bad.csv"])
        assert "file not found" in prompt

    def test_no_quality_section_when_empty(self) -> None:
        raw = [{"table_name": "t", "row_count": 1, "columns": []}]
        prompt = _build_profile_prompt(raw, None)
        assert "Pre-loading issues" not in prompt

    def test_skips_min_max_when_none(self) -> None:
        raw = [
            {
                "table_name": "t",
                "row_count": 1,
                "columns": [
                    {
                        "name": "c",
                        "type": "VARCHAR",
                        "null_count": 0,
                        "unique_count": 1,
                        "sample_values": ["x"],
                        "min_value": None,
                        "max_value": None,
                    }
                ],
            }
        ]
        prompt = _build_profile_prompt(raw)
        assert "min=" not in prompt
        assert "max=" not in prompt


# ---------------------------------------------------------------------------
# DataProfilerAgent.profile — end-to-end unit (mocked Claude)
# ---------------------------------------------------------------------------


class TestProfilerProfile:
    def test_returns_dataprofile_instance(
        self,
        profiler: DataProfilerAgent,
        sample_csv: Path,
    ) -> None:
        result = profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        assert isinstance(result, DataProfile)

    def test_calls_call_structured_with_dataprofile(
        self,
        profiler: DataProfilerAgent,
        mock_base: MagicMock,
        sample_csv: Path,
    ) -> None:
        profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        mock_base.call_structured.assert_called_once()
        # Third positional arg is the response_model
        _, _, model_arg = mock_base.call_structured.call_args.args
        assert model_arg is DataProfile

    def test_uses_custom_table_name(
        self,
        profiler: DataProfilerAgent,
        sample_csv: Path,
    ) -> None:
        profiler.profile(
            ProfileRequest(data_paths=[str(sample_csv)], table_names=["custom_name"])
        )
        assert "custom_name" in profiler._engine.table_names()

    def test_raises_on_mismatched_path_name_lengths(
        self,
        profiler: DataProfilerAgent,
        sample_csv: Path,
    ) -> None:
        with pytest.raises(ValueError, match="same length"):
            profiler.profile(
                ProfileRequest(
                    data_paths=[str(sample_csv), str(sample_csv)],
                    table_names=["only_one"],
                )
            )

    def test_raises_when_no_tables_load(
        self,
        profiler: DataProfilerAgent,
        tmp_path: Path,
    ) -> None:
        missing = str(tmp_path / "ghost.csv")
        with pytest.raises(ValueError, match="No tables could be loaded"):
            profiler.profile(ProfileRequest(data_paths=[missing]))

    def test_missing_file_does_not_crash_when_one_table_loads(
        self,
        profiler: DataProfilerAgent,
        sample_csv: Path,
        tmp_path: Path,
        mock_base: MagicMock,
    ) -> None:
        """One missing file should log a quality issue; the rest still profiles."""
        missing = str(tmp_path / "ghost.csv")
        profiler.profile(
            ProfileRequest(
                data_paths=[str(sample_csv), missing],
                table_names=["real", "missing"],
            )
        )
        # The user prompt passed to Claude should mention the missing table.
        call_args = mock_base.call_structured.call_args
        user_prompt: str = call_args.args[1]
        assert "missing" in user_prompt.lower() or "Could not load" in user_prompt

    def test_prompt_contains_stats_for_loaded_table(
        self,
        profiler: DataProfilerAgent,
        sample_csv: Path,
        mock_base: MagicMock,
    ) -> None:
        profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        user_prompt: str = mock_base.call_structured.call_args.args[1]
        # The sample CSV stem is 'sample' — that should appear as the table name.
        assert "sample" in user_prompt
        assert "row_count" not in user_prompt  # row_count is in the raw dict key
        assert "rows" in user_prompt  # formatted as "N rows"


# ---------------------------------------------------------------------------
# Integration test — requires ANTHROPIC_API_KEY
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDataProfilerIntegration:
    def test_profiles_sample_csv_end_to_end(self, sample_csv: Path) -> None:
        """Real Claude API call: verify the returned DataProfile is valid."""
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        engine = DuckDBEngine()
        agent = DataProfilerAgent(base=base, engine=engine)

        profile = agent.profile(ProfileRequest(data_paths=[str(sample_csv)]))

        assert isinstance(profile, DataProfile)
        assert len(profile.tables) == 1
        assert profile.tables[0].row_count == 3
        assert profile.suggested_grain != ""
        col_names = [c.name for c in profile.tables[0].columns]
        assert "order_id" in col_names
        assert "revenue" in col_names

    def test_profiles_sample_orders_csv(self) -> None:
        """Real Claude API call against the committed sample_orders.csv."""
        import os

        import anthropic

        if not os.getenv("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

        from analytics_agent.agents.base import BaseAgent

        client = anthropic.Anthropic()
        base = BaseAgent(client=client, cache_dir=None)
        engine = DuckDBEngine()
        agent = DataProfilerAgent(base=base, engine=engine)

        profile = agent.profile(ProfileRequest(data_paths=[str(_SAMPLE_CSV)]))

        assert isinstance(profile, DataProfile)
        assert profile.tables[0].row_count == 10
        # Cardinality for customer_state (3 unique values) should be "low".
        state_col = next(
            c for c in profile.tables[0].columns if c.name == "customer_state"
        )
        assert state_col.cardinality == "low"
