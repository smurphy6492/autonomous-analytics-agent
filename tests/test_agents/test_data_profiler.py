"""Unit tests for DataProfilerAgent — fully deterministic, no LLM."""

from __future__ import annotations

from pathlib import Path

import pytest

from analytics_agent.agents.data_profiler import (
    DataProfilerAgent,
    _cardinality,
    _detect_relationships,
    _is_date,
    _is_numeric,
    _suggest_grain,
)
from analytics_agent.db.engine import DuckDBEngine
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    ProfileRequest,
    TableProfile,
)

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
def profiler(engine: DuckDBEngine) -> DataProfilerAgent:
    return DataProfilerAgent(engine=engine)


# ---------------------------------------------------------------------------
# _collect_table_stats — programmatic stats
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
        assert cat_col["unique_count"] == 2

    def test_min_max_for_numeric_column(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler._engine.load_csv(sample_csv, "orders")
        stats = profiler._collect_table_stats("orders")
        rev_col = next(c for c in stats["columns"] if c["name"] == "revenue")
        assert rev_col["min_value"] is not None
        assert rev_col["max_value"] is not None


class TestCollectTableStatsWithNulls:
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


# ---------------------------------------------------------------------------
# Deterministic annotation rules
# ---------------------------------------------------------------------------


class TestCardinality:
    def test_low(self) -> None:
        assert _cardinality(5) == "low"
        assert _cardinality(19) == "low"

    def test_medium(self) -> None:
        assert _cardinality(20) == "medium"
        assert _cardinality(1000) == "medium"

    def test_high(self) -> None:
        assert _cardinality(1001) == "high"
        assert _cardinality(500_000) == "high"


class TestIsNumeric:
    @pytest.mark.parametrize(
        "dtype", ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL(10,2)", "FLOAT", "HUGEINT"]
    )
    def test_numeric_types(self, dtype: str) -> None:
        assert _is_numeric(dtype) is True

    @pytest.mark.parametrize("dtype", ["VARCHAR", "DATE", "TIMESTAMP", "BOOLEAN"])
    def test_non_numeric_types(self, dtype: str) -> None:
        assert _is_numeric(dtype) is False


class TestIsDate:
    def test_native_date_type(self) -> None:
        assert _is_date("DATE", []) is True

    def test_timestamp_type(self) -> None:
        assert _is_date("TIMESTAMP", []) is True

    def test_varchar_with_date_samples(self) -> None:
        assert _is_date("VARCHAR", ["2023-01-01", "2023-02-15"]) is True

    def test_varchar_with_non_date_samples(self) -> None:
        assert _is_date("VARCHAR", ["electronics", "clothing"]) is False

    def test_varchar_no_samples(self) -> None:
        assert _is_date("VARCHAR", []) is False


class TestDetectRelationships:
    def test_shared_id_column_links_tables(self) -> None:
        tables = [
            _table("customers", ["customer_id", "name"]),
            _table("orders", ["order_id", "customer_id"]),
        ]
        rels = _detect_relationships(tables)
        assert any(
            r.from_table == "orders"
            and r.from_column == "customer_id"
            and r.to_table == "customers"
            for r in rels
        )

    def test_non_id_shared_column_not_linked(self) -> None:
        tables = [
            _table("a", ["name", "value"]),
            _table("b", ["name", "other"]),
        ]
        assert _detect_relationships(tables) == []


class TestSuggestGrain:
    def test_prefers_unique_key_column(self) -> None:
        table = TableProfile(
            name="orders",
            row_count=3,
            columns=[
                _col("order_id", unique_count=3),
                _col("category", unique_count=2),
            ],
        )
        assert _suggest_grain([table]) == "order_id"

    def test_falls_back_to_id_like_column(self) -> None:
        table = TableProfile(
            name="events",
            row_count=10,
            columns=[
                _col("user_id", unique_count=4),
                _col("action", unique_count=3),
            ],
        )
        assert _suggest_grain([table]) == "user_id"


# ---------------------------------------------------------------------------
# DataProfilerAgent.profile — end-to-end, deterministic
# ---------------------------------------------------------------------------


class TestProfilerProfile:
    def test_returns_dataprofile_instance(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        result = profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        assert isinstance(result, DataProfile)

    def test_row_count_and_columns_are_correct(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profile = profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        table = profile.tables[0]
        assert table.row_count == 3
        names = {c.name for c in table.columns}
        assert {"order_id", "order_date", "revenue", "category"} <= names

    def test_flags_are_computed(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profile = profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        cols = {c.name: c for c in profile.tables[0].columns}
        assert cols["revenue"].is_numeric is True
        assert cols["order_date"].is_date is True
        assert cols["category"].is_numeric is False
        assert cols["category"].cardinality == "low"

    def test_suggested_grain_is_set(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profile = profiler.profile(ProfileRequest(data_paths=[str(sample_csv)]))
        assert profile.suggested_grain != ""

    def test_uses_custom_table_name(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        profiler.profile(
            ProfileRequest(data_paths=[str(sample_csv)], table_names=["custom_name"])
        )
        assert "custom_name" in profiler._engine.table_names()

    def test_raises_on_mismatched_path_name_lengths(
        self, profiler: DataProfilerAgent, sample_csv: Path
    ) -> None:
        with pytest.raises(ValueError, match="same length"):
            profiler.profile(
                ProfileRequest(
                    data_paths=[str(sample_csv), str(sample_csv)],
                    table_names=["only_one"],
                )
            )

    def test_raises_when_no_tables_load(
        self, profiler: DataProfilerAgent, tmp_path: Path
    ) -> None:
        missing = str(tmp_path / "ghost.csv")
        with pytest.raises(ValueError, match="No tables could be loaded"):
            profiler.profile(ProfileRequest(data_paths=[missing]))

    def test_missing_file_recorded_as_quality_issue(
        self, profiler: DataProfilerAgent, sample_csv: Path, tmp_path: Path
    ) -> None:
        missing = str(tmp_path / "ghost.csv")
        profile = profiler.profile(
            ProfileRequest(
                data_paths=[str(sample_csv), missing],
                table_names=["real", "missing"],
            )
        )
        # The real table still profiled; the missing one is a quality issue.
        assert profile.tables[0].name == "real"
        assert any("missing" in issue for issue in profile.data_quality_issues)

    def test_high_null_column_flagged(
        self, profiler: DataProfilerAgent, tmp_path: Path
    ) -> None:
        csv = tmp_path / "sparse.csv"
        csv.write_text(
            "id,note\n1,a\n2,\n3,\n4,\n5,\n",  # 4/5 notes null
            encoding="utf-8",
        )
        profile = profiler.profile(ProfileRequest(data_paths=[str(csv)]))
        assert any(
            "note" in issue and "null" in issue.lower()
            for issue in profile.data_quality_issues
        )

    def test_profiles_committed_sample_orders(
        self, profiler: DataProfilerAgent
    ) -> None:
        profile = profiler.profile(ProfileRequest(data_paths=[str(_SAMPLE_CSV)]))
        assert profile.tables[0].row_count == 10
        state_col = next(
            c for c in profile.tables[0].columns if c.name == "customer_state"
        )
        assert state_col.cardinality == "low"


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _col(name: str, unique_count: int = 1) -> ColumnProfile:
    return ColumnProfile(
        name=name,
        dtype="VARCHAR",
        null_count=0,
        null_pct=0.0,
        unique_count=unique_count,
        cardinality="low",
    )


def _table(name: str, columns: list[str]) -> TableProfile:
    return TableProfile(name=name, row_count=3, columns=[_col(c) for c in columns])
