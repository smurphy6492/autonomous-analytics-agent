"""Unit tests for DuckDBEngine."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from analytics_agent.db.engine import DuckDBEngine, DuckDBError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CSV = (
    Path(__file__).parent.parent.parent / "data" / "sample" / "sample_orders.csv"
)


@pytest.fixture
def engine() -> DuckDBEngine:
    """Fresh in-memory DuckDB engine for each test."""
    return DuckDBEngine()


@pytest.fixture
def loaded_engine(engine: DuckDBEngine) -> DuckDBEngine:
    """Engine with the sample orders CSV pre-loaded as 'orders'."""
    engine.load_csv(SAMPLE_CSV, "orders")
    return engine


# ---------------------------------------------------------------------------
# load_csv
# ---------------------------------------------------------------------------


class TestLoadCsv:
    def test_loads_existing_csv(self, engine: DuckDBEngine) -> None:
        engine.load_csv(SAMPLE_CSV, "orders")
        assert "orders" in engine.table_names()

    def test_raises_file_not_found(self, engine: DuckDBEngine, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.csv"
        with pytest.raises(FileNotFoundError, match="CSV not found"):
            engine.load_csv(missing, "t")

    def test_reload_does_not_raise(self, loaded_engine: DuckDBEngine) -> None:
        """Loading the same table twice should not fail (CREATE OR REPLACE VIEW)."""
        loaded_engine.load_csv(SAMPLE_CSV, "orders")
        assert "orders" in loaded_engine.table_names()

    def test_custom_table_name(self, engine: DuckDBEngine) -> None:
        engine.load_csv(SAMPLE_CSV, "my_orders")
        assert "my_orders" in engine.table_names()
        assert engine.get_row_count("my_orders") == 10

    def test_csv_fixture_via_tmp_path(
        self, engine: DuckDBEngine, sample_csv: Path
    ) -> None:
        """Verify the conftest sample_csv fixture also works with the engine."""
        engine.load_csv(sample_csv, "tiny")
        assert engine.get_row_count("tiny") == 3


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------


class TestExecute:
    def test_returns_list_of_dicts(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.execute("SELECT order_id, revenue FROM orders LIMIT 2")
        assert isinstance(rows, list)
        assert len(rows) == 2
        assert "order_id" in rows[0]
        assert "revenue" in rows[0]

    def test_count_query(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.execute("SELECT COUNT(*) AS n FROM orders")
        assert rows[0]["n"] == 10

    def test_aggregation(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.execute(
            "SELECT category, SUM(revenue) AS total "
            "FROM orders GROUP BY category ORDER BY total DESC"
        )
        assert len(rows) == 3
        totals = {r["category"]: float(r["total"]) for r in rows}
        # electronics: 875, clothing: 530, furniture: 740
        assert totals["electronics"] == pytest.approx(875.0)
        assert totals["clothing"] == pytest.approx(530.0)
        assert totals["furniture"] == pytest.approx(740.0)

    def test_empty_result(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.execute("SELECT * FROM orders WHERE revenue > 999999")
        assert rows == []

    def test_bad_sql_raises_duckdb_error(self, loaded_engine: DuckDBEngine) -> None:
        with pytest.raises(DuckDBError, match="Query failed"):
            loaded_engine.execute("SELECT nonexistent_col FROM orders")

    def test_syntax_error_raises_duckdb_error(
        self, loaded_engine: DuckDBEngine
    ) -> None:
        with pytest.raises(DuckDBError):
            loaded_engine.execute("SELEKT * FROM orders")


# ---------------------------------------------------------------------------
# describe_table
# ---------------------------------------------------------------------------


class TestDescribeTable:
    def test_returns_schema_rows(self, loaded_engine: DuckDBEngine) -> None:
        schema = loaded_engine.describe_table("orders")
        assert len(schema) > 0
        col_names = [r["column_name"] for r in schema]
        assert "order_id" in col_names
        assert "revenue" in col_names
        assert "category" in col_names

    def test_missing_table_raises(self, engine: DuckDBEngine) -> None:
        with pytest.raises(DuckDBError):
            engine.describe_table("nonexistent")


# ---------------------------------------------------------------------------
# get_sample_rows
# ---------------------------------------------------------------------------


class TestGetSampleRows:
    def test_default_five_rows(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.get_sample_rows("orders")
        assert len(rows) <= 5

    def test_custom_n(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.get_sample_rows("orders", n=3)
        assert len(rows) == 3

    def test_n_larger_than_table(self, loaded_engine: DuckDBEngine) -> None:
        rows = loaded_engine.get_sample_rows("orders", n=1000)
        assert len(rows) == 10  # only 10 rows in the sample


# ---------------------------------------------------------------------------
# get_row_count
# ---------------------------------------------------------------------------


class TestGetRowCount:
    def test_correct_count(self, loaded_engine: DuckDBEngine) -> None:
        assert loaded_engine.get_row_count("orders") == 10


# ---------------------------------------------------------------------------
# table_names
# ---------------------------------------------------------------------------


class TestTableNames:
    def test_empty_initially(self, engine: DuckDBEngine) -> None:
        assert engine.table_names() == []

    def test_tracks_loaded_tables(self, engine: DuckDBEngine) -> None:
        engine.load_csv(SAMPLE_CSV, "a")
        engine.load_csv(SAMPLE_CSV, "b")
        assert "a" in engine.table_names()
        assert "b" in engine.table_names()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_context_manager_closes(self) -> None:
        with DuckDBEngine() as eng:
            eng.load_csv(SAMPLE_CSV, "orders")
            assert eng.get_row_count("orders") == 10
        # After __exit__ the connection is closed; further queries should fail.
        with pytest.raises((DuckDBError, duckdb.Error)):
            eng.execute("SELECT 1")
