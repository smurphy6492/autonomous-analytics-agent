"""DuckDB engine — connection management, CSV loading, and query execution."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


class DuckDBError(Exception):
    """Raised when a DuckDB operation fails."""


class DuckDBEngine:
    """Thin wrapper around a DuckDB in-memory connection.

    Responsibilities:
    - Load CSV files as named tables.
    - Execute SQL and return results as ``list[dict]``.
    - Provide schema and sample-row helpers for prompt construction.
    - Wrap DuckDB exceptions with descriptive messages.

    Usage::

        engine = DuckDBEngine()
        engine.load_csv(Path("orders.csv"), "orders")
        rows = engine.execute("SELECT COUNT(*) AS n FROM orders")
        schema = engine.describe_table("orders")
    """

    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        self._loaded_tables: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_csv(self, path: Path, table_name: str) -> None:
        """Register a CSV file as a DuckDB table.

        DuckDB's ``read_csv_auto`` infers column types automatically.

        Args:
            path: Absolute or relative path to the CSV file.
            table_name: Name to assign to the resulting virtual table.

        Raises:
            FileNotFoundError: If the CSV does not exist.
            DuckDBError: If DuckDB cannot parse the file.
        """
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}")

        try:
            # Use a CREATE OR REPLACE VIEW so re-loading doesn't fail.
            safe_path = str(path).replace("\\", "/")
            sql = (
                f"CREATE OR REPLACE VIEW {table_name} AS "
                f"SELECT * FROM read_csv_auto('{safe_path}')"
            )
            self._conn.execute(sql)
            self._loaded_tables.add(table_name)
            logger.debug("Loaded CSV %s as table '%s'", path, table_name)
        except duckdb.Error as exc:
            raise DuckDBError(
                f"Failed to load '{path}' as table '{table_name}': {exc}"
            ) from exc

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return all rows as a list of dicts.

        Args:
            sql: DuckDB-compatible SQL statement.

        Returns:
            List of row dicts, keyed by column name.  Empty list for queries
            that return no rows.

        Raises:
            DuckDBError: If the query fails.
        """
        try:
            relation = self._conn.execute(sql)
            return relation.fetchdf().to_dict(orient="records")  # type: ignore[return-value]
        except duckdb.Error as exc:
            raise DuckDBError(f"Query failed: {exc}\nSQL: {sql}") from exc

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        """Return schema information for *table_name*.

        Each row has ``column_name``, ``column_type``, ``null``, ``key``,
        ``default``, ``extra`` — the columns returned by DuckDB ``DESCRIBE``.

        Args:
            table_name: Name of a previously loaded table.

        Raises:
            DuckDBError: If the table does not exist or the query fails.
        """
        return self.execute(f"DESCRIBE {table_name}")

    def get_sample_rows(self, table_name: str, n: int = 5) -> list[dict[str, Any]]:
        """Return up to *n* sample rows from *table_name*.

        Args:
            table_name: Name of a previously loaded table.
            n: Number of rows to return (default 5).

        Raises:
            DuckDBError: If the table does not exist or the query fails.
        """
        return self.execute(f"SELECT * FROM {table_name} LIMIT {n}")

    def get_row_count(self, table_name: str) -> int:
        """Return the exact row count for *table_name*.

        Raises:
            DuckDBError: If the table does not exist or the query fails.
        """
        rows = self.execute(f"SELECT COUNT(*) AS n FROM {table_name}")
        return int(rows[0]["n"])

    def table_names(self) -> list[str]:
        """Return the names of all tables/views registered in this session."""
        return sorted(self._loaded_tables)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()
        logger.debug("DuckDB connection closed")

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> DuckDBEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
