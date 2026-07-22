"""Data Profiler — builds a structured DataProfile deterministically from DuckDB.

There is no LLM in this module, by design. Profiling is entirely mechanical:
row counts, null counts, distinct counts, min/max, cardinality buckets, and
numeric/date flags are all computed from DuckDB or from simple rules. An earlier
version handed the already-computed statistics to Claude and asked it to copy
them back verbatim — that put a non-deterministic model in the path of
ground-truth numbers for no benefit and a real corruption risk. The only parts
that ever needed judgement (cardinality bucketing, date detection, relationship
and grain inference) are deterministic rules, so the model is gone.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

from analytics_agent.db.engine import DuckDBEngine, DuckDBError
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    ProfileRequest,
    Relationship,
    TableProfile,
)

logger = logging.getLogger(__name__)

# Cardinality thresholds (unique value counts).
_LOW_CARDINALITY_MAX = 20
_MEDIUM_CARDINALITY_MAX = 1000
# Flag a column as a data-quality concern above this null fraction.
_NULL_WARNING_THRESHOLD = 0.05
# Substrings that mark a DuckDB type as numeric ("INT" covers INTEGER, BIGINT,
# TINYINT, SMALLINT, HUGEINT, UINTEGER, ...).
_NUMERIC_TYPE_TOKENS = ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "NUMERIC")
# A sample value that looks like an ISO-ish date (YYYY-MM-DD or YYYY/MM/DD ...).
_DATE_SAMPLE_RE = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}")


class DataProfilerAgent:
    """Profiles one or more CSV datasets into a typed DataProfile using DuckDB.

    All statistics are collected directly from DuckDB and all annotations
    (cardinality, date/numeric flags, relationships, suggested grain, quality
    issues) are derived by deterministic rules. No Claude API call is made.

    Args:
        engine: :class:`~analytics_agent.db.engine.DuckDBEngine` instance. The
            engine may already have tables loaded; :meth:`profile` loads any
            tables named in the request.
    """

    def __init__(self, engine: DuckDBEngine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def profile(self, request: ProfileRequest) -> DataProfile:
        """Profile the datasets in *request* and return a DataProfile.

        Args:
            request: Paths to CSV files and optional custom table names.

        Returns:
            A validated :class:`~analytics_agent.models.profile.DataProfile`.

        Raises:
            ValueError: If no tables could be loaded or lengths are mismatched.
        """
        paths = [Path(p) for p in request.data_paths]
        table_names = request.table_names or [p.stem for p in paths]

        if len(paths) != len(table_names):
            raise ValueError(
                f"data_paths ({len(paths)}) and table_names ({len(table_names)}) "
                "must have the same length."
            )

        # --- Load CSVs; collect per-file issues rather than aborting. ---
        preloading_issues: list[str] = []
        loaded: list[str] = []
        for path, tname in zip(paths, table_names, strict=True):
            try:
                self._engine.load_csv(path, tname)
                loaded.append(tname)
            except (FileNotFoundError, DuckDBError) as exc:
                preloading_issues.append(f"Could not load '{tname}': {exc}")
                logger.warning("Skipping '%s': %s", tname, exc)

        if not loaded:
            raise ValueError("No tables could be loaded. Check file paths.")

        logger.info("Profiling %d table(s) deterministically…", len(loaded))
        tables = [self._build_table_profile(tname) for tname in loaded]

        return DataProfile(
            tables=tables,
            relationships=_detect_relationships(tables),
            suggested_grain=_suggest_grain(tables),
            data_quality_issues=_quality_issues(tables, preloading_issues),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_table_profile(self, table_name: str) -> TableProfile:
        """Build a TableProfile for *table_name* from DuckDB statistics."""
        stats = self._collect_table_stats(table_name)
        row_count: int = int(stats["row_count"])

        columns: list[ColumnProfile] = []
        for col in stats["columns"]:
            null_count = int(col["null_count"])
            null_pct = null_count / row_count if row_count > 0 else 0.0
            sample_values = [str(v) for v in col["sample_values"]][:10]
            columns.append(
                ColumnProfile(
                    name=col["name"],
                    dtype=col["type"],
                    null_count=null_count,
                    null_pct=null_pct,
                    unique_count=int(col["unique_count"]),
                    cardinality=_cardinality(int(col["unique_count"])),
                    sample_values=sample_values,
                    is_date=_is_date(col["type"], sample_values),
                    is_numeric=_is_numeric(col["type"]),
                    min_value=col["min_value"],
                    max_value=col["max_value"],
                )
            )

        return TableProfile(name=table_name, row_count=row_count, columns=columns)

    def _collect_table_stats(self, table_name: str) -> dict[str, Any]:
        """Collect raw statistics for *table_name* using DuckDB SQL.

        Returns a plain dict (not a Pydantic model) with row count and, per
        column, name/type/null_count/unique_count/sample_values/min/max.
        """
        row_count = self._engine.get_row_count(table_name)
        schema = self._engine.describe_table(table_name)

        columns: list[dict[str, Any]] = []
        for col_info in schema:
            col_name: str = str(col_info["column_name"])
            col_type: str = str(col_info["column_type"])
            quoted = f'"{col_name}"'

            # Null count and unique count in a single pass.
            stats = self._engine.execute(
                f"SELECT COUNT(*) - COUNT({quoted}) AS null_count, "
                f"COUNT(DISTINCT {quoted}) AS unique_count "
                f"FROM {table_name}"
            )[0]
            null_count = int(stats["null_count"] or 0)
            unique_count = int(stats["unique_count"] or 0)

            # Up to 5 non-null sample values, cast to string.
            sample_rows = self._engine.execute(
                f"SELECT CAST({quoted} AS VARCHAR) AS v "
                f"FROM {table_name} "
                f"WHERE {quoted} IS NOT NULL LIMIT 5"
            )
            sample_values = [str(r["v"]) for r in sample_rows]

            # Min and max values (as strings) where supported.
            min_val: str | None = None
            max_val: str | None = None
            try:
                minmax = self._engine.execute(
                    f"SELECT CAST(MIN({quoted}) AS VARCHAR) AS mn, "
                    f"CAST(MAX({quoted}) AS VARCHAR) AS mx "
                    f"FROM {table_name}"
                )[0]
                mn = minmax.get("mn")
                mx = minmax.get("mx")
                min_val = str(mn) if mn is not None else None
                max_val = str(mx) if mx is not None else None
            except DuckDBError:
                pass  # Some types don't support MIN/MAX; skip silently.

            columns.append(
                {
                    "name": col_name,
                    "type": col_type,
                    "null_count": null_count,
                    "unique_count": unique_count,
                    "sample_values": sample_values,
                    "min_value": min_val,
                    "max_value": max_val,
                }
            )

        return {
            "table_name": table_name,
            "row_count": row_count,
            "columns": columns,
        }


# ------------------------------------------------------------------
# Deterministic annotation rules (module-level, independently testable)
# ------------------------------------------------------------------


def _cardinality(unique_count: int) -> Literal["low", "medium", "high"]:
    """Bucket a distinct-value count into low/medium/high cardinality."""
    if unique_count < _LOW_CARDINALITY_MAX:
        return "low"
    if unique_count <= _MEDIUM_CARDINALITY_MAX:
        return "medium"
    return "high"


def _is_numeric(dtype: str) -> bool:
    """Return True if *dtype* is a numeric DuckDB type."""
    upper = dtype.upper()
    return any(token in upper for token in _NUMERIC_TYPE_TOKENS)


def _is_date(dtype: str, sample_values: list[str]) -> bool:
    """Return True for DATE/TIMESTAMP types or text columns of date-like values."""
    upper = dtype.upper()
    if "DATE" in upper or "TIMESTAMP" in upper:
        return True
    if "CHAR" in upper or "TEXT" in upper or "STRING" in upper:
        non_empty = [v for v in sample_values if v]
        return bool(non_empty) and all(_DATE_SAMPLE_RE.match(v) for v in non_empty)
    return False


def _detect_relationships(tables: list[TableProfile]) -> list[Relationship]:
    """Infer foreign-key relationships from id-like columns shared across tables.

    A column named ``id`` or ending in ``_id`` that appears in more than one
    table is treated as a join key. The first table containing it is taken as
    the referenced (parent) side; the others reference it. This is a heuristic
    hint for the query planner, not a verified schema constraint, hence the
    modest confidence.
    """
    col_to_tables: dict[str, list[str]] = defaultdict(list)
    for table in tables:
        for col in table.columns:
            col_to_tables[col.name].append(table.name)

    relationships: list[Relationship] = []
    for col_name, owning_tables in col_to_tables.items():
        if len(owning_tables) < 2:
            continue
        if not (col_name == "id" or col_name.endswith("_id")):
            continue
        parent = owning_tables[0]
        for child in owning_tables[1:]:
            relationships.append(
                Relationship(
                    from_table=child,
                    from_column=col_name,
                    to_table=parent,
                    to_column=col_name,
                    confidence=0.5,
                )
            )
    return relationships


def _suggest_grain(tables: list[TableProfile]) -> str:
    """Suggest the analysis grain — the column that most granularly keys a row.

    Prefers a column in the largest table whose distinct count equals the row
    count (a unique key), then any id-like column, then the first column.
    """
    if not tables:
        return ""
    table = max(tables, key=lambda t: t.row_count)
    for col in table.columns:
        if table.row_count > 0 and col.unique_count == table.row_count:
            return col.name
    for col in table.columns:
        if col.name == "id" or col.name.endswith("_id"):
            return col.name
    return table.columns[0].name if table.columns else ""


def _quality_issues(
    tables: list[TableProfile],
    preloading_issues: list[str],
) -> list[str]:
    """Collect data-quality warnings: pre-loading failures plus high-null columns."""
    issues = list(preloading_issues)
    for table in tables:
        for col in table.columns:
            if col.null_pct > _NULL_WARNING_THRESHOLD:
                issues.append(
                    f"{table.name}.{col.name} is {col.null_pct:.0%} null "
                    f"({col.null_count} of {table.row_count} rows)"
                )
    return issues
