"""Data Profiler Agent — analyses CSV datasets and returns a structured DataProfile."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from analytics_agent.agents.base import BaseAgent
from analytics_agent.db.engine import DuckDBEngine, DuckDBError
from analytics_agent.models.profile import DataProfile, ProfileRequest

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a data profiler agent. Your job is to analyze dataset statistics and produce
a structured DataProfile that downstream agents use to write correct SQL and choose
appropriate visualizations.

You will receive statistics already computed from the data (row counts, null counts,
unique counts, min/max values, sample values). Your job is to:
1. Format these statistics exactly as provided — do NOT alter any numeric values.
2. Set null_pct = null_count / row_count (as a decimal fraction, e.g. 0.05 for 5%).
3. Classify column cardinality:
   - "low"    → fewer than 20 unique values
   - "medium" → 20 to 1,000 unique values
   - "high"   → more than 1,000 unique values
4. Set is_numeric=true for numeric DuckDB types (INTEGER, BIGINT, FLOAT, DOUBLE,
   DECIMAL, HUGEINT, UBIGINT, SMALLINT, TINYINT, etc.)
5. Set is_date=true for DATE or TIMESTAMP types, and for VARCHAR/TEXT columns whose
   sample values match common date patterns (YYYY-MM-DD, YYYY/MM/DD, etc.).
6. Detect foreign-key relationships between tables by matching column names
   (e.g., orders.customer_id references customers.customer_id if both exist).
7. Suggest the analysis grain — the column(s) that most granularly identify a record.
8. List data quality issues worth flagging (e.g., >5% nulls on a key column,
   suspiciously low cardinality for an ID column, etc.). Use an empty list if none.

Respond with a DataProfile JSON object only — no explanation, no markdown fences.\
"""


class DataProfilerAgent:
    """Profiles one or more CSV datasets using DuckDB statistics + Claude.

    The agent collects all numeric statistics (row counts, null counts, unique counts,
    min/max values, sample values) directly from DuckDB, then passes these to Claude
    to produce the final :class:`~analytics_agent.models.profile.DataProfile` with
    semantic annotations (cardinality labels, date/numeric flags, detected
    relationships, suggested analysis grain).

    Args:
        base: Configured :class:`~analytics_agent.agents.base.BaseAgent` for Claude
            API access.
        engine: :class:`~analytics_agent.db.engine.DuckDBEngine` instance.  The
            engine may already have tables loaded; :meth:`profile` will load any
            tables specified in the request.
    """

    def __init__(self, base: BaseAgent, engine: DuckDBEngine) -> None:
        self._base = base
        self._engine = engine

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def profile(self, request: ProfileRequest) -> DataProfile:
        """Profile the datasets specified in *request*.

        Steps:
        1. Load each CSV into DuckDB (using ``request.table_names`` if provided,
           otherwise the filename stem).
        2. Collect raw statistics from DuckDB for every table and column.
        3. Call Claude with the statistics to produce a validated
           :class:`~analytics_agent.models.profile.DataProfile`.

        Args:
            request: Paths to CSV files and optional custom table names.

        Returns:
            A validated :class:`~analytics_agent.models.profile.DataProfile` with
            schema, statistics, and semantic annotations for every table.

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

        # --- Load CSVs; collect per-file quality issues rather than aborting. ---
        quality_issues: list[str] = []
        loaded: list[str] = []
        for path, tname in zip(paths, table_names, strict=True):
            try:
                self._engine.load_csv(path, tname)
                loaded.append(tname)
            except (FileNotFoundError, DuckDBError) as exc:
                quality_issues.append(f"Could not load '{tname}': {exc}")
                logger.warning("Skipping '%s': %s", tname, exc)

        if not loaded:
            raise ValueError("No tables could be loaded. Check file paths.")

        # --- Collect raw statistics (no LLM involved). ---
        raw_stats = [self._collect_table_stats(tname) for tname in loaded]

        # --- Ask Claude to produce the structured DataProfile. ---
        user_prompt = _build_profile_prompt(raw_stats, quality_issues or None)
        logger.info("Profiling %d table(s) via Claude…", len(loaded))
        return self._base.call_structured(_SYSTEM_PROMPT, user_prompt, DataProfile)

    # ------------------------------------------------------------------
    # Stats collection (purely programmatic, no LLM)
    # ------------------------------------------------------------------

    def _collect_table_stats(self, table_name: str) -> dict[str, Any]:
        """Collect raw statistics for *table_name* using DuckDB SQL.

        Returns a plain dict (not a Pydantic model) so it can be serialised
        directly into the prompt without going through model validation first.
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
# Prompt builder (module-level so it can be unit-tested independently)
# ------------------------------------------------------------------


def _build_profile_prompt(
    raw_stats: list[dict[str, Any]],
    existing_quality_issues: list[str] | None = None,
) -> str:
    """Format raw DuckDB statistics as a text prompt for Claude.

    Args:
        raw_stats: List of table-level stat dicts from
            :meth:`DataProfilerAgent._collect_table_stats`.
        existing_quality_issues: Pre-loading issues to include in the prompt
            (e.g., files that could not be read).

    Returns:
        A human-readable prompt string ready to pass to Claude.
    """
    lines: list[str] = [
        "DATASET STATISTICS (preserve all numeric values exactly as shown):",
        "=" * 70,
        "",
    ]

    for tbl in raw_stats:
        row_count: int = int(tbl["row_count"])
        lines.append(f"TABLE: {tbl['table_name']}  ({row_count:,} rows)")
        lines.append("Columns:")
        for col in tbl["columns"]:
            null_pct = col["null_count"] / row_count if row_count > 0 else 0.0
            sample_str = (
                ", ".join(col["sample_values"][:5])
                if col["sample_values"]
                else "(no non-null values)"
            )
            stat_parts: list[str] = [
                f"null_count={col['null_count']}",
                f"null_pct={null_pct:.4f}",
                f"unique={col['unique_count']}",
                f"sample=[{sample_str}]",
            ]
            if col["min_value"] is not None:
                stat_parts.append(f"min={col['min_value']}")
            if col["max_value"] is not None:
                stat_parts.append(f"max={col['max_value']}")
            lines.append(f"  - {col['name']} ({col['type']}): {', '.join(stat_parts)}")
        lines.append("")

    if existing_quality_issues:
        lines.append("Pre-loading issues detected:")
        for issue in existing_quality_issues:
            lines.append(f"  - {issue}")
        lines.append("")

    lines.append(
        "Produce a complete DataProfile JSON object for all tables listed above."
    )
    return "\n".join(lines)
