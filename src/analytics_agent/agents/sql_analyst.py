"""SQL Analyst Agent — generates and executes DuckDB SQL with a retry loop."""

from __future__ import annotations

import logging

from pydantic import BaseModel

from analytics_agent.agents.base import AgentError, BaseAgent
from analytics_agent.db.engine import DuckDBEngine, DuckDBError
from analytics_agent.models.profile import DataProfile, TableProfile
from analytics_agent.models.query_plan import QueryResult, SQLRequest

logger = logging.getLogger(__name__)

_MAX_SQL_ATTEMPTS = 3

_SYSTEM_PROMPT = """\
You are a SQL analyst agent. Your job is to write DuckDB-compatible SQL queries that
answer specific analytical questions.

You will receive:
1. The purpose of the query (what business question to answer).
2. A data profile describing the available tables and their columns (names and types).
3. The expected output type (time_series, summary_table, breakdown, other).

Rules:
- Use ONLY the tables and columns listed in the profile.
- Write standard DuckDB SQL (PostgreSQL-compatible syntax).
- Include appropriate GROUP BY and ORDER BY clauses.
- Use TRY_CAST or CAST for type conversions where needed.
- Return only the sql field — no commentary, no explanation.

If a previous attempt failed, you will see the exact error message. Fix exactly
that issue and nothing else.\
"""


class _SQLOutput(BaseModel):
    """Structured wrapper so Claude returns SQL as validated JSON."""

    sql: str


class SQLAnalystAgent:
    """Generates and executes SQL queries with a DuckDB-error retry loop.

    When DuckDB rejects the generated SQL, the error message is fed back to Claude
    in the next attempt (up to :data:`_MAX_SQL_ATTEMPTS` total attempts).  This
    self-correcting loop is the core agentic behaviour of this agent.

    Args:
        base: Configured :class:`~analytics_agent.agents.base.BaseAgent` for Claude
            API access.
        engine: :class:`~analytics_agent.db.engine.DuckDBEngine` instance with the
            relevant tables already loaded.
    """

    def __init__(self, base: BaseAgent, engine: DuckDBEngine) -> None:
        self._base = base
        self._engine = engine

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def execute_query(self, request: SQLRequest) -> QueryResult:
        """Generate SQL and execute it, retrying on DuckDB errors.

        Retry loop (max :data:`_MAX_SQL_ATTEMPTS` attempts):

        1. Ask Claude for SQL given the query plan and data profile.
        2. Execute the SQL with DuckDB.
        3. If DuckDB raises an error, include the error message in the next
           prompt so Claude can correct its SQL.
        4. After all attempts are exhausted, return a failed
           :class:`~analytics_agent.models.query_plan.QueryResult`.

        A :class:`~analytics_agent.agents.base.AgentError` (Claude API failure)
        is not retried here — it is returned immediately as a failed result so
        the pipeline can log it and continue.

        Args:
            request: The planned query to execute.  ``previous_error`` on the
                request seeds the first attempt if the caller already has an
                error context (unusual; normally ``None`` on the first call).

        Returns:
            A :class:`~analytics_agent.models.query_plan.QueryResult` with
            success/failure status, the executed SQL, row data (on success),
            and the number of attempts made.
        """
        previous_error: str | None = request.previous_error
        last_sql = ""

        for attempt in range(1, _MAX_SQL_ATTEMPTS + 1):
            attempt_request = SQLRequest(
                planned_query=request.planned_query,
                data_profile=request.data_profile,
                previous_error=previous_error,
            )

            # --- Generate SQL via Claude. ---
            try:
                sql_output = self._generate_sql(attempt_request)
                last_sql = sql_output.sql.strip()
            except AgentError as exc:
                logger.error(
                    "Agent error generating SQL for '%s' (attempt %d): %s",
                    request.planned_query.query_id,
                    attempt,
                    exc,
                )
                return QueryResult(
                    query_id=request.planned_query.query_id,
                    sql=last_sql,
                    success=False,
                    error=str(exc),
                    attempts=attempt,
                )

            logger.debug(
                "Attempt %d/%d — generated SQL for '%s':\n%s",
                attempt,
                _MAX_SQL_ATTEMPTS,
                request.planned_query.query_id,
                last_sql,
            )

            # --- Execute with DuckDB. ---
            try:
                rows = self._engine.execute(last_sql)
                logger.info(
                    "Query '%s' succeeded on attempt %d (%d rows)",
                    request.planned_query.query_id,
                    attempt,
                    len(rows),
                )
                return QueryResult(
                    query_id=request.planned_query.query_id,
                    sql=last_sql,
                    success=True,
                    data=rows,
                    row_count=len(rows),
                    attempts=attempt,
                )
            except DuckDBError as exc:
                previous_error = str(exc)
                logger.warning(
                    "SQL attempt %d/%d failed for '%s': %s",
                    attempt,
                    _MAX_SQL_ATTEMPTS,
                    request.planned_query.query_id,
                    previous_error,
                )

        logger.error(
            "All %d SQL attempts failed for '%s'",
            _MAX_SQL_ATTEMPTS,
            request.planned_query.query_id,
        )
        return QueryResult(
            query_id=request.planned_query.query_id,
            sql=last_sql,
            success=False,
            error=previous_error,
            attempts=_MAX_SQL_ATTEMPTS,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _generate_sql(self, request: SQLRequest) -> _SQLOutput:
        """Call Claude to generate SQL for *request*.

        Returns:
            A :class:`_SQLOutput` containing the generated SQL string.
        """
        user_prompt = _build_sql_prompt(request)
        return self._base.call_structured(_SYSTEM_PROMPT, user_prompt, _SQLOutput)


# ------------------------------------------------------------------
# Prompt builders (module-level so they can be unit-tested independently)
# ------------------------------------------------------------------


def _build_sql_prompt(request: SQLRequest) -> str:
    """Format a SQL generation prompt from *request*.

    Includes the query purpose, expected output type, relevant schema, and
    (on retry attempts) the error message from the previous attempt.
    """
    pq = request.planned_query
    schema_block = _format_schema(request.data_profile, pq.required_tables)

    lines: list[str] = [
        f"Query purpose: {pq.purpose}",
        f"Output type: {pq.expected_output_type}",
        f"Aggregation grain: {pq.aggregation_grain}",
        "",
        "Available schema:",
        schema_block,
    ]

    if pq.required_columns:
        lines += [
            "",
            f"Key columns to include: {', '.join(pq.required_columns)}",
        ]

    if request.previous_error:
        lines += [
            "",
            "PREVIOUS ATTEMPT FAILED — fix this error exactly:",
            f"  {request.previous_error}",
        ]

    lines += ["", "Return valid DuckDB SQL in the `sql` field."]
    return "\n".join(lines)


def _format_schema(profile: DataProfile, table_names: list[str]) -> str:
    """Format schema information for the tables referenced by the query plan.

    Columns include their dtype and a short sample of values to help Claude
    construct correct SQL (e.g. knowing whether to cast a date column).
    """
    blocks: list[str] = []
    for tname in table_names:
        tbl: TableProfile | None = profile.get_table(tname)
        if tbl is None:
            blocks.append(f"TABLE {tname}: (not found in profile — double-check name)")
            continue
        col_lines = []
        for col in tbl.columns:
            sample_hint = (
                f" [e.g. {', '.join(col.sample_values[:3])}]"
                if col.sample_values
                else ""
            )
            col_lines.append(f"  {col.name} ({col.dtype}){sample_hint}")
        blocks.append(
            f"TABLE {tname} ({tbl.row_count:,} rows):\n" + "\n".join(col_lines)
        )
    return "\n\n".join(blocks)
