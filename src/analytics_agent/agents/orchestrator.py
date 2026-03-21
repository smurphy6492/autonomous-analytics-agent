"""Analytics Orchestrator Agent — query planning and results synthesis."""

from __future__ import annotations

import json
import logging

from analytics_agent.agents.base import BaseAgent
from analytics_agent.models.query_plan import (
    PlannedQuery,
    QueryPlan,
    QueryPlanRequest,
    QueryResult,
)
from analytics_agent.models.report import AnalysisSynthesis, SynthesisRequest

logger = logging.getLogger(__name__)

_PLAN_SYSTEM_PROMPT = """\
You are an analytics orchestrator agent. Your job is to decide what SQL analyses
are needed to answer a business question given a structured data profile.

You will receive:
1. The business question to answer.
2. A DataProfile describing all available tables, their columns (names, types,
   cardinality), and detected relationships between tables.

Your output is a QueryPlan with 2-4 planned queries. Each planned query must:
- Have a unique query_id (snake_case, e.g. "monthly_revenue", "category_breakdown").
- State its purpose clearly (what specific question it answers).
- List only tables and columns that exist in the profile.
- Specify the aggregation_grain (e.g. "monthly", "by_category", "by_state").
- Classify the expected_output_type as one of: time_series, summary_table,
  breakdown, other.

Think step-by-step: what are the key dimensions and metrics needed to answer the
question? Then define the minimal set of queries that covers them.

Respond with a QueryPlan JSON object only — no explanation, no markdown fences.\
"""

_SYNTHESIS_SYSTEM_PROMPT = """\
You are an analytics orchestrator agent. Your job is to synthesize SQL query
results into a professional analytical report.

You will receive:
1. The original business question.
2. The query plan (what analyses were performed and why).
3. The actual query results as JSON.

Your output is an AnalysisSynthesis with:

executive_summary: 3-5 sentences that directly answer the business question
  with specific numbers from the data. Be concrete — cite actual figures.

key_metrics: 3-5 highlighted numbers (label + value string + optional context).
  Format values clearly, e.g. "$1.2M", "14.3%", "42 days".

chart_specs: 1-3 chart specifications. For each chart:
  - chart_id: unique snake_case id
  - chart_type: one of line, bar, horizontal_bar, pie, scatter, heatmap
  - title: descriptive title
  - data_source: the query_id that provides the data (must exist in results)
  - x_column / y_column / values_column / names_column: exact column names
    from the query result rows (check the data carefully)
  - Omit columns that are not needed for the chart type

data_tables: 0-2 table specs for data tables to show in the report.
  Each needs a table_id, title, data_source (query_id), and optional columns
  list. Keep max_rows at 20 or fewer.

Be specific and data-driven. Do not make claims unsupported by the data.
If a query failed (success=false), note this and work with available data.

Respond with an AnalysisSynthesis JSON object only — no explanation, no markdown.\
"""


class OrchestratorAgent:
    """Plans analytical queries and synthesizes results into report-ready output.

    This agent is called twice per pipeline run:

    1. :meth:`plan_queries` — takes a business question + data profile and returns
       a :class:`~analytics_agent.models.query_plan.QueryPlan` (what SQL to run).

    2. :meth:`synthesize` — takes the query results and returns an
       :class:`~analytics_agent.models.report.AnalysisSynthesis` (summary, metrics,
       chart specs, data table specs).

    Args:
        base: Configured :class:`~analytics_agent.agents.base.BaseAgent` for Claude
            API access.
    """

    def __init__(self, base: BaseAgent) -> None:
        self._base = base

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def plan_queries(self, request: QueryPlanRequest) -> QueryPlan:
        """Produce a :class:`~analytics_agent.models.query_plan.QueryPlan`.

        Args:
            request: The business question and data profile.

        Returns:
            A validated QueryPlan with 2-4 planned queries.

        Raises:
            AgentError: If the Claude API call fails after retries.
        """
        user_prompt = _build_plan_prompt(request)
        logger.info(
            "Orchestrator planning queries for question: %.80s…",
            request.business_question,
        )
        plan = self._base.call_structured(_PLAN_SYSTEM_PROMPT, user_prompt, QueryPlan)
        logger.info(
            "Query plan produced: %d queries — %s",
            len(plan.queries),
            [q.query_id for q in plan.queries],
        )
        return plan

    def synthesize(self, request: SynthesisRequest) -> AnalysisSynthesis:
        """Synthesize query results into an AnalysisSynthesis.

        Args:
            request: The business question, query plan, and query results.

        Returns:
            A validated AnalysisSynthesis with executive summary, key metrics,
            chart specs, and data table specs.

        Raises:
            AgentError: If the Claude API call fails after retries.
        """
        user_prompt = _build_synthesis_prompt(request)
        logger.info(
            "Orchestrator synthesizing %d query result(s)…",
            len(request.query_results),
        )
        synthesis = self._base.call_structured(
            _SYNTHESIS_SYSTEM_PROMPT, user_prompt, AnalysisSynthesis
        )
        logger.info(
            "Synthesis complete: %d metrics, %d charts, %d tables",
            len(synthesis.key_metrics),
            len(synthesis.chart_specs),
            len(synthesis.data_tables),
        )
        return synthesis


# ------------------------------------------------------------------
# Prompt builders (module-level so they can be unit-tested independently)
# ------------------------------------------------------------------


def _build_plan_prompt(request: QueryPlanRequest) -> str:
    """Format the query-planning prompt from the request.

    Includes the business question and a concise text representation of the
    data profile (tables, columns, types, cardinality, sample values).
    """
    profile = request.data_profile

    lines: list[str] = [
        f"Business question: {request.business_question}",
        "",
        "Available data profile:",
        "=" * 60,
    ]

    for tbl in profile.tables:
        lines.append(f"\nTABLE: {tbl.name}  ({tbl.row_count:,} rows)")
        for col in tbl.columns:
            flags: list[str] = []
            if col.is_date:
                flags.append("date")
            if col.is_numeric:
                flags.append("numeric")
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            sample_str = (
                f" e.g. {', '.join(col.sample_values[:3])}" if col.sample_values else ""
            )
            lines.append(
                f"  {col.name} ({col.dtype}, cardinality={col.cardinality})"
                f"{flag_str}{sample_str}"
            )

    if profile.relationships:
        lines.append("\nDetected relationships:")
        for rel in profile.relationships:
            lines.append(
                f"  {rel.from_table}.{rel.from_column} → {rel.to_table}.{rel.to_column}"
            )

    if profile.suggested_grain:
        lines.append(f"\nSuggested analysis grain: {profile.suggested_grain}")

    if profile.data_quality_issues:
        lines.append("\nData quality warnings:")
        for issue in profile.data_quality_issues:
            lines.append(f"  - {issue}")

    lines.append(
        "\nProduce a QueryPlan JSON object with 2-4 queries to answer "
        "the business question."
    )
    return "\n".join(lines)


def _build_synthesis_prompt(request: SynthesisRequest) -> str:
    """Format the synthesis prompt from the request.

    Includes the business question, query plan, and all query results
    (success and failure) as JSON so Claude can reference actual values.
    """
    lines: list[str] = [
        f"Business question: {request.business_question}",
        "",
        f"Analysis approach: {request.query_plan.analysis_approach}",
        "",
        "Query plan:",
    ]

    for q in request.query_plan.queries:
        lines.append(
            f"  [{q.query_id}] {q.purpose} "
            f"(grain={q.aggregation_grain}, type={q.expected_output_type})"
        )

    lines.append("\nQuery results:")
    lines.append("=" * 60)

    for query_id, result in request.query_results.items():
        if result.success and result.data:
            # Show first 30 rows as JSON for Claude to reference.
            preview = result.data[:30]
            lines.append(
                f"\nquery_id={query_id}  status=SUCCESS  rows={result.row_count}"
            )
            lines.append(f"SQL: {result.sql}")
            lines.append("Data (first 30 rows):")
            lines.append(json.dumps(preview, default=str, indent=2))
        else:
            lines.append(f"\nquery_id={query_id}  status=FAILED  error={result.error}")

    lines.append(
        "\nProduce an AnalysisSynthesis JSON object. "
        "Column names in chart_specs must match exactly those in the query data."
    )
    return "\n".join(lines)


def _format_planned_queries(queries: list[PlannedQuery]) -> str:
    """Format a list of planned queries for logging or display."""
    return ", ".join(q.query_id for q in queries)


def _format_result_summary(results: dict[str, QueryResult]) -> str:
    """Format query result summary for logging."""
    parts = []
    for qid, r in results.items():
        status = f"OK({r.row_count})" if r.success else f"FAIL({r.error})"
        parts.append(f"{qid}={status}")
    return ", ".join(parts)
