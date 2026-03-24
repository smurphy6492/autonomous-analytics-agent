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

_COVERAGE_SYSTEM_PROMPT = """\
You are a quality assurance reviewer for analytics reports. Your job is to check
whether a proposed report layout fully addresses every part of a business question.

You will receive:
1. The original business question.
2. A summary of the charts and tables planned for the report.

Your task: identify any GAPS — parts of the business question that are not
adequately covered by at least one chart or table.

Rules:
- A multi-part question (e.g. "X and Y?") requires EACH part to have dedicated
  visual coverage (chart or table showing a breakdown, not just a mention in the
  summary text).
- A chart/table that only shows a single "dominant value" column (e.g.
  "most_used_payment_type = credit_card" for every row) does NOT count as
  covering that facet — the user needs to see the full distribution.
- If all parts are covered, respond with exactly: PASS
- If there are gaps, respond with: GAPS: followed by a numbered list of what is
  missing, each as a brief actionable instruction for the report builder.

Respond with PASS or GAPS only — no preamble, no explanation beyond the list.\
"""

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

chart_specs: 2-4 chart specifications. Each distinct facet of the business
  question should have at least one chart. For each chart:
  - chart_id: unique snake_case id
  - chart_type: one of line, bar, horizontal_bar, pie, scatter, heatmap
  - title: descriptive title
  - data_source: the query_id that provides the data (must exist in results)
  - x_column / y_column / values_column / names_column: exact column names
    from the query result rows (check the data carefully)
  - Omit columns that are not needed for the chart type
  - max_rows: set this to limit how many rows appear in the chart (e.g. 10 for
    a "top 10" bar chart). The query may return more rows than needed for the
    chart — use max_rows to display only the most relevant subset.
  - bar_norm: set to "percent" for 100% stacked bar charts that show the
    percentage breakdown per group (e.g. payment method share per state).
    Use this when the absolute counts vary widely but the composition matters.

data_tables: 0-2 table specs for data tables to show in the report.
  Each needs a table_id, title, data_source (query_id), and optional columns
  list. Keep max_rows at 20 or fewer.

Important guidelines:
- Be specific and data-driven. Do not make claims unsupported by the data.
- If a query failed (success=false), note this and work with available data.
- Every successful query should be represented in either a chart OR a data
  table. Do not ignore any query result — each was planned for a reason.
- Prefer breakdown charts (bar, pie) over single "dominant value" columns when
  showing categorical distributions (e.g. payment methods, categories). If a
  query returns counts by category, use a chart or detailed table — not a
  single column showing only the top value, which loses the distribution.
- When multiple query results cover different facets of the question, use charts
  for the most visual facets and tables for detailed comparisons.

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

    def validate_coverage(
        self,
        business_question: str,
        synthesis: AnalysisSynthesis,
    ) -> list[str]:
        """Check whether the synthesis covers every facet of the question.

        Makes a lightweight LLM call to review chart/table titles against the
        business question.  Returns an empty list if coverage is adequate, or a
        list of gap descriptions if parts of the question are unaddressed.

        Args:
            business_question: The original question the report answers.
            synthesis: The current synthesis output to review.

        Returns:
            A list of gap descriptions (empty if PASS).
        """
        user_prompt = _build_coverage_prompt(business_question, synthesis)
        logger.info("Validating analytical coverage…")
        response = self._base.call(_COVERAGE_SYSTEM_PROMPT, user_prompt)
        response = response.strip()

        if response.upper().startswith("PASS"):
            logger.info("Coverage validation: PASS")
            return []

        # Parse gaps from response.
        gaps: list[str] = []
        for line in response.splitlines():
            line = line.strip()
            if not line or line.upper().startswith("GAPS"):
                continue
            # Strip leading numbering (e.g. "1. ", "2. ")
            cleaned = line.lstrip("0123456789.) ").strip("- ")
            if cleaned:
                gaps.append(cleaned)

        if gaps:
            logger.warning(
                "Coverage validation found %d gap(s): %s", len(gaps), gaps
            )
        else:
            logger.info("Coverage validation: PASS (no gaps parsed)")

        return gaps


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


def _build_coverage_prompt(
    business_question: str,
    synthesis: AnalysisSynthesis,
) -> str:
    """Format the coverage-validation prompt."""
    lines: list[str] = [
        f"Business question: {business_question}",
        "",
        "Planned report contents:",
    ]

    lines.append("\nCharts:")
    for spec in synthesis.chart_specs:
        extras = []
        if spec.color_column:
            extras.append(f"grouped by {spec.color_column}")
        if spec.bar_norm:
            extras.append(f"normalized={spec.bar_norm}")
        extra_str = f" ({', '.join(extras)})" if extras else ""
        lines.append(
            f"  - [{spec.chart_type}] {spec.title} "
            f"(data: {spec.data_source}, x={spec.x_column}, y={spec.y_column}"
            f"{extra_str})"
        )

    lines.append("\nData tables:")
    if synthesis.data_tables:
        for tbl in synthesis.data_tables:
            cols_str = ", ".join(tbl.columns) if tbl.columns else "all columns"
            lines.append(f"  - {tbl.title} (data: {tbl.data_source}, cols: {cols_str})")
    else:
        lines.append("  (none)")

    lines.append("\nExecutive summary excerpt:")
    lines.append(f"  {synthesis.executive_summary[:300]}")

    lines.append(
        "\nDoes every part of the business question have dedicated chart or "
        "table coverage? Respond with PASS or GAPS."
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
