"""Pipeline Runner — orchestrates the full analytics pipeline end-to-end."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path

import anthropic

from analytics_agent.agents.base import AgentError, BaseAgent
from analytics_agent.agents.data_profiler import DataProfilerAgent
from analytics_agent.agents.orchestrator import OrchestratorAgent
from analytics_agent.agents.sql_analyst import SQLAnalystAgent
from analytics_agent.agents.viz_agent import VizAgent
from analytics_agent.config import Settings
from analytics_agent.db.engine import DuckDBEngine
from analytics_agent.models.profile import ProfileRequest
from analytics_agent.models.query_plan import QueryPlanRequest, SQLRequest
from analytics_agent.models.report import AnalysisReport, SynthesisRequest
from analytics_agent.pipeline.context import PipelineContext
from analytics_agent.report.builder import ReportBuilder

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Runs the full analytics pipeline from CSV paths + question → HTML report.

    Steps:
    1. Load CSVs into DuckDB.
    2. Profile datasets (DataProfilerAgent).
    3. Plan SQL queries (OrchestratorAgent.plan_queries).
    4. Execute queries with retry (SQLAnalystAgent).
    5. Synthesise results into insights + chart specs (OrchestratorAgent.synthesize).
    6. Render charts (VizAgent).
    7. Assemble and write HTML report (ReportBuilder).

    Args:
        settings: Application configuration (API key, model, paths).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine = DuckDBEngine()
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        base = BaseAgent(
            client=client,
            model=settings.model,
            cache_dir=settings.cache_dir,
        )
        self._profiler = DataProfilerAgent(base=base, engine=self._engine)
        self._orchestrator = OrchestratorAgent(base=base)
        self._sql_analyst = SQLAnalystAgent(base=base, engine=self._engine)
        self._viz_agent = VizAgent()
        self._report_builder = ReportBuilder()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(
        self,
        data_paths: list[Path],
        business_question: str,
        title: str = "Analytics Report",
        output_path: Path | None = None,
    ) -> AnalysisReport:
        """Execute the pipeline and write the HTML report to *output_path*.

        Args:
            data_paths: Paths to CSV files to analyse.
            business_question: The business question to answer.
            title: Report title (used in HTML header).
            output_path: Where to write the HTML file.  Defaults to
                ``<settings.output_dir>/<title_slug>.html``.

        Returns:
            The assembled :class:`~analytics_agent.models.report.AnalysisReport`.

        Raises:
            ValueError: If ``data_paths`` is empty or no files can be loaded.
            AgentError: If a critical agent call fails after all retries.
        """
        if not data_paths:
            raise ValueError("data_paths must not be empty.")

        resolved = [Path(p) for p in data_paths]
        table_names = [p.stem for p in resolved]

        if output_path is None:
            output_path = self._settings.output_dir / f"{_slugify(title)}.html"

        ctx = PipelineContext(
            data_paths=resolved,
            table_names=table_names,
            business_question=business_question,
            output_path=output_path,
            title=title,
        )
        ctx.start_time = datetime.now(UTC)
        logger.info("Pipeline started — question: %.120s", business_question)

        self._step_profile(ctx)
        self._step_plan(ctx)
        self._step_execute(ctx)
        self._step_synthesise(ctx)
        self._step_render_charts(ctx)

        ctx.end_time = datetime.now(UTC)
        report = self._assemble_report(ctx)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._report_builder.write(report, output_path)
        logger.info(
            "Pipeline complete in %dms — report written to %s",
            ctx.elapsed_ms(),
            output_path,
        )
        return report

    # ------------------------------------------------------------------
    # Pipeline steps
    # ------------------------------------------------------------------

    def _step_profile(self, ctx: PipelineContext) -> None:
        """Step 1 — Profile all CSV datasets."""
        logger.info("Step 1/6: Profiling %d dataset(s)…", len(ctx.data_paths))
        try:
            request = ProfileRequest(
                data_paths=[str(p) for p in ctx.data_paths],
                table_names=ctx.table_names,
            )
            ctx.profile = self._profiler.profile(request)
            logger.info(
                "Profiling complete: %d table(s), %d quality issue(s)",
                len(ctx.profile.tables),
                len(ctx.profile.data_quality_issues),
            )
        except (ValueError, AgentError) as exc:
            # Profiling failure is fatal — cannot continue without a schema.
            ctx.record_error(f"Profiling failed: {exc}")
            raise

    def _step_plan(self, ctx: PipelineContext) -> None:
        """Step 2 — Plan analytical queries."""
        if ctx.profile is None:
            raise RuntimeError("Cannot plan queries: profiling step did not complete")
        logger.info("Step 2/6: Planning queries…")
        try:
            request = QueryPlanRequest(
                business_question=ctx.business_question,
                data_profile=ctx.profile,
            )
            ctx.query_plan = self._orchestrator.plan_queries(request)
            logger.info(
                "Query plan: %d queries — %s",
                len(ctx.query_plan.queries),
                [q.query_id for q in ctx.query_plan.queries],
            )
        except AgentError as exc:
            ctx.record_error(f"Query planning failed: {exc}")
            raise

    def _step_execute(self, ctx: PipelineContext) -> None:
        """Step 3 — Execute all planned queries (with retry per query)."""
        if ctx.profile is None:
            raise RuntimeError("Cannot execute: profiling step did not complete")
        if ctx.query_plan is None:
            raise RuntimeError("Cannot execute: planning step did not complete")
        logger.info(
            "Step 3/6: Executing %d query/queries…",
            len(ctx.query_plan.queries),
        )
        for planned in ctx.query_plan.queries:
            request = SQLRequest(
                planned_query=planned,
                data_profile=ctx.profile,
            )
            result = self._sql_analyst.execute_query(request)
            ctx.query_results[planned.query_id] = result
            if result.success:
                logger.info(
                    "Query '%s' succeeded (%d rows, %d attempt(s))",
                    planned.query_id,
                    result.row_count,
                    result.attempts,
                )
            else:
                msg = (
                    f"Query '{planned.query_id}' failed after "
                    f"{result.attempts} attempt(s): {result.error}"
                )
                logger.warning(msg)
                ctx.record_error(msg)

        successful = sum(1 for r in ctx.query_results.values() if r.success)
        logger.info(
            "%d/%d queries succeeded",
            successful,
            len(ctx.query_plan.queries),
        )

    def _step_synthesise(self, ctx: PipelineContext) -> None:
        """Step 4 — Synthesise query results into insights + chart specs."""
        if ctx.query_plan is None:
            raise RuntimeError("Cannot synthesise: planning step did not complete")
        logger.info("Step 4/6: Synthesising results…")
        try:
            request = SynthesisRequest(
                business_question=ctx.business_question,
                query_plan=ctx.query_plan,
                query_results=ctx.query_results,
            )
            ctx.synthesis = self._orchestrator.synthesize(request)
            logger.info(
                "Synthesis complete: %d key metrics, %d chart specs",
                len(ctx.synthesis.key_metrics),
                len(ctx.synthesis.chart_specs),
            )
        except AgentError as exc:
            ctx.record_error(f"Synthesis failed: {exc}")
            raise

    def _step_render_charts(self, ctx: PipelineContext) -> None:
        """Step 5 — Render Plotly charts from chart specs."""
        if ctx.synthesis is None:
            raise RuntimeError("Cannot render charts: synthesis step did not complete")
        specs = ctx.synthesis.chart_specs
        logger.info("Step 5/6: Rendering %d chart(s)…", len(specs))
        for spec in specs:
            source_result = ctx.query_results.get(spec.data_source)
            if source_result is None or not source_result.success:
                logger.warning(
                    "Chart '%s' skipped — data source '%s' unavailable",
                    spec.chart_id,
                    spec.data_source,
                )
                ctx.record_error(
                    f"Chart '{spec.chart_id}' skipped: "
                    f"data source '{spec.data_source}' has no results"
                )
                continue
            rendered = self._viz_agent.render(spec, source_result.data or [])
            ctx.rendered_charts.append(rendered)

        successful = sum(1 for c in ctx.rendered_charts if c.success)
        logger.info(
            "Step 5/6 complete: %d/%d charts rendered successfully",
            successful,
            len(ctx.rendered_charts),
        )

    # ------------------------------------------------------------------
    # Report assembly
    # ------------------------------------------------------------------

    def _assemble_report(self, ctx: PipelineContext) -> AnalysisReport:
        """Assemble pipeline context into an AnalysisReport."""
        if ctx.query_plan is None or ctx.synthesis is None:
            raise RuntimeError("Cannot assemble report: pipeline did not complete")

        logger.info("Step 6/6: Assembling report…")
        return AnalysisReport(
            title=ctx.title,
            business_question=ctx.business_question,
            executive_summary=ctx.synthesis.executive_summary,
            key_metrics=ctx.synthesis.key_metrics,
            rendered_charts=ctx.rendered_charts,
            data_tables=ctx.synthesis.data_tables,
            query_plan=ctx.query_plan,
            query_results=ctx.query_results,
            data_sources=ctx.table_names,
            analysis_approach=ctx.query_plan.analysis_approach,
            execution_time_ms=ctx.elapsed_ms(),
            agent_calls=ctx.agent_calls,
            errors=ctx.errors,
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _slugify(text: str) -> str:
    """Convert *text* to a filesystem-safe slug (max 40 chars)."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[-\s]+", "_", slug)
    return slug[:40]
