"""PipelineContext — shared state threaded through every pipeline step."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from analytics_agent.models.profile import DataProfile
from analytics_agent.models.query_plan import QueryPlan, QueryResult
from analytics_agent.models.report import AgentCallLog, AnalysisSynthesis, RenderedChart


@dataclass
class PipelineContext:
    """Accumulates state across all pipeline steps.

    Created at the start of a run by
    :class:`~analytics_agent.pipeline.runner.PipelineRunner`
    and passed through each step.  At the end it is passed to
    :meth:`~analytics_agent.pipeline.runner.PipelineRunner._assemble_report` to
    :class:`~analytics_agent.models.report.AnalysisReport`.

    Args:
        data_paths: Input CSV paths loaded into DuckDB.
        table_names: Table name for each CSV (parallel to ``data_paths``).
        business_question: The question the pipeline is answering.
        output_path: Destination for the generated HTML report.
        title: Report title.
    """

    # --- Inputs (set at construction time) ---
    data_paths: list[Path]
    table_names: list[str]
    business_question: str
    output_path: Path
    title: str = "Analytics Report"

    # --- Accumulated state (filled in by pipeline steps) ---
    profile: DataProfile | None = None
    query_plan: QueryPlan | None = None
    query_results: dict[str, QueryResult] = field(default_factory=dict)
    synthesis: AnalysisSynthesis | None = None
    rendered_charts: list[RenderedChart] = field(default_factory=list)

    # --- Execution metadata ---
    start_time: datetime | None = None
    end_time: datetime | None = None
    errors: list[str] = field(default_factory=list)
    agent_calls: list[AgentCallLog] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def record_error(self, message: str) -> None:
        """Append *message* to the errors list."""
        self.errors.append(message)

    def elapsed_ms(self) -> int:
        """Return elapsed wall-clock time in milliseconds (0 if not started/ended)."""
        if self.start_time is None or self.end_time is None:
            return 0
        delta = self.end_time - self.start_time
        return int(delta.total_seconds() * 1000)
