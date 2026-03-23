"""Report models — output of the Orchestrator synthesis call and report builder."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from analytics_agent.models.chart_spec import ChartSpec
from analytics_agent.models.query_plan import QueryPlan, QueryResult


class KeyMetric(BaseModel):
    """A single highlighted metric for the executive summary."""

    label: str  # e.g. "Total Revenue"
    value: str  # e.g. "$1.2M"  (pre-formatted string)
    context: str | None = None  # e.g. "+15% vs prior period"

    model_config = {"extra": "forbid"}


class DataTableSpec(BaseModel):
    """Specification for a data table to include in the report."""

    table_id: str
    title: str
    data_source: str  # query_id that provides the rows
    columns: list[str] | None = None  # None means include all columns
    max_rows: int = Field(default=20, ge=1)

    model_config = {"extra": "forbid"}


class AnalysisSynthesis(BaseModel):
    """Synthesis output from the Orchestrator after SQL results are available.

    This is the second call to the Orchestrator — it transforms raw query
    results into human-readable insights and rendering instructions.
    """

    executive_summary: str  # 3-5 sentences directly answering the business question
    key_metrics: list[KeyMetric] = Field(default_factory=list, max_length=8)
    chart_specs: list[ChartSpec] = Field(default_factory=list)
    data_tables: list[DataTableSpec] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


class SynthesisRequest(BaseModel):
    """Input to the Orchestrator's synthesis call."""

    business_question: str
    query_plan: QueryPlan
    query_results: dict[str, QueryResult]

    model_config = {"extra": "forbid"}


class RenderedChart(BaseModel):
    """A chart that has been rendered to an HTML div by the Viz Agent."""

    chart_id: str
    title: str
    html: str  # Plotly figure rendered as an HTML <div>
    success: bool
    error: str | None = None

    model_config = {"extra": "forbid"}


class AgentCallLog(BaseModel):
    """Metadata about a single agent API call for pipeline observability."""

    agent_name: str
    call_type: str  # e.g. "plan_queries", "synthesize", "generate_sql"
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    duration_ms: int = 0
    success: bool = True
    error: str | None = None

    model_config = {"extra": "forbid"}


class AnalysisReport(BaseModel):
    """The complete assembled report, ready for HTML rendering.

    This is the final output of the pipeline runner and is passed to the
    ReportBuilder to produce the HTML file.
    """

    title: str
    business_question: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    executive_summary: str
    key_metrics: list[KeyMetric]
    rendered_charts: list[RenderedChart]
    data_tables: list[DataTableSpec]
    query_plan: QueryPlan
    query_results: dict[str, QueryResult]
    data_sources: list[str]  # Table/file names used
    analysis_approach: str
    execution_time_ms: int = 0
    agent_calls: list[AgentCallLog] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    model_config = {"extra": "forbid"}
