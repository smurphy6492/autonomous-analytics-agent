"""Pydantic data models for agent data contracts."""

from analytics_agent.models.chart_spec import ChartSpec, ChartType
from analytics_agent.models.profile import (
    ColumnProfile,
    DataProfile,
    ProfileRequest,
    Relationship,
    TableProfile,
)
from analytics_agent.models.query_plan import (
    PlannedQuery,
    QueryPlan,
    QueryPlanRequest,
    QueryResult,
    SQLRequest,
)
from analytics_agent.models.report import (
    AgentCallLog,
    AnalysisReport,
    AnalysisSynthesis,
    DataTableSpec,
    KeyMetric,
    RenderedChart,
    SynthesisRequest,
)

__all__ = [
    # chart_spec
    "ChartSpec",
    "ChartType",
    # profile
    "ColumnProfile",
    "DataProfile",
    "ProfileRequest",
    "Relationship",
    "TableProfile",
    # query_plan
    "PlannedQuery",
    "QueryPlan",
    "QueryPlanRequest",
    "QueryResult",
    "SQLRequest",
    # report
    "AgentCallLog",
    "AnalysisReport",
    "AnalysisSynthesis",
    "DataTableSpec",
    "KeyMetric",
    "RenderedChart",
    "SynthesisRequest",
]
