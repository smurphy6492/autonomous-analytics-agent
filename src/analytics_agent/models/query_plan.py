"""Query plan models — output of the Analytics Orchestrator (planning call)."""

from typing import Literal

from pydantic import BaseModel, Field

from analytics_agent.models.profile import DataProfile


class PlannedQuery(BaseModel):
    """A single analytical query as planned by the Orchestrator."""

    query_id: str  # e.g. "time_series", "category_summary"
    purpose: str  # What business question this query answers
    required_tables: list[str]
    required_columns: list[str]
    aggregation_grain: str  # e.g. "daily", "monthly", "by_category"
    expected_output_type: Literal["time_series", "summary_table", "breakdown", "other"]

    model_config = {"extra": "forbid"}


class QueryPlan(BaseModel):
    """Complete plan for analysing a dataset to answer a business question.

    Produced by the Orchestrator's planning call and consumed by the
    SQL Analyst Agent.
    """

    analysis_approach: str  # 1-2 sentence summary of the analytical approach
    queries: list[PlannedQuery] = Field(min_length=1, max_length=4)

    model_config = {"extra": "forbid"}


class QueryResult(BaseModel):
    """Result from executing a single planned query via the SQL Analyst Agent."""

    query_id: str
    sql: str  # The SQL that was actually executed
    success: bool
    data: list[dict[str, object]] | None = None  # Rows as list of dicts
    row_count: int = 0
    error: str | None = None
    attempts: int = Field(ge=1, default=1)

    model_config = {"extra": "forbid"}


class QueryPlanRequest(BaseModel):
    """Input to the Orchestrator's query-planning call."""

    business_question: str
    data_profile: DataProfile

    model_config = {"extra": "forbid"}


class SQLRequest(BaseModel):
    """Input to the SQL Analyst Agent for a single query attempt."""

    planned_query: PlannedQuery
    data_profile: DataProfile
    previous_error: str | None = None  # Set on retry attempts

    model_config = {"extra": "forbid"}
