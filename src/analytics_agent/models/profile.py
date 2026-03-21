"""Data profile models — output of the Data Profiler Agent."""

from typing import Literal

from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    """Profile statistics for a single column."""

    name: str
    dtype: str  # int64, float64, object, datetime64, etc.
    null_count: int = Field(ge=0)
    null_pct: float = Field(ge=0.0, le=1.0)
    unique_count: int = Field(ge=0)
    cardinality: Literal["low", "medium", "high"]
    sample_values: list[str] = Field(default_factory=list, max_length=10)
    is_date: bool = False
    is_numeric: bool = False
    min_value: str | None = None
    max_value: str | None = None

    model_config = {"extra": "forbid"}


class TableProfile(BaseModel):
    """Profile for a single table / CSV file."""

    name: str
    row_count: int = Field(ge=0)
    columns: list[ColumnProfile]

    model_config = {"extra": "forbid"}


class Relationship(BaseModel):
    """Detected foreign-key relationship between two tables."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)

    model_config = {"extra": "forbid"}


class DataProfile(BaseModel):
    """Complete profile of all input datasets.

    This is the primary output of the Data Profiler Agent and is passed
    to every downstream agent to inform SQL generation and chart selection.
    """

    tables: list[TableProfile]
    relationships: list[Relationship] = Field(default_factory=list)
    suggested_grain: str  # e.g. "order_id" or "order_id + product_id"
    data_quality_issues: list[str] = Field(default_factory=list)

    model_config = {"extra": "forbid"}

    def get_table(self, name: str) -> TableProfile | None:
        """Return the TableProfile for the given table name, or None."""
        return next((t for t in self.tables if t.name == name), None)

    def table_names(self) -> list[str]:
        """Return all table names in this profile."""
        return [t.name for t in self.tables]


class ProfileRequest(BaseModel):
    """Input to the Data Profiler Agent."""

    data_paths: list[str]  # str paths; converted to Path in the agent
    table_names: list[str] | None = None  # Optional custom names

    model_config = {"extra": "forbid"}
