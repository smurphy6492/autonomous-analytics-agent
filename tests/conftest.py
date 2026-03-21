"""Shared pytest fixtures for the analytics agent test suite."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from dotenv import load_dotenv

# Load .env so ANTHROPIC_API_KEY is available for integration tests.
load_dotenv()


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a small sample CSV for testing."""
    data = (
        "order_id,order_date,revenue,category\n"
        "1,2023-01-01,100.00,electronics\n"
        "2,2023-01-02,200.00,clothing\n"
        "3,2023-01-03,150.00,electronics\n"
    )
    path = tmp_path / "sample.csv"
    path.write_text(data)
    return path


@pytest.fixture
def mock_anthropic_client() -> MagicMock:
    """Mock Anthropic client for unit tests (no API calls made)."""
    client = MagicMock()
    # Configure default response shape used by BaseAgent
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text='{"result": "ok"}')]
    client.messages.create.return_value = mock_message
    return client


@pytest.fixture
def sample_data_profile() -> dict:  # type: ignore[type-arg]
    """Pre-built profile dict for testing downstream agents.

    Replace with a real DataProfile fixture once models are defined in Phase 2.
    """
    return {
        "tables": [
            {
                "name": "orders",
                "row_count": 3,
                "columns": [
                    {
                        "name": "order_id",
                        "dtype": "int64",
                        "null_count": 0,
                        "null_pct": 0.0,
                        "unique_count": 3,
                        "cardinality": "low",
                        "sample_values": ["1", "2", "3"],
                        "is_date": False,
                        "is_numeric": True,
                        "min_value": "1",
                        "max_value": "3",
                    },
                    {
                        "name": "order_date",
                        "dtype": "object",
                        "null_count": 0,
                        "null_pct": 0.0,
                        "unique_count": 3,
                        "cardinality": "low",
                        "sample_values": ["2023-01-01", "2023-01-02", "2023-01-03"],
                        "is_date": True,
                        "is_numeric": False,
                        "min_value": "2023-01-01",
                        "max_value": "2023-01-03",
                    },
                    {
                        "name": "revenue",
                        "dtype": "float64",
                        "null_count": 0,
                        "null_pct": 0.0,
                        "unique_count": 3,
                        "cardinality": "low",
                        "sample_values": ["100.0", "200.0", "150.0"],
                        "is_date": False,
                        "is_numeric": True,
                        "min_value": "100.0",
                        "max_value": "200.0",
                    },
                    {
                        "name": "category",
                        "dtype": "object",
                        "null_count": 0,
                        "null_pct": 0.0,
                        "unique_count": 2,
                        "cardinality": "low",
                        "sample_values": ["electronics", "clothing"],
                        "is_date": False,
                        "is_numeric": False,
                        "min_value": None,
                        "max_value": None,
                    },
                ],
            }
        ],
        "relationships": [],
        "suggested_grain": "order_id",
        "data_quality_issues": [],
    }


@pytest.fixture
def sample_query_results() -> dict:  # type: ignore[type-arg]
    """Pre-built query results dict for testing synthesis.

    Replace with real QueryResult fixtures once models are defined in Phase 2.
    """
    return {
        "revenue_by_category": {
            "query_id": "revenue_by_category",
            "sql": (
                "SELECT category, SUM(revenue) AS total FROM orders GROUP BY category"
            ),
            "success": True,
            "data": [
                {"category": "electronics", "total": 250.0},
                {"category": "clothing", "total": 200.0},
            ],
            "row_count": 2,
            "error": None,
            "attempts": 1,
        }
    }
