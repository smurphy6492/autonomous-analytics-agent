"""Unit tests for DataProfile and related models."""

import pytest
from pydantic import ValidationError

from analytics_agent.models.profile import ColumnProfile, DataProfile, TableProfile


def make_column(**kwargs: object) -> ColumnProfile:
    defaults: dict[str, object] = {
        "name": "col",
        "dtype": "object",
        "null_count": 0,
        "null_pct": 0.0,
        "unique_count": 5,
        "cardinality": "low",
    }
    defaults.update(kwargs)
    return ColumnProfile.model_validate(defaults)


def make_table(name: str = "orders", rows: int = 100) -> TableProfile:
    return TableProfile(
        name=name,
        row_count=rows,
        columns=[
            make_column(name="id", dtype="int64", is_numeric=True),
            make_column(name="category", dtype="object"),
        ],
    )


class TestColumnProfile:
    def test_valid(self) -> None:
        col = make_column(name="price", dtype="float64", is_numeric=True)
        assert col.name == "price"
        assert col.is_numeric is True

    def test_null_pct_bounds(self) -> None:
        with pytest.raises(ValidationError):
            make_column(null_pct=1.5)
        with pytest.raises(ValidationError):
            make_column(null_pct=-0.1)

    def test_invalid_cardinality(self) -> None:
        with pytest.raises(ValidationError):
            make_column(cardinality="extreme")

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnProfile(
                name="x",
                dtype="int",
                null_count=0,
                null_pct=0.0,
                unique_count=1,
                cardinality="low",
                unknown="oops",  # type: ignore[call-arg]
            )


class TestDataProfile:
    def test_valid(self) -> None:
        profile = DataProfile(
            tables=[make_table("orders"), make_table("products", 50)],
            suggested_grain="order_id",
        )
        assert len(profile.tables) == 2
        assert profile.relationships == []
        assert profile.data_quality_issues == []

    def test_get_table_found(self) -> None:
        profile = DataProfile(
            tables=[make_table("orders")],
            suggested_grain="order_id",
        )
        t = profile.get_table("orders")
        assert t is not None
        assert t.name == "orders"

    def test_get_table_not_found(self) -> None:
        profile = DataProfile(tables=[make_table()], suggested_grain="id")
        assert profile.get_table("missing") is None

    def test_table_names(self) -> None:
        profile = DataProfile(
            tables=[make_table("a"), make_table("b")],
            suggested_grain="id",
        )
        assert profile.table_names() == ["a", "b"]

    def test_roundtrip(self) -> None:
        profile = DataProfile(
            tables=[make_table()],
            suggested_grain="id",
            data_quality_issues=["high nulls in col"],
        )
        restored = DataProfile.model_validate(profile.model_dump())
        assert restored == profile
