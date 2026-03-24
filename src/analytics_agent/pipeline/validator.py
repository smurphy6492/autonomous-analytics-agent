"""Pipeline data-quality validators — pure functions, no side effects.

These run automatically after each SQL query and each chart render so that
data problems surface immediately rather than silently producing a bad report.

Three failure modes that prompted this module:

  A. SQL returns 0 rows (e.g. ``CURRENT_DATE`` filter on a historical dataset).
  B. Chart HTML still contains ``bdata`` (Plotly Python ≥ 6.0 binary encoding
     not decoded by older Plotly.js builds — axes render as row indices).
  C. A numeric axis contains sequential integers [0, 1, 2, ...] rather than
     actual values, indicating wrong column mapping or undecoded ``bdata``.
"""

from __future__ import annotations

import re
import statistics
from typing import Any

from analytics_agent.models.chart_spec import ChartSpec
from analytics_agent.models.query_plan import QueryResult

# Threshold: correlation ≥ this value with range(n) → looks like row indices.
_INDEX_CORR_THRESHOLD = 0.99
# Minimum rows before the correlation check is meaningful.
_MIN_ROWS_FOR_CORR = 4

# Detect any surviving bdata key in rendered HTML.
_BDATA_KEY_RE = re.compile(r'"bdata"\s*:')

# Match "x":[...] or "y":[...] where the array contains only numbers
# (no quotes inside) — i.e., decoded numeric axes. Date/string axes are
# excluded because they contain quoted values.
_NUMERIC_AXIS_RE = re.compile(r'"([xy])"\s*:\s*(\[[\d,.\-eE+\s]+\])')


def validate_query_result(result: QueryResult) -> list[str]:
    """Inspect a successful QueryResult for data-quality problems.

    Returns a list of human-readable warning strings.  Empty list = no issues.
    Does not raise.  Callers should log each warning and optionally surface
    them in the report.

    Checks performed:
    - Zero rows (Bug A: likely a date filter on historical data).
    - Zero-variance columns (all rows identical — wrong column mapping).
    - Sequential-index columns (values correlate ≥ 0.99 with range(n) — Bug C).
    """
    warnings: list[str] = []

    if result.row_count == 0:
        warnings.append(
            f"[{result.query_id}] Query returned 0 rows — "
            "possible date filter on historical data (CURRENT_DATE on past dataset?)"
        )
        return warnings  # Nothing more to inspect without rows.

    if not result.data:
        return warnings

    numeric_cols = _collect_numeric_columns(result.data)

    for col, values in numeric_cols.items():
        n = len(values)
        if n < 2:
            continue

        # Check: zero variance.
        if len(set(values)) == 1:
            warnings.append(
                f"[{result.query_id}] Column '{col}' has zero variance "
                f"(all {n} rows = {values[0]}) — possible wrong column mapping"
            )
            continue

        # Check: sequential row indices.
        if n >= _MIN_ROWS_FOR_CORR:
            corr = _index_correlation(values)
            if corr >= _INDEX_CORR_THRESHOLD:
                warnings.append(
                    f"[{result.query_id}] Column '{col}' looks like sequential "
                    f"row indices (corr={corr:.3f} with range({n})) — "
                    "possible undecoded bdata or wrong column mapping"
                )

    return warnings


def validate_chart_html(html: str, spec: ChartSpec) -> list[str]:
    """Inspect rendered chart HTML for known rendering defects.

    Returns a list of human-readable warning strings.  Empty list = no issues.
    Does not raise.  This is a regression guard — particularly for the Plotly
    bdata encoding bug (Bug B) and sequential-index axis bug (Bug C).

    Checks performed:
    - Residual ``bdata`` key in HTML (should have been decoded by renderer).
    - Numeric axis arrays that look like sequential row indices.
    - Numeric axis arrays where all values are identical.
    """
    if not html:
        return []

    warnings: list[str] = []

    # Check: bdata not fully decoded (Bug B regression guard).
    if _BDATA_KEY_RE.search(html):
        warnings.append(
            f"[{spec.chart_id}] HTML still contains 'bdata' — "
            "Plotly bdata was not decoded; axes may render as row indices. "
            "Check analytics_agent.viz.renderer._decode_bdata()."
        )

    # Check numeric axis arrays for sequential indices and zero variance.
    for match in _NUMERIC_AXIS_RE.finditer(html):
        axis = match.group(1)
        raw = match.group(2)
        values = _parse_float_array(raw)
        if not values or len(values) < 2:
            continue

        if len(set(values)) == 1:
            warnings.append(
                f"[{spec.chart_id}] Axis '{axis}' contains {len(values)} "
                f"identical values ({values[0]}) — possible wrong column mapping"
            )
            continue

        if len(values) >= _MIN_ROWS_FOR_CORR:
            corr = _index_correlation(values)
            if corr >= _INDEX_CORR_THRESHOLD:
                warnings.append(
                    f"[{spec.chart_id}] Axis '{axis}' looks like sequential row "
                    f"indices (first 5: {values[:5]}, corr={corr:.3f}) — "
                    "possible undecoded bdata or wrong column mapping"
                )

    return warnings


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _collect_numeric_columns(
    data: list[dict[str, Any]],
) -> dict[str, list[float]]:
    """Return a mapping of column → float values for all numeric columns."""
    cols: dict[str, list[float]] = {}
    for row in data:
        for col, val in row.items():
            # Exclude booleans (bool is a subclass of int in Python).
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                cols.setdefault(col, []).append(float(val))
    return cols


def _index_correlation(values: list[float]) -> float:
    """Return Pearson correlation of *values* with range(len(values)).

    Returns 0.0 on any statistical error (e.g. zero variance in either series).
    """
    n = len(values)
    expected = list(range(n))
    try:
        return statistics.correlation(values, expected)
    except statistics.StatisticsError:
        return 0.0


def _parse_float_array(raw: str) -> list[float]:
    """Parse a JSON numeric array string into a Python list of floats.

    Handles scientific notation and negative numbers.  Returns empty list on
    any parse failure.
    """
    try:
        return [float(v) for v in re.findall(r"-?[\d]+(?:\.\d+)?(?:[eE][+\-]?\d+)?", raw)]
    except ValueError:
        return []
