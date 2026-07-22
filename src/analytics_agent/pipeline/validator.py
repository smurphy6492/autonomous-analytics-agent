"""Pipeline data-quality validators — pure functions, no side effects.

These run automatically after each SQL query and each chart render so that
data problems surface immediately rather than silently producing a bad report.

Five failure modes that prompted this module:

  A. SQL returns 0 rows (e.g. ``CURRENT_DATE`` filter on a historical dataset).
  B. Chart HTML still contains ``bdata`` (Plotly Python ≥ 6.0 binary encoding
     not decoded by older Plotly.js builds — axes render as row indices).
  C. A numeric axis contains sequential integers [0, 1, 2, ...] rather than
     actual values, indicating wrong column mapping or undecoded ``bdata``.
  D. A multi-table join fans out and inflates aggregates (e.g. joining
     order_items to payments double-counts revenue). The SQL runs cleanly and
     returns ``success=True`` — the retry loop never sees it, because the retry
     loop only catches queries that *error*, not queries that are silently
     wrong. See :func:`validate_join_fanout`.
  E. The executive summary quotes a headline figure that appears in no query
     result — a fabricated or mis-transcribed number. See
     :func:`validate_summary_numbers`.

Scope and honest limits: these are deterministic *heuristics*, not a
correctness proof. They catch specific, recurring failure signatures. They do
NOT verify that a query's business logic is right — a join that fans out by a
small factor, or a wrong-but-plausible aggregation, can still pass. The
authoritative correctness check is the golden-answer eval in ``eval/`` (see
``eval/golden_cases.py``), which compares agent output against independently
verified reference numbers.
"""

from __future__ import annotations

import re
import statistics
from typing import Any

from analytics_agent.models.chart_spec import ChartSpec
from analytics_agent.models.profile import DataProfile
from analytics_agent.models.query_plan import PlannedQuery, QueryResult

# Threshold: correlation ≥ this value with range(n) → looks like row indices.
_INDEX_CORR_THRESHOLD = 0.99
# Minimum rows before the correlation check is meaningful.
_MIN_ROWS_FOR_CORR = 4
# A grouped/breakdown result should never have more rows than its largest
# source table. If it does, the join almost certainly fanned out.
_FANOUT_ROW_MULTIPLIER = 1.0
# Relative tolerance when matching a summary figure to a query-result value.
_FIGURE_MATCH_RTOL = 0.02

# Detect any surviving bdata key in rendered HTML.
_BDATA_KEY_RE = re.compile(r'"bdata"\s*:')


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
    """Inspect rendered chart HTML for the one defect only the HTML can show.

    Axis-mapping defects (sequential indices, constant axes) are now checked on
    the figure object before serialization — see
    :func:`analytics_agent.viz.renderer.figure_axis_warnings`, which reads the
    real trace arrays instead of regex-scraping the serialized string. This
    function keeps only the ``bdata`` residual check, which is inherently about
    the serialized output: Plotly ≥ 6.0 encodes numeric arrays as base64
    ``bdata`` objects that older Plotly.js can't read, and the renderer decodes
    them — this is the regression guard proving it did.

    Returns a list of human-readable warning strings. Empty list = no issues.
    Does not raise.
    """
    if not html:
        return []

    if _BDATA_KEY_RE.search(html):
        return [
            f"[{spec.chart_id}] HTML still contains 'bdata' — "
            "Plotly bdata was not decoded; axes may render as row indices. "
            "Check analytics_agent.viz.renderer._decode_bdata()."
        ]

    return []


def validate_join_fanout(
    result: QueryResult,
    planned_query: PlannedQuery,
    profile: DataProfile,
) -> list[str]:
    """Flag results whose row count betrays a fanned-out join (Bug D).

    The retry loop in :class:`~analytics_agent.agents.sql_analyst.SQLAnalystAgent`
    only reacts to SQL that *raises* a DuckDB error. A query that joins two
    tables at the wrong grain — the classic ``order_items`` × ``payments``
    double-count — runs cleanly, returns ``success=True``, and inflates every
    downstream SUM. Nothing upstream catches it.

    This heuristic catches the most common signature: a grouped or aggregated
    result that returns **more rows than its largest source table**. A
    ``GROUP BY category`` cannot legitimately produce more rows than the base
    table has; when it does, the join multiplied rows before aggregating.

    Honest limits: this does not catch a join that fans out by a factor small
    enough to keep the row count under the source-table size, nor a
    wrong-but-plausible aggregation. In particular the canonical
    ``order_items × payments`` double-count keeps the same row grain after a
    ``GROUP BY category`` — it inflates the SUM without inflating the row count,
    so it slips past this check and is caught only by the golden eval. This is a
    smoke alarm for the common signature, not a proof of correctness.

    Args:
        result: The successful query result to inspect.
        planned_query: The plan that produced the query — supplies the set of
            source tables the query was allowed to touch.
        profile: The data profile, used to look up source-table row counts.

    Returns:
        A list of human-readable warning strings. Empty list = no issue.
        Never raises.
    """
    if not result.success or result.row_count == 0:
        return []

    # A single-table query cannot fan out via a join.
    if len(planned_query.required_tables) < 2:
        return []

    source_row_counts = [
        tbl.row_count
        for name in planned_query.required_tables
        if (tbl := profile.get_table(name)) is not None
    ]
    if not source_row_counts:
        return []

    largest_source = max(source_row_counts)
    if result.row_count > largest_source * _FANOUT_ROW_MULTIPLIER:
        return [
            f"[{result.query_id}] Result has {result.row_count:,} rows but the "
            f"largest source table has only {largest_source:,} — a multi-table "
            f"join ({', '.join(planned_query.required_tables)}) likely fanned "
            "out and may be inflating aggregates. Verify join grain."
        ]

    return []


def validate_summary_numbers(
    executive_summary: str,
    query_results: dict[str, QueryResult],
) -> list[str]:
    """Flag headline figures in the summary that no query result supports (Bug E).

    The synthesis step asks an LLM to write an executive summary "citing actual
    figures". Nothing verifies it actually did — a transposed digit or an
    invented total reads just as confidently as a real one. This check extracts
    every *headline* figure from the summary (currency amounts, percentages, and
    large numbers) and confirms each one matches a value that appears in some
    successful query result, within a small tolerance.

    Deliberately scoped to headline figures. Small bare integers (``top 5``,
    ``past 12 months``, ``3 categories``) are structural language, not data, and
    are skipped to keep the check low-noise. Currency (``$1.23M``), percentages
    (``11.69%``), and numbers ≥ 1,000 (with optional commas or K/M suffix) are
    the figures a stakeholder would act on — those must trace to the data.

    Args:
        executive_summary: The synthesized summary text.
        query_results: All query results, keyed by ``query_id``.

    Returns:
        A list of warnings, one per unsupported figure. Empty list = all
        headline figures trace to a query result. Never raises.
    """
    if not executive_summary:
        return []

    result_values = _collect_result_values(query_results)
    if not result_values:
        return []

    warnings: list[str] = []
    for raw_token, figure in _extract_summary_figures(executive_summary):
        if not _figure_supported(figure, result_values):
            warnings.append(
                f"Executive summary cites '{raw_token}' but no query result "
                "contains a matching value — possible fabricated or "
                "mis-transcribed figure. Verify against the data."
            )
    return warnings


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


# Headline figures: $-amounts, percentages, or >=1000 numbers with optional
# thousands separators and an optional K/M/B suffix.
_FIGURE_RE = re.compile(
    r"""
    (?P<currency>\$)?
    (?P<num>\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)
    (?P<suffix>[KkMmBb](?![A-Za-z]))?
    (?P<percent>\s*%|\s*percent(?:age)?\b)?
    """,
    re.VERBOSE,
)

_SUFFIX_MULTIPLIERS = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


def _extract_summary_figures(text: str) -> list[tuple[str, float]]:
    """Extract headline figures from summary text as (raw_token, value) pairs.

    Only returns figures worth verifying: currency amounts, percentages, and
    plain numbers ≥ 1,000. Small bare integers are skipped as structural.
    """
    figures: list[tuple[str, float]] = []
    for m in _FIGURE_RE.finditer(text):
        num_str = m.group("num").replace(",", "")
        try:
            value = float(num_str)
        except ValueError:
            continue

        suffix = (m.group("suffix") or "").lower()
        if suffix:
            value *= _SUFFIX_MULTIPLIERS[suffix]

        is_currency = m.group("currency") is not None
        is_percent = m.group("percent") is not None
        had_separator = "," in m.group("num")

        # Skip small bare integers (structural language, not data).
        if not (is_currency or is_percent or had_separator or value >= 1000):
            continue

        raw_token = m.group(0).strip()
        figures.append((raw_token, value))
    return figures


def _collect_result_values(query_results: dict[str, QueryResult]) -> list[float]:
    """Collect every numeric value from all successful query results."""
    values: list[float] = []
    for result in query_results.values():
        if not result.success or not result.data:
            continue
        for row in result.data:
            for val in row.values():
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    values.append(float(val))
    return values


def _figure_supported(figure: float, result_values: list[float]) -> bool:
    """Return True if *figure* matches any result value within tolerance.

    Matches against each result value directly and against derived forms the
    summary commonly uses: the raw value, its absolute value, and rounding to a
    currency-friendly magnitude (a summary's "$1.23M" must match a raw
    1,233,131.72). Percentages already share the raw scale (11.69 vs 11.69).
    """
    if figure == 0:
        return any(abs(v) < 1e-9 for v in result_values)

    for v in result_values:
        for candidate in (v, abs(v)):
            if candidate == 0:
                continue
            if abs(figure - candidate) <= _FIGURE_MATCH_RTOL * abs(candidate):
                return True
    return False


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
