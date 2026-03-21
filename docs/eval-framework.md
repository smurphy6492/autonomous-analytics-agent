# Evaluation Framework — Autonomous Analytics Agent

Structured rubric for assessing agent output quality after a pipeline run. Use alongside `examples/sample_questions.md`.

Run this evaluation after the build is complete and again after any significant change to agent prompts or pipeline logic.

---

## How to Use

1. Run the agent against a question from `sample_questions.md`
2. Score the output across the three layers below
3. Note any failure modes observed
4. Use findings to update prompts, add guardrails, or log as known limitations

A passing run requires **all three layers** to pass. A strong run earns bonus marks on the strategic questions at the end.

---

## Layer 1: Correctness

*Does the agent return the right answer when one exists?*

| Check | Pass Criteria | Failure Mode |
|-------|---------------|--------------|
| SQL executes without error | Query runs against DuckDB with no syntax or runtime errors | Query fails silently or returns empty result without explanation |
| SQL logic is correct | Joins, filters, and aggregations match the stated purpose | Wrong grain (e.g., double-counting from a bad join), wrong date filter |
| Numbers are internally consistent | Totals match detail rows; percentages sum to ~100% | Summary table contradicts chart values |
| Answer addresses the question | The executive summary directly answers what was asked | Summary is generic and could apply to any dataset |

**Score:** ___/ 4 checks passing

---

## Layer 2: Reasoning Quality

*Does the explanation reflect sound analytical thinking?*

| Check | Pass Criteria | Failure Mode |
|-------|---------------|--------------|
| Approach is stated | Agent explains what it analyzed and why, not just what it found | Jumps to conclusions with no explanation of method |
| Findings are specific | Uses actual numbers, not vague language ("revenue increased significantly") | Hedged, non-committal summary with no quantification |
| Visualizations match the question | Chart type is appropriate (time series → line, comparison → bar, etc.) | Bar chart for time series data; pie chart for more than 5 categories |
| Insight goes beyond the obvious | Summary surfaces something non-trivial — a pattern, a concentration, an anomaly | Restates the question back as the answer ("Revenue was highest in months with the most orders") |

**Score:** ___/ 4 checks passing

---

## Layer 3: Epistemic Honesty

*Does the agent know what it doesn't know?*

| Check | Pass Criteria | Failure Mode |
|-------|---------------|--------------|
| Ambiguous questions trigger clarification or disclosure | Agent either asks for clarification OR explicitly states the interpretation it chose | Silently picks one interpretation with no acknowledgment |
| Unanswerable questions are refused cleanly | Agent explains why the question can't be answered with available data | Fabricates a number; confuses a proxy metric for the requested metric |
| Data quality issues are surfaced | If profiler flagged nulls, date gaps, or anomalies — these appear as caveats in the output | Agent ignores known data quality issues and presents clean-looking results |
| Assumptions are labeled | Any analytical assumption (e.g., "I'm treating delivered orders only as revenue") is stated explicitly | Assumptions are buried or absent |

**Score:** ___/ 4 checks passing

---

## Strategic Questions

Run these after a complete evaluation session to assess overall portfolio readiness.

1. **The jaw-drop test:** Is there one finding in this output that a business user would not have arrived at on their own with a spreadsheet? If not, the agent is automating effort but not adding intelligence.

2. **The wrong answer test:** If the agent returned a subtly incorrect answer, would you catch it from the output alone — or would you need to re-run the SQL yourself? A good agent makes its reasoning auditable.

3. **The handoff test:** Could you show this report to a VP without editing it? If the framing, tone, or structure would embarrass you, the report template needs work.

4. **The failure test:** Run one truly unanswerable question and one ambiguous question. Does the agent's response build or erode trust? Graceful failure is a feature.

5. **The domain transfer test:** Take a question written for Olist and substitute a different dataset. Does the agent's behavior degrade gracefully, or does it hallucinate schema details it doesn't have?

---

## Evaluation Log

Use this table to track runs over time.

| Date | Question | L1 Score | L2 Score | L3 Score | Notes |
|------|----------|----------|----------|----------|-------|
| | | /4 | /4 | /4 | |
| | | /4 | /4 | /4 | |
| | | /4 | /4 | /4 | |
