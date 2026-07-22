# Review Backlog — Autonomous Analytics Agent

Findings from the hiring-manager review (`/project-review`, 2026-07-21) that were
not fixed in the first pass. Critical findings were fixed and verified; the items
below are parked for future iteration. Each keeps its severity, the fix, and the
interview question it exposes so a future session can pick it up cold.

Review score: **64/100** — "advance with reservations."

---

## Fixed and verified (for reference)

- **C1** — No correctness eval. Added `eval/` golden-answer harness (reference SQL
  + hand-verified numbers + deterministic scorer) and a `validate_summary_numbers`
  check that every headline figure in the summary traces to a query result.
- **C2** — Retry loop only catches SQL that errors, not silently-wrong SQL. Added
  `validate_join_fanout` (flags grouped results larger than any source table) and
  reframed the retry loop's limits honestly in code and README.
- **C4** — CI auto-fixed and never ran e2e. Made `make check` non-mutating
  (`ruff check` + `ruff format --check`) and added a stubbed-LLM end-to-end
  pipeline test that runs in CI with no API key.
- **C3 (corrected)** — The reviewer flagged the website as misstating demo
  provenance ("Real Kaggle" over synthetic data) with unverifiable numbers. **This
  was wrong.** The Olist files are the real Kaggle dataset, finance is real Yahoo
  Finance, and only marketing/saas are synthetic — which the site already labels
  correctly. `scripts/verify_demo_numbers.py` reproduces every published figure
  ($1.23M, $5.27M, 9,465 items) exactly from the raw data. No website copy was
  changed because it was accurate. The real gap — no one-command way to reproduce
  the figures — is now closed by the verify script.

---

## Medium — probe from a sharp interviewer, worth a future pass

### M1. Data Profiler puts an LLM in the path of already-computed statistics
`data_profiler.py` computes exact row/null/unique/min/max in DuckDB, then hands
them to Claude with "do NOT alter any numeric values" and trusts it to copy them
back. Zero benefit, silent-corruption risk on ground-truth numbers.
- **Fix:** Build the numeric fields of `DataProfile` programmatically; use the LLM
  only for the genuinely semantic annotations (cardinality bucket, is_date,
  relationships), or drop it there entirely.
- **Interview question:** "Why is an LLM in the path of your null counts and row
  counts at all?"

### M2. "Past 12 months" is silently redefined as 12 months before the data's max date
The generated SQL correctly derives the window from `MAX(timestamp)` (2018 for
Olist), but a stakeholder reading "past 12 months" on a 2026-dated report is
misled, and nothing in the report discloses the actual window.
- **Fix:** Surface the real analysis window in the report ("12 months ending
  2018-08, the latest data available") and state it as an assumption.
- **Interview question:** "Your report is dated 2026 and says 'past 12 months' but
  the data ends in 2018 — what window did you actually analyze, and does the reader
  know?"

### M3. Coverage and metric-sanity checks are LLM-grading-LLM, marketed as "validation gates"
They can only catch order-of-magnitude absurdities and missing facets, not a
wrong-but-plausible number — but the website counts them among "8 Validation
Gates."
- **Fix:** On the website, reframe them honestly as heuristic guards; reserve
  "validation" for checks with a deterministic ground truth (the new fan-out guard,
  summary-number check, and golden eval). Consider revising the "8 Validation
  Gates" metric copy. (Website copy change — hold for a portfolio pass.)
- **Interview question:** "Which of your gates would catch a query that returns a
  confident, wrong revenue figure?"

### M4. No limitations section anywhere in the repo or case study
The most important thing to state about an autonomous LLM analytics tool — what it
cannot be trusted to do — is absent. The README now documents validator limits,
but there is no standalone limitations section, and the website's "Self-Review
Loop" reads as capability marketing.
- **Fix:** Add a plain limitations section (no correctness guarantee on unseen
  questions, silent-wrong-SQL risk beyond the fan-out signature, single-shot with
  no human review, cost/latency per report, unvalidated on multi-table causal
  questions).
- **Interview question:** "Where would you *not* deploy this, and why?"

---

## Low — polish

### L1. `_extract_json` last-resort slice breaks on JSON arrays
`base.py::_extract_json` falls back to first `{` / last `}`, which mishandles a
top-level JSON array and can grab the wrong span.
- **Fix:** Parse defensively or require object roots.

### L2. Dead code in `orchestrator.py`
`_format_planned_queries` and `_format_result_summary` are unused.
- **Fix:** Remove them.

### L3. Model name hardcoded in multiple places
`claude-sonnet-4-6` appears as `_DEFAULT_MODEL` in `base.py` and again in config
and README.
- **Fix:** Single source of truth (config), referenced everywhere else.

### L4. Chart validation regex-parses Plotly HTML
`_NUMERIC_AXIS_RE` scrapes rendered HTML for correctness signals; brittle across
Plotly versions.
- **Fix:** Validate the figure object before serialization instead of the HTML
  string.
