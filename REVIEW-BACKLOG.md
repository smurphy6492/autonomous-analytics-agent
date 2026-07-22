# Review Backlog — Autonomous Analytics Agent

Findings from the hiring-manager review (`/project-review`, 2026-07-21). The
critical findings were fixed and verified in the first pass; the medium and low
findings were cleared in a second pass. One website-copy item remains.

Review score at time of review: **64/100** — "advance with reservations."

---

## Critical — fixed and verified (first pass)

- **C1** — No correctness eval. Added the `eval/` reference-answer harness
  (reference SQL + verified numbers + deterministic scorer, 8/8 on a live run)
  and a `validate_summary_numbers` check that every headline figure in the
  summary traces to a query result.
- **C2** — Retry loop only catches SQL that errors, not silently-wrong SQL. Added
  `validate_join_fanout` (flags grouped results larger than any source table) and
  reframed the retry loop's limits honestly in code and README.
- **C4** — CI auto-fixed and never ran e2e. Made `make check` non-mutating
  (`ruff check` + `ruff format --check`) and added a stubbed-LLM end-to-end
  pipeline test that runs in CI with no API key.
- **C3 (corrected)** — The reviewer flagged the website as misstating demo
  provenance with unverifiable numbers. **This was wrong.** The Olist files are
  the real Kaggle dataset and every published figure reproduces exactly via
  `scripts/verify_demo_numbers.py`. No copy changed because it was accurate.

---

## Medium & Low — cleared (second pass)

- **M1** — Removed the LLM from the profiler entirely. `data_profiler.py` now
  builds the whole `DataProfile` deterministically (stats from DuckDB;
  cardinality, date/numeric flags, relationships, grain from rules). No model in
  the path of ground-truth numbers.
- **M2** — The report now discloses the data's actual date window in the
  methodology section, so "past 12 months" on an older dataset is not misleading.
- **M4** — Added a plain limitations section to the README (no correctness
  guarantee on unseen questions, silent-wrong-SQL beyond the fan-out signature,
  single-shot with no human review, cost/latency, out-of-scope analysis).
- **L1** — `_extract_json` now recovers a top-level JSON array instead of
  mis-slicing from the first inner brace.
- **L2** — Removed dead code (`_format_planned_queries`, `_format_result_summary`).
- **L3** — Single `DEFAULT_MODEL` constant in config, imported by base.
- **L4** — Chart axis-mapping checks now inspect the Plotly figure object
  (`figure_axis_warnings`) instead of regex-scraping the serialized HTML; the
  bdata residual check remains as the serialization backstop.

---

## Remaining

### M3. Website "Self-Review Loop" narrative (copy polish)
The worst of this was already fixed on the site: the metric card is now an honest
"8/8 Eval Score" (not "8 Validation Gates"), and a paragraph distinguishes
deterministic correctness checks from the LLM-based guards. What remains is a
lighter polish of the surrounding "Self-Review Loop" prose so the coverage and
metric-sanity bullets read as heuristic guards rather than guarantees.
- **Where:** `personal-website/src/data/projects.ts`, analytics-agent entry.
- **Why deferred:** low-severity copy refinement, best batched into a future
  portfolio-wide writing pass rather than a one-off edit.
- **Interview question it touches:** "Which of your checks would catch a
  confident, wrong number?" — the site should make the answer (the eval, not the
  LLM guards) unambiguous.
