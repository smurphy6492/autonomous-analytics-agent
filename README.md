# Autonomous Analytics Agent

A multi-agent AI system that takes a dataset and a business question, then autonomously produces a professional HTML report with an executive summary, data tables, and interactive charts.

**Stack:** Python · Claude API (Anthropic SDK) · DuckDB · Plotly · Jinja2 · Typer

---

## What It Does

1. **Profiles** your CSV data using DuckDB (schema, null rates, cardinality, relationships)
2. **Plans** the right SQL queries to answer your question (Claude Orchestrator)
3. **Executes** SQL against DuckDB with automatic error-correction retry (up to 3 attempts)
4. **Synthesises** results into an executive summary, key metrics, and chart specs (Claude Orchestrator)
5. **Renders** Plotly charts from the chart specs
6. **Writes** a self-contained HTML report you can open in any browser

---

## Quick Start

### 1. Install

```bash
cd projects/autonomous-analytics-agent
pip install -e ".[dev]"
```

### 2. Set your API key

```bash
cp .env.example .env
# Edit .env and add your Anthropic API key:
# ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Get the dataset (see [Dataset Setup](#dataset-setup) below)

### 4. Run

```bash
analytics-agent \
  --data-dir data/raw/olist \
  --question "What product categories are driving the most revenue, and how has this changed over the past 12 months?" \
  --output output/olist_revenue_analysis.html \
  --title "Olist Revenue Analysis"
```

The report will open-ready at `output/olist_revenue_analysis.html`.

---

## Dataset Setup

This project is designed for the **Brazilian E-Commerce Public Dataset (Olist)** from Kaggle.

### Option A: Kaggle CLI (recommended)

```bash
# Install Kaggle CLI if needed
pip install kaggle

# Set up Kaggle credentials (one-time)
# Download your kaggle.json from https://www.kaggle.com/settings → API
mkdir -p ~/.kaggle && cp kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json

# Download and unzip
kaggle datasets download olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/olist/
```

### Option B: Manual download

1. Go to https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Click **Download** (requires a free Kaggle account)
3. Unzip the archive
4. Copy the 8 CSV files into `data/raw/olist/`

### Expected files

```
data/raw/olist/
├── olist_orders_dataset.csv           # 99,441 rows
├── olist_order_items_dataset.csv      # 112,650 rows
├── olist_customers_dataset.csv        # 99,441 rows
├── olist_products_dataset.csv         # 32,951 rows
├── olist_sellers_dataset.csv          # 3,095 rows
├── olist_order_payments_dataset.csv   # 103,886 rows
├── olist_order_reviews_dataset.csv    # 100,000 rows
└── olist_geolocation_dataset.csv      # 1,000,000 rows
```

> **Tip:** The geolocation file is large (1M rows). If profiling is slow, remove it — it's not needed for revenue analysis.

### Alternative datasets

Any folder of CSV files works. Try:
- **Superstore Sales** (Tableau sample) — simpler, ~10k rows
- **NYC Taxi Data** (subset) — trip-level, good for time-series questions

---

## CLI Reference

```
Usage: analytics-agent [OPTIONS]

  Run the analytics pipeline against a directory of CSV files.

Options:
  -d, --data-dir  DIRECTORY  Directory of CSV files to analyse  [required]
  -q, --question  TEXT       Business question to answer        [required]
  -o, --output    FILE       Output HTML path (default: output/<title>.html)
  -t, --title     TEXT       Report title  [default: Analytics Report]
  -v, --verbose              Enable debug logging
  --help                     Show this message and exit
```

### Example questions

```bash
# Revenue analysis
analytics-agent -d data/raw/olist \
  -q "What product categories are driving the most revenue?" \
  -t "Revenue by Category"

# Time-series trend
analytics-agent -d data/raw/olist \
  -q "How has monthly order volume changed over the past 2 years?" \
  -t "Order Volume Trend"

# Customer geography
analytics-agent -d data/raw/olist \
  -q "Which Brazilian states have the highest average order value?" \
  -t "Revenue by State"

# Delivery performance
analytics-agent -d data/raw/olist \
  -q "What is the average delivery time by product category, and which categories have the most delays?" \
  -t "Delivery Performance"
```

---

## Architecture

```
User: data directory + business question
             │
             ▼
   ┌──────────────────┐
   │  Data Profiler   │  DuckDB stats → Claude → DataProfile
   │  Agent           │  (schema, cardinality, relationships)
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  Orchestrator    │  DataProfile + question → QueryPlan
   │  Agent (plan)    │  (2-4 SQL queries to run)
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  SQL Analyst     │  PlannedQuery → SQL → DuckDB execution
   │  Agent           │  (retry loop: 3 attempts, feeds errors back)
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  Orchestrator    │  QueryResults → AnalysisSynthesis
   │  Agent (synth)   │  (summary + key metrics + chart specs)
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  Viz Agent       │  ChartSpec + data → Plotly HTML div
   │                  │  (deterministic, no API call)
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  Report Builder  │  Jinja2 → self-contained HTML file
   └──────────────────┘
```

### Key design decisions

| Decision | Choice | Why |
|---|---|---|
| SQL engine | DuckDB (in-memory) | Fast, zero-setup, reads CSVs natively |
| Structured output | JSON mode + Pydantic | Type-safe contracts between agents |
| SQL retry | Feed error back to Claude | Self-correcting loop, no hard-coding of fixes |
| Chart rendering | Deterministic (no LLM) | Specs are already structured — no need for another API call |
| API caching | Hash-keyed `.cache/` dir | Avoids redundant calls during development |
| Report format | Self-contained HTML | No server needed, shareable as a single file |

---

## Project Structure

```
src/analytics_agent/
├── agents/
│   ├── base.py           # BaseAgent — Claude API + retry + caching
│   ├── data_profiler.py  # Data Profiler Agent
│   ├── orchestrator.py   # Analytics Orchestrator (planning + synthesis)
│   ├── sql_analyst.py    # SQL Analyst Agent (with retry loop)
│   └── viz_agent.py      # Viz Agent (deterministic Plotly rendering)
├── models/
│   ├── profile.py        # DataProfile, ColumnProfile, TableProfile
│   ├── query_plan.py     # QueryPlan, PlannedQuery, QueryResult
│   ├── chart_spec.py     # ChartSpec, ChartType
│   └── report.py         # AnalysisReport, AnalysisSynthesis, KeyMetric
├── pipeline/
│   ├── context.py        # PipelineContext (shared state)
│   └── runner.py         # PipelineRunner (wires all agents together)
├── db/
│   └── engine.py         # DuckDBEngine (CSV loading + query execution)
├── viz/
│   └── renderer.py       # render_chart() — Plotly Express dispatcher
├── report/
│   ├── builder.py        # ReportBuilder — Jinja2 HTML assembly
│   └── templates/
│       └── report.html.jinja2
├── cli.py                # Typer CLI entry point
└── config.py             # Settings from environment variables
```

---

## Development

### Setup

```bash
make install        # installs the package with dev tools and wires pre-commit
```

`make install` runs `pre-commit install`, so the ruff and mypy hooks run on staged files before every commit.
The same `make check` runs in CI on every push and pull request (`.github/workflows/ci.yml`), so a change that fails lint, type-check, or tests cannot merge.

### Run checks

```bash
make check          # lint + type-check + tests (non-mutating — this is what CI runs)
make lint           # ruff check + ruff format --check (reports, does not modify)
make fix            # ruff check --fix + ruff format (modifies files; local use only)
make type-check     # mypy
make test           # pytest with coverage
```

CI runs `make check`, which is strictly non-mutating: `ruff check .` and `ruff format --check .` report problems and fail rather than auto-fixing. Auto-fixing in CI would let a broken change pass by silently repairing it; `make fix` is the local-only mutating variant.

### Configuration (`.env`)

```bash
ANTHROPIC_API_KEY=sk-ant-...     # Required
ANTHROPIC_MODEL=claude-sonnet-4-6  # Optional, default shown
DATA_DIR=data/raw/olist          # Optional default data directory
OUTPUT_DIR=output                # Optional output directory
CACHE_DIR=.cache                 # Optional API response cache
```

### Disable API caching

Set `CACHE_DIR=` (empty) in `.env` to disable caching and always hit the live API.

### Run unit tests (no API key needed)

```bash
pytest -m "not integration"
```

### Run integration tests (requires API key)

```bash
pytest -m integration
```

---

## Correctness and reproducibility

An LLM-to-SQL-to-summary pipeline can produce output that looks right and is wrong. Two things guard against that, and both are runnable.

### Golden-answer eval

`eval/` holds business questions paired with a reference query and expected numbers on a small committed dataset (`eval/golden_dataset.csv`). The reference answers are authored independently of the agent and verified against the data — `tests/test_eval` runs each reference query and asserts it reproduces the expected numbers. The agent is graded on how many of those numbers it reproduces, so "did it work" becomes an accuracy number instead of a vibe.

```bash
python -m eval.run_eval          # runs the real agent over the golden cases (needs API key)
pytest tests/test_eval           # CI-safe: proves the reference numbers reproduce and the scorer is correct
```

The reference numbers are themselves reproducible — `tests/test_eval` runs each reference query against the committed dataset and asserts it produces the expected values, so the ground truth is verified, not trusted.

### Verifying the demo figures

The headline figures in the portfolio demo (Health & Beauty at $1.23M, a $5.27M top-5 total) are recomputed from the raw Kaggle Olist data with an explicit reference query:

```bash
python scripts/verify_demo_numbers.py    # PASS/FAIL against the published figures (needs the Olist data)
```

### What the runtime validators do and don't catch

Deterministic validators (`analytics_agent/pipeline/validator.py`) run after every query and flag specific failure signatures: zero rows, sequential-index axes, undecoded Plotly `bdata`, a join that fans out and inflates aggregates, and a summary figure that no query result supports. These are heuristics, not a correctness proof — a wrong-but-plausible aggregation can still pass a validator. The golden eval is the authoritative correctness check; the validators are the fast smoke alarms. The self-correcting SQL retry loop, likewise, only reacts to queries that *error* — it does not catch SQL that runs cleanly but computes the wrong thing, which is what the fan-out guard and the eval are for.

---

## Example Output

The pipeline produces a self-contained HTML report with:

- **Executive summary** — 3-5 sentences with specific numbers answering the question
- **Key metrics** — highlighted figures (e.g., "Total Revenue: R$13.6M")
- **Interactive charts** — Plotly line/bar/pie charts embedded as HTML
- **Data tables** — top-N rows from query results
- **Methodology section** — the SQL queries used (collapsible)

---

## Extending the Agent

### Add a new chart type

1. Add a variant to `ChartType` in `models/chart_spec.py`
2. Add a renderer in `viz/renderer.py` following the existing pattern
3. The Orchestrator will automatically discover it via the updated schema

### Use a different dataset

Point `--data-dir` at any folder of CSVs. The Data Profiler handles schema detection automatically — no configuration needed.

### Adjust the model

Set `ANTHROPIC_MODEL=claude-opus-4-6` in `.env` for higher-quality analysis (slower, higher cost) or `claude-haiku-4-5-20251001` for speed/cost optimisation.
