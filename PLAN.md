# Autonomous Analytics Agent — Implementation Plan

**Goal:** Build a multi-agent pipeline that takes an arbitrary dataset + business question and autonomously produces a professional HTML report with executive summary, SQL-generated data tables, and Python-generated charts.

**Portfolio Value:** Demonstrates autonomous task execution, multi-agent orchestration, real-world data pipelines, and decision-making loops — the core skills highlighted in Sean's Analytics + AI Systems Builder brand.

---

## Table of Contents

1. [Dataset Selection](#1-dataset-selection)
2. [Project Structure](#2-project-structure)
3. [Agent Definitions](#3-agent-definitions)
4. [Pipeline Orchestration](#4-pipeline-orchestration)
5. [Chart Spec Format](#5-chart-spec-format)
6. [Report Template](#6-report-template)
7. [Build Sequence](#7-build-sequence)
8. [Test Strategy](#8-test-strategy)
9. [Portfolio Integration](#9-portfolio-integration)

---

## 1. Dataset Selection

### Recommended: Brazilian E-Commerce Public Dataset (Olist)

**Source:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

**Why this dataset:**
- Real-world e-commerce data with orders, customers, products, payments, reviews
- Multiple related tables — demonstrates proper data modeling and joins
- ~100k orders — large enough to be interesting, small enough to run locally
- Rich dimensions: time (2016-2018), geography (states), product categories, payment methods
- Well-documented schema with clear business meaning

**Tables (8 CSVs):**
| File | Records | Key Columns |
|------|---------|-------------|
| `olist_orders_dataset.csv` | 99,441 | order_id, customer_id, order_status, order_purchase_timestamp |
| `olist_order_items_dataset.csv` | 112,650 | order_id, product_id, seller_id, price, freight_value |
| `olist_customers_dataset.csv` | 99,441 | customer_id, customer_city, customer_state |
| `olist_products_dataset.csv` | 32,951 | product_id, product_category_name |
| `olist_sellers_dataset.csv` | 3,095 | seller_id, seller_city, seller_state |
| `olist_order_payments_dataset.csv` | 103,886 | order_id, payment_type, payment_value |
| `olist_order_reviews_dataset.csv` | 100,000 | order_id, review_score, review_comment_message |
| `olist_geolocation_dataset.csv` | 1,000,000 | zip_code_prefix, lat, lng, city, state |

**Download instructions:**
1. Kaggle account required (free)
2. Download via Kaggle CLI: `kaggle datasets download olistbr/brazilian-ecommerce`
3. Or manual download from the URL above
4. Place unzipped CSVs in `data/raw/olist/`

**Alternative datasets (if Kaggle access is problematic):**
- **Superstore Sales** (Tableau sample) — simpler, ~10k rows
- **NYC Taxi Data** (subset) — trip-level data, good for time series
- **Instacart Market Basket** — 3M+ orders, may be too large for demo

---

## 2. Project Structure

```
autonomous-analytics-agent/
├── PLAN.md                          # This file
├── README.md                        # Project overview and usage
├── pyproject.toml                   # Dependencies and tooling config
├── Makefile                         # Dev commands
├── .pre-commit-config.yaml
├── .env.example                     # ANTHROPIC_API_KEY placeholder
├── .gitignore
│
├── src/
│   └── analytics_agent/
│       ├── __init__.py
│       ├── config.py                # Settings, API keys, paths
│       ├── cli.py                   # Command-line interface
│       │
│       ├── agents/                  # Agent implementations
│       │   ├── __init__.py
│       │   ├── base.py              # BaseAgent class with retry logic
│       │   ├── data_profiler.py     # Data Profiler Agent
│       │   ├── orchestrator.py      # Analytics Orchestrator Agent
│       │   ├── sql_analyst.py       # SQL Analyst Agent
│       │   └── viz_agent.py         # Python Viz Agent
│       │
│       ├── models/                  # Pydantic models for data contracts
│       │   ├── __init__.py
│       │   ├── profile.py           # DataProfile, ColumnStats, etc.
│       │   ├── query_plan.py        # QueryPlan, AnalysisStep, etc.
│       │   ├── chart_spec.py        # ChartSpec, ChartType enum, etc.
│       │   └── report.py            # ReportConfig, Section, etc.
│       │
│       ├── pipeline/                # Pipeline orchestration
│       │   ├── __init__.py
│       │   ├── runner.py            # Main pipeline execution
│       │   └── context.py           # PipelineContext (shared state)
│       │
│       ├── db/                      # DuckDB utilities
│       │   ├── __init__.py
│       │   └── engine.py            # Connection, query execution, error handling
│       │
│       ├── viz/                     # Visualization utilities
│       │   ├── __init__.py
│       │   └── renderer.py          # Plotly chart rendering from specs
│       │
│       └── report/                  # Report generation
│           ├── __init__.py
│           ├── builder.py           # HTML report assembly
│           └── templates/
│               └── report.html.jinja2  # Jinja2 template
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Fixtures: sample data, mock agents
│   ├── test_agents/
│   │   ├── test_data_profiler.py
│   │   ├── test_orchestrator.py
│   │   ├── test_sql_analyst.py
│   │   └── test_viz_agent.py
│   ├── test_models/
│   │   └── test_chart_spec.py
│   ├── test_pipeline/
│   │   └── test_runner.py
│   └── test_db/
│       └── test_engine.py
│
├── data/
│   ├── raw/                         # Downloaded datasets (gitignored)
│   │   └── olist/                   # Olist CSVs go here
│   └── sample/                      # Small sample files for testing (committed)
│       └── sample_orders.csv
│
├── output/                          # Generated reports (gitignored)
│   └── .gitkeep
│
└── examples/
    ├── run_retail_analysis.py       # Example: full pipeline run
    └── sample_questions.md          # Example business questions
```

---

## 3. Agent Definitions

### 3.1 Data Profiler Agent

**Purpose:** Analyze the input dataset and produce a structured profile that downstream agents can use to write correct SQL and choose appropriate visualizations.

**Inputs:**
```python
@dataclass
class ProfileRequest:
    data_paths: list[Path]           # Paths to CSV/Parquet files
    table_names: list[str] | None    # Optional custom table names
```

**Outputs:**
```python
@dataclass
class DataProfile:
    tables: list[TableProfile]
    relationships: list[Relationship]  # Detected FK relationships
    suggested_grain: str               # e.g., "order_id" or "order_id + product_id"
    data_quality_issues: list[str]     # Warnings for downstream agents

@dataclass
class TableProfile:
    name: str
    row_count: int
    columns: list[ColumnProfile]

@dataclass
class ColumnProfile:
    name: str
    dtype: str                         # int64, float64, object, datetime64, etc.
    null_count: int
    null_pct: float
    unique_count: int
    cardinality: str                   # "low" (<20), "medium" (20-1000), "high" (>1000)
    sample_values: list[str]           # 5 sample values
    is_date: bool
    is_numeric: bool
    min_value: str | None              # For numeric/date columns
    max_value: str | None
```

**Prompt Design Notes:**
- System prompt establishes role as a data profiler
- User prompt includes: file paths, schema info from DuckDB `DESCRIBE`, sample rows
- Output is structured JSON matching the `DataProfile` model
- Use `response_format` with JSON schema if using Claude's structured output

**Tools Needed:**
- DuckDB query execution (via `db/engine.py`)
- File system read (to check file existence)

**Retry Logic:**
- If DuckDB fails to read a file, return error in `data_quality_issues` rather than failing

---

### 3.2 Analytics Orchestrator Agent

**Purpose:** The "brain" of the pipeline. Takes the business question + data profile and produces:
1. A query plan (what SQL to run)
2. After SQL results: an executive summary + chart specifications
3. After charts: final report assembly

**This agent is called multiple times during the pipeline.**

#### Call 1: Query Planning

**Inputs:**
```python
@dataclass
class QueryPlanRequest:
    business_question: str
    data_profile: DataProfile
```

**Outputs:**
```python
@dataclass
class QueryPlan:
    analysis_approach: str             # 1-2 sentence summary of approach
    queries: list[PlannedQuery]

@dataclass
class PlannedQuery:
    query_id: str                      # e.g., "time_series", "summary"
    purpose: str                       # What this query answers
    required_tables: list[str]
    required_columns: list[str]
    aggregation_grain: str             # e.g., "daily", "monthly", "by_category"
    expected_output_type: str          # "time_series", "summary_table", "breakdown"
```

**Prompt Design Notes:**
- Include the full data profile in the prompt
- Ask the model to think step-by-step about what analyses would answer the question
- Constrain to 2-4 queries max (avoid over-analysis)
- Output is structured JSON

---

#### Call 2: Results Synthesis

**Inputs:**
```python
@dataclass
class SynthesisRequest:
    business_question: str
    query_plan: QueryPlan
    query_results: dict[str, QueryResult]  # query_id -> result data
```

**Outputs:**
```python
@dataclass
class AnalysisSynthesis:
    executive_summary: str             # 3-5 sentences, key findings
    key_metrics: list[KeyMetric]
    chart_specs: list[ChartSpec]       # Defined in section 5
    data_tables: list[DataTableSpec]   # Tables to include in report

@dataclass
class KeyMetric:
    label: str                         # e.g., "Total Revenue"
    value: str                         # e.g., "$1.2M"
    context: str | None                # e.g., "+15% vs prior period"
```

**Prompt Design Notes:**
- Include actual query results (as markdown tables or JSON)
- Ask for specific, data-backed insights (not generic statements)
- Chart specs should reference the actual data available
- Executive summary should directly answer the business question

---

### 3.3 SQL Analyst Agent

**Purpose:** Generate and execute SQL queries based on the query plan. Handle errors with retry.

**Inputs:**
```python
@dataclass
class SQLRequest:
    planned_query: PlannedQuery
    data_profile: DataProfile
    previous_error: str | None = None  # For retry attempts
```

**Outputs:**
```python
@dataclass
class QueryResult:
    query_id: str
    sql: str                           # The SQL that was executed
    success: bool
    data: list[dict] | None            # Query results as list of row dicts
    row_count: int
    error: str | None
    attempts: int                      # How many attempts it took
```

**Prompt Design Notes:**
- System prompt: "You are a SQL analyst. Write DuckDB-compatible SQL."
- Include the data profile (table names, column names, types)
- Include the specific `PlannedQuery` with its purpose
- If retrying, include the previous SQL and error message
- Ask for SQL only (no explanation) in a specific format

**Error Handling / Retry Loop:**
```
attempt 1: generate SQL, execute
  if success: return result
  if error: extract error message

attempt 2: re-prompt with error message, generate fixed SQL, execute
  if success: return result
  if error: extract error message

attempt 3: same as attempt 2
  if error: return failure with all attempts logged
```

**Max retries:** 3

**Common DuckDB errors to handle:**
- Column not found → check profile for correct column name
- Type mismatch → cast appropriately
- Syntax error → fix syntax
- Ambiguous column → add table alias

---

### 3.4 Python Viz Agent

**Purpose:** Render Plotly charts from chart specifications.

**Inputs:**
```python
@dataclass
class VizRequest:
    chart_spec: ChartSpec
    data: list[dict]                   # The data to visualize
```

**Outputs:**
```python
@dataclass
class RenderedChart:
    chart_id: str
    html: str                          # Plotly figure as HTML div
    success: bool
    error: str | None
```

**Implementation Notes:**

This agent is different from the others — it may not need an LLM call at all. The chart spec is already structured, so the Viz Agent can be deterministic Python code that:
1. Validates the chart spec
2. Builds a Plotly figure using the spec
3. Renders to HTML

**However**, if the chart spec is ambiguous or the data doesn't match, an LLM call can be used to:
- Fix data column mapping issues
- Adjust chart parameters for better display
- Handle edge cases (e.g., too many categories)

**Recommended approach:** Start deterministic, add LLM fallback only if needed.

**Plotly chart types to support:**
| ChartType | Plotly Express Function |
|-----------|-------------------------|
| `line` | `px.line()` |
| `bar` | `px.bar()` |
| `horizontal_bar` | `px.bar(orientation='h')` |
| `pie` | `px.pie()` |
| `scatter` | `px.scatter()` |
| `heatmap` | `px.imshow()` or `go.Heatmap` |
| `table` | Rendered as HTML table, not Plotly |

---

## 4. Pipeline Orchestration

### 4.1 Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PIPELINE RUNNER                             │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: Data Profiling                                              │
│   Input:  data_paths, table_names                                   │
│   Agent:  DataProfilerAgent                                         │
│   Output: DataProfile                                               │
│   Store:  context.profile                                           │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: Query Planning                                              │
│   Input:  business_question, context.profile                        │
│   Agent:  OrchestratorAgent.plan_queries()                          │
│   Output: QueryPlan                                                 │
│   Store:  context.query_plan                                        │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: SQL Execution (loop over planned queries)                   │
│   For each PlannedQuery in context.query_plan.queries:              │
│     Input:  planned_query, context.profile                          │
│     Agent:  SQLAnalystAgent (with retry loop)                       │
│     Output: QueryResult                                             │
│     Store:  context.query_results[query_id]                         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: Results Synthesis                                           │
│   Input:  business_question, context.query_plan, context.results    │
│   Agent:  OrchestratorAgent.synthesize()                            │
│   Output: AnalysisSynthesis (summary, metrics, chart_specs)         │
│   Store:  context.synthesis                                         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 5: Chart Rendering (loop over chart specs)                     │
│   For each ChartSpec in context.synthesis.chart_specs:              │
│     Input:  chart_spec, relevant data from context.query_results    │
│     Agent:  VizAgent                                                │
│     Output: RenderedChart                                           │
│     Store:  context.rendered_charts[chart_id]                       │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 6: Report Generation                                           │
│   Input:  context (all accumulated data)                            │
│   Module: ReportBuilder                                             │
│   Output: HTML file written to output/                              │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 PipelineContext

```python
@dataclass
class PipelineContext:
    """Shared state across pipeline steps."""

    # Inputs
    data_paths: list[Path]
    table_names: list[str]
    business_question: str
    output_path: Path

    # Accumulated state
    profile: DataProfile | None = None
    query_plan: QueryPlan | None = None
    query_results: dict[str, QueryResult] = field(default_factory=dict)
    synthesis: AnalysisSynthesis | None = None
    rendered_charts: dict[str, RenderedChart] = field(default_factory=dict)

    # Execution metadata
    start_time: datetime | None = None
    end_time: datetime | None = None
    errors: list[str] = field(default_factory=list)
    agent_calls: list[AgentCallLog] = field(default_factory=list)
```

### 4.3 Error Handling Strategy

| Error Type | Handling |
|------------|----------|
| Data file not found | Fail fast with clear error message |
| DuckDB query syntax error | Retry with SQL Analyst (up to 3 attempts) |
| DuckDB query returns 0 rows | Log warning, continue (empty result is valid) |
| Claude API error (rate limit) | Exponential backoff retry (3 attempts) |
| Claude API error (other) | Fail with error logged |
| Chart rendering error | Log warning, skip chart, continue |
| All queries failed | Generate report with error section instead of data |

### 4.4 Logging

Use Python `logging` module with structured output:
```python
logger = logging.getLogger("analytics_agent")

# Log format
"%(asctime)s | %(levelname)s | %(name)s | %(message)s"

# Key log points:
# - Pipeline start/end
# - Each agent call (input summary, output summary, duration)
# - SQL query execution (query, row count, duration)
# - Errors and retries
```

---

## 5. Chart Spec Format

### 5.1 ChartSpec Model

```python
from enum import Enum
from pydantic import BaseModel

class ChartType(str, Enum):
    LINE = "line"
    BAR = "bar"
    HORIZONTAL_BAR = "horizontal_bar"
    PIE = "pie"
    SCATTER = "scatter"
    HEATMAP = "heatmap"

class ChartSpec(BaseModel):
    """Specification for a chart to be rendered."""

    chart_id: str                      # Unique identifier
    chart_type: ChartType
    title: str

    # Data mapping
    data_source: str                   # query_id that provides the data
    x_column: str | None = None        # Column for x-axis (not needed for pie)
    y_column: str | None = None        # Column for y-axis
    color_column: str | None = None    # Column for color grouping
    size_column: str | None = None     # Column for size (scatter only)
    values_column: str | None = None   # Column for values (pie only)
    names_column: str | None = None    # Column for names (pie only)

    # Display options
    x_label: str | None = None
    y_label: str | None = None
    show_legend: bool = True
    height: int = 400                  # Pixels

    # Formatting
    x_format: str | None = None        # e.g., "%Y-%m-%d" for dates
    y_format: str | None = None        # e.g., ",.0f" for numbers
    color_palette: str = "plotly"      # Plotly color palette name

class Config:
    extra = "forbid"                   # Fail if unknown fields are passed
```

### 5.2 Example Chart Specs

**Line chart (time series):**
```json
{
  "chart_id": "revenue_over_time",
  "chart_type": "line",
  "title": "Monthly Revenue Trend",
  "data_source": "monthly_revenue",
  "x_column": "month",
  "y_column": "total_revenue",
  "x_label": "Month",
  "y_label": "Revenue (R$)",
  "y_format": ",.0f"
}
```

**Bar chart (breakdown):**
```json
{
  "chart_id": "revenue_by_category",
  "chart_type": "bar",
  "title": "Revenue by Product Category",
  "data_source": "category_breakdown",
  "x_column": "category",
  "y_column": "revenue",
  "color_column": "category",
  "x_label": "Category",
  "y_label": "Revenue (R$)"
}
```

**Pie chart:**
```json
{
  "chart_id": "payment_method_share",
  "chart_type": "pie",
  "title": "Payment Method Distribution",
  "data_source": "payment_breakdown",
  "values_column": "payment_count",
  "names_column": "payment_type"
}
```

---

## 6. Report Template

### 6.1 HTML Structure

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ report_title }}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        /* Embedded CSS - clean, professional styling */
    </style>
</head>
<body>
    <header>
        <h1>{{ report_title }}</h1>
        <p class="subtitle">Generated on {{ generated_at }}</p>
        <p class="question"><strong>Business Question:</strong> {{ business_question }}</p>
    </header>

    <section id="executive-summary">
        <h2>Executive Summary</h2>
        <div class="summary-text">{{ executive_summary }}</div>

        <div class="key-metrics">
            {% for metric in key_metrics %}
            <div class="metric-card">
                <div class="metric-value">{{ metric.value }}</div>
                <div class="metric-label">{{ metric.label }}</div>
                {% if metric.context %}
                <div class="metric-context">{{ metric.context }}</div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
    </section>

    <section id="analysis">
        <h2>Analysis</h2>

        {% for chart in charts %}
        <div class="chart-container">
            <h3>{{ chart.title }}</h3>
            {{ chart.html | safe }}
        </div>
        {% endfor %}

        {% for table in data_tables %}
        <div class="table-container">
            <h3>{{ table.title }}</h3>
            {{ table.html | safe }}
        </div>
        {% endfor %}
    </section>

    <section id="methodology">
        <h2>Methodology</h2>
        <p><strong>Data Sources:</strong> {{ data_sources | join(", ") }}</p>
        <p><strong>Analysis Approach:</strong> {{ analysis_approach }}</p>
        <details>
            <summary>SQL Queries Used</summary>
            {% for query in queries %}
            <div class="query-block">
                <p><strong>{{ query.purpose }}</strong></p>
                <pre><code>{{ query.sql }}</code></pre>
            </div>
            {% endfor %}
        </details>
    </section>

    <footer>
        <p>Generated by Autonomous Analytics Agent</p>
        <p>Pipeline execution time: {{ execution_time }}</p>
    </footer>
</body>
</html>
```

### 6.2 CSS Design Notes

- Clean, professional look suitable for business reports
- Mobile-responsive (works on phone/tablet)
- Light color scheme with subtle shadows
- Metric cards in a flex grid
- Charts at full container width
- Collapsible methodology section (don't clutter main view)
- Print-friendly (hide unnecessary elements, good page breaks)

---

## 7. Build Sequence

### Phase 1: Project Scaffold (Day 1)

**What:** Set up project structure, dependencies, and tooling.

**Files created:**
- `pyproject.toml`
- `Makefile`
- `.pre-commit-config.yaml`
- `.gitignore`
- `.env.example`
- `src/analytics_agent/__init__.py`
- `src/analytics_agent/config.py`
- `tests/__init__.py`
- `tests/conftest.py`

**Dependencies:**
```toml
dependencies = [
    "anthropic>=0.39.0",
    "duckdb>=1.1.0",
    "plotly>=5.24.0",
    "pandas>=2.2.0",
    "pydantic>=2.9.0",
    "jinja2>=3.1.0",
    "python-dotenv>=1.0.0",
    "httpx>=0.27.0",
]
```

**Verification:**
```bash
pip install -e ".[dev]"
make check  # Should pass (no code yet, but tooling works)
```

---

### Phase 2: Data Models (Day 1-2)

**What:** Define all Pydantic models for data contracts between agents.

**Files created:**
- `src/analytics_agent/models/__init__.py`
- `src/analytics_agent/models/profile.py`
- `src/analytics_agent/models/query_plan.py`
- `src/analytics_agent/models/chart_spec.py`
- `src/analytics_agent/models/report.py`
- `tests/test_models/test_chart_spec.py`

**Verification:**
- Unit tests for model validation (valid/invalid inputs)
- `make check` passes

---

### Phase 3: DuckDB Engine (Day 2)

**What:** Build the database abstraction layer.

**Files created:**
- `src/analytics_agent/db/__init__.py`
- `src/analytics_agent/db/engine.py`
- `tests/test_db/test_engine.py`
- `data/sample/sample_orders.csv` (small test file)

**Features:**
- `load_csv(path, table_name)` — register CSV as DuckDB table
- `execute(sql)` -> `list[dict]`
- `describe_table(name)` -> schema info
- `get_sample_rows(name, n)` -> sample data
- Error wrapping with clear messages

**Verification:**
- Unit tests with sample CSV
- Can load file, run query, get results

---

### Phase 4: Base Agent Class (Day 2-3)

**What:** Create the base class for all agents with Claude API integration.

**Files created:**
- `src/analytics_agent/agents/__init__.py`
- `src/analytics_agent/agents/base.py`

**Features:**
```python
class BaseAgent:
    def __init__(self, client: Anthropic, model: str = "claude-sonnet-4-20250514"):
        ...

    def call(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[BaseModel] | None = None,
        max_retries: int = 3,
    ) -> str | BaseModel:
        """Call Claude API with retry logic and optional structured output."""
        ...
```

**Verification:**
- Integration test (requires API key, mark as `@pytest.mark.integration`)
- Mock test for retry logic

---

### Phase 5: Data Profiler Agent (Day 3)

**What:** Implement the Data Profiler Agent.

**Files created:**
- `src/analytics_agent/agents/data_profiler.py`
- `tests/test_agents/test_data_profiler.py`

**Verification:**
- Unit test with sample CSV (mocked LLM call)
- Integration test with real LLM (marked skip if no API key)
- Profile output matches expected structure

---

### Phase 6: SQL Analyst Agent (Day 3-4)

**What:** Implement the SQL Analyst Agent with retry loop.

**Files created:**
- `src/analytics_agent/agents/sql_analyst.py`
- `tests/test_agents/test_sql_analyst.py`

**Verification:**
- Unit test: given a query plan, produces valid SQL
- Unit test: retry loop works (simulate error, then success)
- Integration test with DuckDB execution

---

### Phase 7: Orchestrator Agent (Day 4-5)

**What:** Implement the Analytics Orchestrator Agent (both planning and synthesis).

**Files created:**
- `src/analytics_agent/agents/orchestrator.py`
- `tests/test_agents/test_orchestrator.py`

**Verification:**
- Unit test: query planning produces valid QueryPlan
- Unit test: synthesis produces valid AnalysisSynthesis
- Chart specs reference valid data sources

---

### Phase 8: Viz Agent (Day 5)

**What:** Implement chart rendering from specs.

**Files created:**
- `src/analytics_agent/viz/__init__.py`
- `src/analytics_agent/viz/renderer.py`
- `src/analytics_agent/agents/viz_agent.py`
- `tests/test_agents/test_viz_agent.py`

**Verification:**
- Unit test: each chart type renders to HTML
- HTML contains Plotly div
- Invalid specs raise clear errors

---

### Phase 9: Report Builder (Day 5-6)

**What:** Implement HTML report generation with Jinja2.

**Files created:**
- `src/analytics_agent/report/__init__.py`
- `src/analytics_agent/report/builder.py`
- `src/analytics_agent/report/templates/report.html.jinja2`
- `src/analytics_agent/report/templates/styles.css` (embedded in template)

**Verification:**
- Unit test: generates valid HTML
- Report opens in browser and looks correct
- All sections render (even with empty data)

---

### Phase 10: Pipeline Runner (Day 6-7)

**What:** Wire all agents together in the pipeline runner.

**Files created:**
- `src/analytics_agent/pipeline/__init__.py`
- `src/analytics_agent/pipeline/context.py`
- `src/analytics_agent/pipeline/runner.py`
- `tests/test_pipeline/test_runner.py`

**Verification:**
- Integration test: full pipeline with sample data
- Report generated in `output/`
- All steps logged correctly

---

### Phase 11: CLI (Day 7)

**What:** Command-line interface for running the pipeline.

**Files created:**
- `src/analytics_agent/cli.py`
- Update `pyproject.toml` with entry point

**Usage:**
```bash
analytics-agent run \
  --data data/raw/olist/*.csv \
  --question "What is the monthly revenue trend and which product categories drive the most revenue?" \
  --output output/olist_analysis.html
```

**Verification:**
- `analytics-agent --help` works
- Full run with Olist data produces report

---

### Phase 12: Example and Documentation (Day 7-8)

**What:** Create example scripts and documentation.

**Files created:**
- `examples/run_retail_analysis.py`
- `examples/sample_questions.md`
- `README.md`

**README structure:**
1. What this project does
2. Quick start (install, set API key, run)
3. How it works (architecture diagram)
4. Example output (screenshot or link)
5. Project structure
6. Development (how to contribute/extend)

---

### Phase 13: Polish and Edge Cases (Day 8)

**What:** Handle edge cases, improve error messages, test with multiple datasets.

**Tasks:**
- Test with different datasets (not just Olist)
- Improve error messages for common failures
- Add progress output to CLI
- Ensure graceful degradation (partial results if some queries fail)
- Code review and cleanup

---

## 8. Test Strategy

### 8.1 Test Pyramid

```
                    ┌─────────────┐
                    │   E2E (1)   │  Full pipeline with real data
                    └─────────────┘
               ┌───────────────────────┐
               │   Integration (5-10)   │  Agent + DuckDB + real API
               └───────────────────────┘
          ┌─────────────────────────────────┐
          │        Unit Tests (30+)          │  Models, rendering, utilities
          └─────────────────────────────────┘
```

### 8.2 Test Categories

**Unit Tests (no external dependencies):**
- Model validation (valid/invalid inputs)
- Chart spec parsing
- DuckDB engine (with sample CSV)
- Report HTML generation (with mock data)
- Retry logic (with mock client)

**Integration Tests (require Claude API):**
- Data Profiler Agent produces valid profile
- SQL Analyst Agent generates executable SQL
- Orchestrator Agent produces valid plan
- Mark with `@pytest.mark.integration`
- Skip if `ANTHROPIC_API_KEY` not set

**End-to-End Test:**
- Full pipeline with Olist sample data
- Verify report file is created
- Verify report contains expected sections
- Run as part of CI (with API key secret)

### 8.3 Test Fixtures

```python
# conftest.py

@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a small sample CSV for testing."""
    data = """order_id,order_date,revenue,category
    1,2023-01-01,100.00,electronics
    2,2023-01-02,200.00,clothing
    3,2023-01-03,150.00,electronics
    """
    path = tmp_path / "sample.csv"
    path.write_text(data)
    return path

@pytest.fixture
def mock_anthropic_client():
    """Mock Anthropic client for unit tests."""
    ...

@pytest.fixture
def sample_data_profile() -> DataProfile:
    """Pre-built profile for testing downstream agents."""
    ...

@pytest.fixture
def sample_query_results() -> dict[str, QueryResult]:
    """Pre-built query results for testing synthesis."""
    ...
```

### 8.4 CI Configuration

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: make lint
      - run: make type-check
      - run: pytest -m "not integration"  # Unit tests only

  integration:
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: pytest -m integration
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
```

---

## 9. Portfolio Integration

### 9.1 What to Showcase

This project demonstrates:

| Capability | How It's Demonstrated |
|------------|----------------------|
| **Autonomous Task Execution** | Agents work without human intervention after initial prompt |
| **Multi-Agent Orchestration** | 4 specialized agents coordinated by pipeline runner |
| **Real-World Data Pipelines** | ETL from CSV → DuckDB → Charts → HTML |
| **Decision-Making Loops** | SQL retry loop, chart spec generation based on data |
| **Modern Python** | Type hints, Pydantic, async-ready, full test coverage |

### 9.2 Website Integration Steps

1. **Add project to portfolio** (`/portfolio-updater` skill)
   - Title: "Autonomous Analytics Agent"
   - Description: Multi-agent pipeline that answers business questions with auto-generated reports
   - Tags: Python, AI Agents, Claude API, Data Analysis, Plotly

2. **Create case study** (`content-writer` agent)
   - Problem statement
   - Architecture decisions
   - Challenges and solutions
   - Example output (embedded report or screenshot)
   - What I learned

3. **Live demo option**
   - Host a sample report on the website (static HTML)
   - Or: embed an iframe showing a generated report
   - Link to GitHub repo

4. **GitHub repo showcase**
   - Clean README with architecture diagram
   - Example output in `examples/` or `docs/`
   - GitHub Actions badge showing tests pass

### 9.3 Demo Scenarios

Prepare 2-3 canned demos with different datasets/questions:

**Demo 1: E-commerce Revenue Analysis (Olist)**
- Question: "What is the monthly revenue trend and which product categories drive the most revenue?"
- Shows: time series, bar chart, summary metrics

**Demo 2: Customer Behavior (Olist)**
- Question: "Which states have the highest customer lifetime value and what payment methods do they prefer?"
- Shows: geographic breakdown, payment analysis, heatmap

**Demo 3: Operational Metrics (Olist)**
- Question: "What is the trend in order delivery time and how does it vary by seller location?"
- Shows: time-to-delivery metrics, seller analysis

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Claude API rate limits during demo | Medium | High | Add backoff/retry, cache results for demos |
| Generated SQL is wrong/unsafe | Medium | Medium | Retry loop, SQL validation, DuckDB sandbox |
| Chart spec doesn't match data | Medium | Low | Validate specs against query results, fallback to table |
| Olist data too complex for single analysis | Low | Medium | Simplify to 2-3 tables for initial demo |
| Report HTML breaks in some browsers | Low | Low | Test in Chrome, Firefox, Safari; use standard CSS |

---

## Open Questions

1. **Structured output:** Should we use Claude's JSON mode / tool_use for guaranteed structured output, or parse from text? (Recommend: JSON mode when available)

2. **Async execution:** Should agents run async for parallelism (e.g., multiple SQL queries at once)? (Recommend: Start synchronous, add async later if needed)

3. **Caching:** Should we cache Claude API responses during development to save costs? (Recommend: Yes, use `diskcache` or similar)

4. **Dataset download:** Should the project auto-download the Olist dataset, or require manual download? (Recommend: Manual download, document clearly)

---

## Success Criteria

The project is complete when:

- [ ] `make check` passes (lint, type-check, unit tests)
- [ ] Integration tests pass with real Claude API
- [ ] Full pipeline produces a valid HTML report for Olist data
- [ ] Report answers the business question with charts and summary
- [ ] README documents installation and usage
- [ ] Code is clean enough for portfolio showcase
- [ ] At least one demo scenario runs reliably in < 60 seconds

---

## Appendix: Sample Prompts

### Data Profiler System Prompt

```
You are a data profiler agent. Your job is to analyze dataset schemas and statistics to help downstream agents write correct SQL and choose appropriate visualizations.

You will receive:
1. Table schemas (column names and types)
2. Row counts
3. Sample rows from each table
4. Basic statistics (null counts, unique counts)

Your output must be a JSON object matching the DataProfile schema. Be precise about:
- Data types (distinguish dates from strings, integers from floats)
- Cardinality (low/medium/high based on unique count)
- Relationships between tables (foreign keys)
- Data quality issues (high null rates, suspicious values)
- The suggested grain for analysis (what defines a unique record)
```

### SQL Analyst System Prompt

```
You are a SQL analyst agent. Your job is to write DuckDB-compatible SQL queries that answer specific analytical questions.

You will receive:
1. A query purpose (what question to answer)
2. A data profile (available tables and columns)
3. The expected output type (time series, summary, breakdown)

Rules:
- Use only tables and columns that exist in the profile
- Use DuckDB SQL syntax (similar to PostgreSQL)
- Include appropriate aggregations (SUM, COUNT, AVG)
- Format dates appropriately for the expected grain
- Return only the SQL query, no explanation

If you previously attempted this query and it failed, you will see the error message. Fix the issue and try again.
```

### Orchestrator System Prompt (Synthesis)

```
You are an analytics orchestrator agent. Your job is to synthesize query results into insights and visualizations.

You will receive:
1. The original business question
2. The query plan (what analyses were requested)
3. The actual query results (data tables)

Your output must include:
1. An executive summary (3-5 sentences) that directly answers the business question with specific numbers
2. Key metrics (3-5) with labels, values, and context
3. Chart specifications that visualize the key findings
4. Data tables to include in the report

Be specific and data-driven. Do not make claims that are not supported by the data.
```

---

*Plan created: 2026-03-19*
*Target completion: 8 working days from start*
