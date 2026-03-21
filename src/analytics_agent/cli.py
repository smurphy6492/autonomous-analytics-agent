"""Command-line interface for the Autonomous Analytics Agent."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    name="analytics-agent",
    help=(
        "Autonomous Analytics Agent — takes a dataset and a business question, "
        "then produces a self-contained HTML report with an executive summary, "
        "data tables, and Plotly charts."
    ),
    add_completion=False,
)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Silence noisy third-party loggers unless verbose.
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("anthropic").setLevel(logging.WARNING)


@app.command()
def analyze(
    data_dir: Annotated[
        Path,
        typer.Option(
            "--data-dir",
            "-d",
            help=(
                "Directory containing CSV files to analyse. "
                "All .csv files in the directory are loaded."
            ),
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
        ),
    ],
    question: Annotated[
        str,
        typer.Option(
            "--question",
            "-q",
            help="The business question to answer (wrap in quotes).",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help=(
                "Output path for the HTML report. "
                "Defaults to output/<question_slug>.html."
            ),
            writable=True,
        ),
    ] = None,
    title: Annotated[
        str,
        typer.Option(
            "--title",
            "-t",
            help="Report title shown in the HTML header.",
        ),
    ] = "Analytics Report",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable debug-level logging.",
        ),
    ] = False,
) -> None:
    """Run the analytics pipeline against a directory of CSV files."""
    _configure_logging(verbose)
    logger = logging.getLogger("analytics_agent.cli")

    # Discover CSVs in the data directory.
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        typer.echo(f"Error: no .csv files found in {data_dir}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Found {len(csv_files)} CSV file(s) in {data_dir}:")
    for f in csv_files:
        typer.echo(f"  {f.name}")
    typer.echo()

    # Late imports so startup is fast when --help is used.
    try:
        from analytics_agent.config import get_settings
        from analytics_agent.pipeline.runner import PipelineRunner
    except ImportError as exc:
        typer.echo(f"Import error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        settings = get_settings()
    except ValueError as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    # Resolve output path now so we can display it after the run.
    if output is None:
        slug = re.sub(r"[^\w\s-]", "", title.lower())
        slug = re.sub(r"[-\s]+", "_", slug)[:40]
        resolved_output = Path(settings.output_dir) / f"{slug}.html"
    else:
        resolved_output = output

    runner = PipelineRunner(settings=settings)

    typer.echo(f"Question: {question}")
    typer.echo(f"Model:    {settings.model}")
    typer.echo(f"Output:   {resolved_output}")
    typer.echo()

    try:
        report = runner.run(
            data_paths=csv_files,
            business_question=question,
            title=title,
            output_path=resolved_output,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Pipeline failed: %s", exc, exc_info=verbose)
        typer.echo(f"\nPipeline failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    # Success summary.
    successful_charts = sum(1 for c in report.rendered_charts if c.success)
    typer.echo("\nDone!")
    typer.echo(f"  Report         : {resolved_output}")
    typer.echo(f"  Execution time : {report.execution_time_ms / 1000:.1f}s")
    typer.echo(f"  Queries run    : {len(report.query_results)}")
    typer.echo(f"  Charts rendered: {successful_charts}")
    if report.errors:
        typer.echo(f"  Warnings       : {len(report.errors)}")
        for err in report.errors:
            typer.echo(f"    - {err}")


def main() -> None:
    """Entry point for the ``analytics-agent`` console script."""
    app()


if __name__ == "__main__":
    main()
