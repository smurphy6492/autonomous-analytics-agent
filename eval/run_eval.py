"""Run the real analytics agent over the golden cases and report accuracy.

Requires ``ANTHROPIC_API_KEY`` (it runs the actual LLM pipeline). For a
CI-safe, no-API-key check that the scoring logic and reference numbers are
sound, see ``tests/test_eval/``.

Usage::

    python -m eval.run_eval
    python -m eval.run_eval --model claude-sonnet-4-6
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

from analytics_agent.config import Settings
from analytics_agent.pipeline.runner import PipelineRunner
from eval.golden_cases import DATASET_PATH, GOLDEN_CASES
from eval.scorer import CaseScore, collect_numeric_values, overall_accuracy, score_case

logger = logging.getLogger(__name__)

# Any golden run must clear this to be considered a pass overall.
_PASS_THRESHOLD = 1.0


def run() -> int:
    """Run the golden eval end-to-end. Returns a process exit code."""
    parser = argparse.ArgumentParser(description="Run the analytics agent golden eval.")
    parser.add_argument("--model", default=None, help="Override the model id.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    settings = Settings()
    if args.model:
        settings.model = args.model

    runner = PipelineRunner(settings=settings)

    scores: list[CaseScore] = []
    with tempfile.TemporaryDirectory() as tmp:
        for case in GOLDEN_CASES:
            output_path = Path(tmp) / f"{case.case_id}.html"
            report = runner.run(
                data_paths=[DATASET_PATH],
                business_question=case.question,
                title=case.case_id,
                output_path=output_path,
            )
            agent_values = collect_numeric_values(report.query_results)
            score = score_case(case, agent_values)
            scores.append(score)
            _print_case(score)

    accuracy = overall_accuracy(scores)
    print("\n" + "=" * 60)
    print(
        f"OVERALL ACCURACY: {accuracy:.0%} "
        f"({sum(s.matched_count for s in scores)}/"
        f"{sum(s.total_count for s in scores)} reference metrics matched)"
    )
    print("=" * 60)

    return 0 if accuracy >= _PASS_THRESHOLD else 1


def _print_case(score: CaseScore) -> None:
    status = "PASS" if score.passed else "FAIL"
    print(f"\n[{status}] {score.case_id}  ({score.accuracy:.0%})")
    for m in score.metrics:
        mark = "ok" if m.matched else "MISS"
        closest = f"{m.closest:,.2f}" if m.closest is not None else "—"
        print(f"    {mark:>4}  {m.name}: expected {m.expected:,.2f}, closest {closest}")


if __name__ == "__main__":
    sys.exit(run())
