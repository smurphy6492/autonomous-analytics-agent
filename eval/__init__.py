"""Golden-answer evaluation harness for the analytics agent.

The pipeline is an LLM → SQL → summary chain. Heuristic validators (see
``analytics_agent.pipeline.validator``) catch specific failure signatures, but
they cannot prove the agent's *numbers* are correct. This package does:

- ``golden_cases`` — business questions paired with a reference query and
  expected numbers, authored independently of the agent and verified against a
  small committed dataset.
- ``scorer`` — deterministic scoring of agent output against those expected
  numbers (no LLM, no API key required).
- ``run_eval`` — runs the real agent over the golden cases and reports an
  accuracy number. Requires ``ANTHROPIC_API_KEY``.

The reference numbers are themselves reproducible: the reference SQL executes
against ``golden_dataset.csv`` and reproduces the expected values, which the
test suite asserts. So the "ground truth" is verifiable, not just asserted.
"""
