.PHONY: install lint format fix type-check test check clean

install:
	pip install -e ".[dev]"
	pre-commit install

# Non-mutating: reports problems and fails. This is what CI and `check` run.
lint:
	ruff check .
	ruff format --check .

# Mutating: fixes what it can. For local use before committing — never in CI,
# where auto-fixing would let a broken change pass by silently repairing it.
fix:
	ruff check . --fix
	ruff format .

format: fix

type-check:
	mypy src/

test:
	pytest

check: lint type-check test

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
