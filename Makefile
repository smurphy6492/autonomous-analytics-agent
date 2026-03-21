.PHONY: install lint type-check test check clean

install:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check . --fix && ruff format .

type-check:
	mypy src/

test:
	pytest

check: lint type-check test

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
