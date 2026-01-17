.PHONY: help install install-dev test lint format type-check clean run build

help:
	@echo "Available commands:"
	@echo "  make install      - Install CSI package"
	@echo "  make install-dev  - Install CSI with dev dependencies"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code"
	@echo "  make type-check   - Run type checker"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make build        - Build package"

install:
	pip install -e .

install-dev:
	pip install -e .[dev]
	pre-commit install

test:
	pytest tests/ -v --cov=csi --cov-report=term-missing

lint:
	ruff check csi tests
	ruff format --check csi tests

format:
	ruff format csi tests
	ruff check --fix csi tests

type-check:
	mypy csi

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

build:
	python -m build

