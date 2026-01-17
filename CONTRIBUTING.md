# Contributing to Cold Storage Inspector (CSI)

Thank you for your interest in contributing to CSI! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/cold-storage-inspector.git`
3. Create a virtual environment: `python -m venv venv`
4. Activate the virtual environment: `source venv/bin/activate` (or `venv\Scripts\activate` on Windows)
5. Install dependencies: `pip install -e .[dev]`
6. Install pre-commit hooks: `pre-commit install`
7. Run tests: `pytest`

## Code Style

- We use `ruff` for linting and formatting
- Run `ruff check .` and `ruff format .` before committing
- We use `mypy` for type checking (run `mypy csi`)
- Follow PEP 8 style guidelines

## Testing

- Write tests for all new features
- Maintain at least 80% code coverage
- Run tests with: `pytest`
- Run tests with coverage: `pytest --cov=csi`

## Git Workflow

1. Create a branch from `main`: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Write/update tests
4. Ensure all tests pass: `pytest`
5. Run linting: `ruff check .`
6. Commit your changes: `git commit -m "Add feature: description"`
7. Push to your fork: `git push origin feature/your-feature-name`
8. Open a Pull Request

## Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in imperative mood (e.g., "Add", "Fix", "Update")
- Reference issues when applicable: "Fix #123: description"

## Pull Request Process

1. Ensure your PR includes tests for new functionality
2. Ensure all CI checks pass
3. Request review from maintainers
4. Address any review feedback
5. Squash commits if requested

## Questions?

Open an issue for questions or discussions about contributions.

