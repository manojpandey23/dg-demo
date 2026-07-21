# Contributing

Thank you for your interest in contributing to `dagster-config-framework`.

## Development setup

```bash
# Clone the repo
git clone https://github.com/manojpandey23/dg-demo.git
cd dg-demo

# Install dependencies (requires uv — https://docs.astral.sh/uv/)
make dev

# This installs all dependencies and sets up pre-commit hooks
```

## Running tests

```bash
# Unit tests (no external services needed)
make test

# With coverage report
make test-cov

# Integration tests require a running PostgreSQL instance
# Start one with:
docker compose up postgres -d
uv run pytest tests/framework/test_db_asset_integration.py -v
```

## Code quality

The project uses three tools for code quality, all configured in `pyproject.toml`:

- **ruff** — linting and import sorting
- **black** — code formatting
- **mypy** — type checking

```bash
make lint       # Check without modifying
make format     # Auto-fix formatting
make typecheck  # Run mypy
```

Pre-commit hooks run these checks automatically on every commit.

## Project structure

```
framework/          # The library — this is what gets packaged
├── builder/        # Config discovery, parsing, Dagster object construction
├── model/          # Pydantic V2 config models
├── core/           # Runtime asset/sensor/resource handlers
├── transformation/ # Column and table transform DSL
├── validation/     # Data quality rules and engine
├── postgres/       # Schema management and materialization
├── cdc/            # Change data capture
├── io/             # I/O managers
└── utils/          # Shared utilities

tests/              # Test suite (pytest)
src/test_domain/    # Example domain configs for development
demo/               # Self-contained demo (docker compose)
```

## Adding a new feature

The framework uses decorator-based registries for all extensible components.
To add a new asset type, transform function, validation rule, or resource:

1. Write the handler function with the appropriate decorator
2. Import it so the decorator runs at module load time
3. Add tests
4. Update the config models if new YAML fields are needed

See `docs/architecture.md` for the full module map and extensibility examples.

## Submitting changes

1. Fork the repo and create a feature branch from `main`
2. Make your changes with tests
3. Ensure all checks pass: `make lint && make typecheck && make test`
4. Open a pull request with a clear description of what changed and why

## Reporting issues

Open an issue on GitHub with:

- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS
