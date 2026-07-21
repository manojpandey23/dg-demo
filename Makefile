.PHONY: help install dev lint format typecheck test test-cov clean build dagster-dev docker-build docker-up docker-down demo

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

dev: ## Install all dependencies (dev + prod)
	uv sync
	uv run pre-commit install

lint: ## Run ruff linter and black check
	uv run ruff check framework/ tests/
	uv run black --check framework/ tests/

format: ## Auto-format code with ruff and black
	uv run ruff check --fix framework/ tests/
	uv run black framework/ tests/

typecheck: ## Run mypy type checker
	uv run mypy framework/

test: ## Run tests
	uv run pytest tests/

test-cov: ## Run tests with coverage report
	uv run pytest tests/ --cov=framework --cov-report=term-missing --cov-report=html

clean: ## Remove build artifacts and caches
	rm -rf dist/ build/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

build: ## Build the wheel package
	uv build

dagster-dev: ## Start Dagster dev server with all demo pipelines
	uv run dagster dev -m demo.definitions

docker-build: ## Build the Docker/Podman image
	docker build -t dagster-config-framework:latest .

docker-up: ## Start the full demo stack (Postgres + API + Dagster)
	docker compose up -d

docker-down: ## Stop the demo stack and remove volumes
	docker compose down -v

demo: docker-up ## Run the full demo stack
	@echo ""
	@echo "  Demo running — all 5 pipelines loaded"
	@echo ""
	@echo "  Dagster UI   http://localhost:3000"
	@echo "  Mock API     http://localhost:8000"
	@echo "  PostgreSQL   localhost:7432 (user: ods, db: ods)"
	@echo ""
	@echo "  Pipelines:"
	@echo "    1. Cash Balance   — API ingestion (append + full refresh)"
	@echo "    2. Orders         — transforms, merge, derived columns"
	@echo "    3. Customers      — SCD Type 2 dimension with history"
	@echo "    4. Trades         — CDC with change tracking"
	@echo "    5. File Ingestion — CSV file drop with file formatters"
	@echo ""
