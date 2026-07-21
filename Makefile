.PHONY: help install dev lint format typecheck test test-cov clean build docker-build docker-up docker-down demo

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

dagster-dev: ## Start Dagster webserver in dev mode
	uv run dagster dev -m test_domain.definitions

docker-build: ## Build the Docker/Podman image
	docker build -t dagster-config-framework:latest .

docker-up: ## Start the full demo stack (Postgres + API + Dagster)
	docker compose up -d

docker-down: ## Stop the demo stack
	docker compose down -v

demo: docker-up ## Alias for docker-up — run the full demo
	@echo "\n  Demo running at http://localhost:3000 (Dagster UI)"
	@echo "  Mock API at http://localhost:8000"
	@echo "  Postgres at localhost:7432\n"
