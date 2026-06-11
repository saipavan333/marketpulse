# ============================================================================
# MarketPulse — developer workflow
# ============================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local dev --
install: ## Install package + dev/test dependencies into current env
	pip install -e ".[dev,spark,warehouse]"

lint: ## Run ruff lint + format check
	ruff check src tests dags scripts
	ruff format --check src tests dags scripts

format: ## Auto-format and fix lint issues
	ruff format src tests dags scripts
	ruff check --fix src tests dags scripts

typecheck: ## Run mypy static type checks
	mypy src

test: ## Run unit tests (fast, no services needed)
	pytest tests/unit -v

test-spark: ## Run Spark-marked tests (local Spark session)
	pytest -m spark -v

test-integration: ## Run the end-to-end local pipeline test
	pytest tests/integration -v

test-all: ## Run the full test suite with coverage
	pytest --cov=marketpulse --cov-report=term-missing

demo: ## Run the COMPLETE pipeline locally, no Docker needed (great first step!)
	python scripts/run_local_pipeline.py --symbols AAPL,GS,NVDA --minutes 30

# ------------------------------------------------------------- docker stack --
up: ## Start the full platform (Kafka, MinIO, Spark, Airflow, Postgres, dashboard)
	cp -n .env.example .env || true
	docker compose up -d --build
	@echo "Airflow:   http://localhost:8080  (admin/admin)"
	@echo "MinIO:     http://localhost:9001  (minioadmin/minioadmin)"
	@echo "Spark UI:  http://localhost:8081"
	@echo "Dashboard: http://localhost:8501"

down: ## Stop the platform
	docker compose down

destroy: ## Stop and delete ALL data volumes
	docker compose down -v

produce: ## Start the market data producer (streams events into Kafka)
	docker compose exec app python -m marketpulse.cli produce --duration 600

stream: ## Start Spark Structured Streaming job (Kafka -> bronze Delta)
	docker compose exec app python -m marketpulse.cli stream

logs: ## Tail logs from all services
	docker compose logs -f --tail=50

ps: ## Show service status
	docker compose ps

# --------------------------------------------------------------------- dbt --
dbt-build: ## Run dbt models + tests against the warehouse
	docker compose exec app bash -c "cd dbt/marketpulse_dbt && dbt build --profiles-dir ."

dbt-docs: ## Generate dbt documentation site
	docker compose exec app bash -c "cd dbt/marketpulse_dbt && dbt docs generate --profiles-dir ."

# -------------------------------------------------------------------- misc --
clean: ## Remove caches and local data artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf data/ lake/ checkpoints/ spark-warehouse/ metastore_db/ derby.log

.PHONY: help install lint format typecheck test test-spark test-integration test-all demo \
        up down destroy produce stream logs ps dbt-build dbt-docs clean
