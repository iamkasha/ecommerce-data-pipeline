.PHONY: help up down seed run-pipeline dbt-run dbt-test quality test clean

help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Ecommerce Data Pipeline — Available Commands"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  make up              Start all local services (Docker)"
	@echo "  make down            Stop all services"
	@echo "  make seed            Seed source PostgreSQL with fake data"
	@echo "  make run-pipeline    Run full batch pipeline end-to-end"
	@echo "  make stream-start    Start Kafka streaming producer"
	@echo "  make dbt-run         Run all dbt models"
	@echo "  make dbt-test        Run dbt tests"
	@echo "  make quality         Run Great Expectations data quality checks"
	@echo "  make test            Run unit + integration tests"
	@echo "  make analytics       Print analytics report"
	@echo "  make clean           Remove generated files"

up:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 15
	@echo "✅ Services running:"
	@echo "   Airflow UI    → http://localhost:8080 (admin/admin)"
	@echo "   Kafka UI      → http://localhost:8090"
	@echo "   MinIO Console → http://localhost:9001 (minioadmin/minioadmin)"
	@echo "   Grafana       → http://localhost:3000 (admin/admin)"

down:
	docker compose down -v

seed:
	python scripts/seed_postgres.py

run-pipeline:
	@echo "→ Step 1: Extract (PostgreSQL → S3 Bronze)"
	python -m ingestion.batch.postgres_extractor
	@echo "→ Step 2: Bronze → Silver (PySpark)"
	python -m processing.bronze_to_silver.clean_orders
	python -m processing.bronze_to_silver.clean_users
	python -m processing.bronze_to_silver.clean_products
	@echo "→ Step 3: Data Quality Checks"
	python -m data_quality.validate
	@echo "→ Step 4: Silver → Gold (PySpark)"
	python -m processing.silver_to_gold.aggregate_daily
	@echo "→ Step 5: Load to Warehouse"
	python -m warehouse.loaders.redshift_loader
	@echo "→ Step 6: dbt Transformations"
	cd dbt && dbt run --profiles-dir .
	@echo "→ Step 7: Analytics Report"
	python -m analytics.reports
	@echo "✅ Pipeline complete!"

stream-start:
	python -m ingestion.streaming.kafka_producer

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

quality:
	python -m data_quality.validate

test:
	pytest tests/ -v --cov=. --cov-report=term-missing

analytics:
	python -m analytics.reports

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null; true
	rm -rf .pytest_cache htmlcov .coverage
