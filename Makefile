.PHONY: help install test test-unit test-integration test-local test-stress localstack-up localstack-down clean

help:
	@echo ""
	@echo "fluxio local testing"
	@echo ""
	@echo "  make install          Install fluxio + dev dependencies"
	@echo "  make test             Run all tests (unit + integration via moto)"
	@echo "  make test-unit        Run unit tests only (no AWS mock)"
	@echo "  make test-integration Run integration tests with moto (no Docker)"
	@echo "  make localstack-up    Start LocalStack via Docker Compose"
	@echo "  make localstack-down  Stop LocalStack"
	@echo "  make test-local       Run full end-to-end against LocalStack"
	@echo "  make test-stress N=20 Stress-test with N concurrent workflows"
	@echo ""

install:
	pip install -e ".[dev]"

test: test-unit test-integration

test-unit:
	pytest tests/test_exactly_once.py -v

test-integration:
	pytest tests/test_integration.py -v

localstack-up:
	docker compose up -d
	@echo "Waiting for LocalStack to be ready..."
	@until curl -sf http://localhost:4566/_localstack/health > /dev/null 2>&1; do sleep 1; done
	@echo "LocalStack is ready."

localstack-down:
	docker-compose down -v

test-local: localstack-up
	python tests/local_runner.py --verbose

N ?= 20
test-stress: localstack-up
	python tests/local_runner.py --stress $(N) --verbose

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache dist build *.egg-info
