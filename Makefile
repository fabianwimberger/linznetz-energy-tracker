.PHONY: all build up down clean lint format test typecheck setup

PYTHON ?= .venv/bin/python

all: build

build:
	@echo "Building Docker image..."
	@docker compose build

up:
	@echo "Starting services..."
	@docker compose up -d

down:
	@echo "Stopping services..."
	@docker compose down

clean:
	@echo "Cleaning up..."
	@docker compose down -v

setup:
	@echo "Downloading vendor libraries..."
	@$(PYTHON) download_vendors.py

lint:
	@echo "Running linters..."
	@$(PYTHON) -m ruff check .

format:
	@echo "Formatting code..."
	@$(PYTHON) -m ruff format .

typecheck:
	@echo "Running type checker..."
	@$(PYTHON) -m mypy .

test:
	@echo "Running tests..."
	@$(PYTHON) -m pytest -v --cov=. --cov-report=term-missing
