.PHONY: all build up down clean lint format test typecheck setup

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
	@python download_vendors.py

lint:
	@echo "Running linters..."
	@ruff check .

format:
	@echo "Formatting code..."
	@ruff format .

typecheck:
	@echo "Running type checker..."
	@mypy . --ignore-missing-imports

test:
	@echo "Running tests..."
	@pytest -v
