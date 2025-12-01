.PHONY: all build test test-e2e test-all test-coverage test-race clean db help

BINARY_NAME=benchmark
BINARY_PATH=./cmd/benchmark

# Detect docker compose command (docker-compose vs docker compose)
DOCKER_COMPOSE := $(shell command -v docker-compose 2>/dev/null)
ifndef DOCKER_COMPOSE
	DOCKER_COMPOSE := docker compose
endif

# Default target - starts database and builds
all: db build
	@echo ""
	@echo "✓ Development environment ready!"
	@echo "  Database: running on localhost:5432"
	@echo "  Binary: ./$(BINARY_NAME)"
	@echo ""
	@echo "Run: ./$(BINARY_NAME) --help"

# Start the database
db:
	@echo "Starting TimescaleDB..."
	@$(DOCKER_COMPOSE) up -d timescaledb
	@echo "Waiting for database to be ready..."
	@sleep 2
	@$(DOCKER_COMPOSE) exec -T timescaledb pg_isready -U postgres -d homework > /dev/null 2>&1 || sleep 3
	@echo "✓ Database ready on localhost:5432"

# Build the benchmark tool
build:
	@echo "Building benchmark tool..."
	@go build -o $(BINARY_NAME) $(BINARY_PATH)
	@echo "✓ Build complete: ./$(BINARY_NAME)"

# Run unit tests only (exclude e2e tests)
test:
	@echo "Running unit tests..."
	@go test -short ./...

# Run end-to-end tests (requires Docker)
test-e2e:
	@echo "Running end-to-end tests..."
	@echo "Note: This will start TimescaleDB containers via TestContainers"
	@go test -v -timeout 5m ./test/e2e/...

# Run all tests (unit + e2e)
test-all:
	@echo "Running all tests (unit + e2e)..."
	@go test -v -timeout 5m ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report: coverage.html"

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	@go test -race ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f $(BINARY_NAME)
	@rm -f coverage.out coverage.html
	@rm -rf profiles/
	@echo "✓ Clean complete"

# Help
help:
	@echo "TimescaleDB Benchmark Tool - Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make              Start database and build"
	@echo "  make all          Start database and build"
	@echo "  make db           Start TimescaleDB database only"
	@echo "  make build        Build the benchmark tool only"
	@echo ""
	@echo "Testing:"
	@echo "  make test         Run unit tests"
	@echo "  make test-e2e     Run end-to-end tests (requires Docker)"
	@echo "  make test-all     Run all tests (unit + e2e)"
	@echo "  make test-coverage Run tests with coverage report"
	@echo "  make test-race    Run tests with race detector"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean        Remove build artifacts"
	@echo ""
	@echo "Quick start:"
	@echo "  make              # Sets up everything"
	@echo "  ./$(BINARY_NAME) --help"
