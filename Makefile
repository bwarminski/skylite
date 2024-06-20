# Makefile for my-api project

# Go parameters
GO := go
GOFILES := $(shell find . -name '*.go' -not -path "./vendor/*")

# Application name and build output
APP_NAME := s3qlite
BIN_DIR := bin
BUILD_OUTPUT := $(BIN_DIR)/$(APP_NAME)

# Commands
.PHONY: all build run clean test

# Default target
all: build

# Build the application
build:
	@echo "Building the application..."
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BUILD_OUTPUT) ./cmd/$(APP_NAME)

# Run the application
run: build
	@echo "Running the application..."
	$(BUILD_OUTPUT)

# Clean the build output
clean:
	@echo "Cleaning up..."
	$(GO) clean
	rm -rf $(BIN_DIR)

# Run tests
test:
	@echo "Running tests..."
	$(GO) test ./...

# Format the code
fmt:
	@echo "Formatting the code..."
	$(GO) fmt ./...

# Run linting (assuming you have golint installed)
lint:
	@echo "Linting the code..."
	golint ./...

# Install dependencies (if using Go modules)
deps:
	@echo "Getting dependencies..."
	$(GO) mod tidy

# Help command to display all available commands
help:
	@echo "Makefile commands:"
	@echo "  all    - Build the application (default)"
	@echo "  build  - Build the application"
	@echo "  run    - Run the application"
	@echo "  clean  - Clean the build output"
	@echo "  test   - Run tests"
	@echo "  fmt    - Format the code"
	@echo "  lint   - Run linting"
	@echo "  deps   - Install dependencies"
