.PHONY: lint format build install-e

lint:
	uv run ruff check
	uv run ruff format --check

format:
	uv run ruff check --fix
	uv run ruff format

build:
	uv build

install-e:
	uv sync --group dev
