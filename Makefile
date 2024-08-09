.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo "Targets:"
	@echo "  format        Format the code"
	@echo "  lint          Lint the code"
	@echo "  build         Build the package"
	@echo "  install-e     Install the package in editable mode"
	@echo "  help          Show this help message"
	@echo ""
	@echo "  format-isort  Format the code with isort"
	@echo "  format-black  Format the code with black"
	@echo "  lint-isort    Lint the code with isort"
	@echo "  lint-black    Lint the code with black"
	@echo "  lint-pyright  Lint the code with pyright"
	@echo "  lint-flake8    the code with flake8"

.PHONY: format
format: format-isort format-black

.PHONY: format-isort
format-isort:
	isort --force-sort-within-sections --profile=black .

.PHONY: format-black
format-black:
	black -l 100 .

.PHONY: lint
lint: lint-isort lint-black lint-pyright lint-flake8

.PHONY: lint-isort
lint-isort:
	isort --force-sort-within-sections --profile=black --check .

.PHONY: lint-black
lint-black:
	black -l 100 --check .

.PHONY: lint-pyright
lint-pyright:
	pyright

.PHONY: lint-flake8
lint-flake8:
	flake8 .

.PHONY: build
build:
	python -m pip install --upgrade build

.PHONY: install-e
install-e:
	python -m pip install --upgrade -e .
