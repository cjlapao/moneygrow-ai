# Convenience targets for the ticker discovery FastAPI service

APP_MODULE ?= ticker_discovery_service:app
HOST ?= 0.0.0.0
PORT ?= 5005
ENV_FILE ?= ../.env

PYTHON ?= python3
VENV ?= $(abspath .venv)
PYTHON_CMD ?= $(VENV)/bin/python
PIP ?= $(VENV)/bin/pip
UVICORN_ARGS ?=

DOCKER_IMAGE ?= ticker-discovery
DOCKER_TAG ?= latest
DOCKERFILE ?= Dockerfile
DOCKER_CONTEXT ?= .

DEPS_MARKER ?= $(VENV)/.deps-installed

.PHONY: install venv run run-dev test lint docker-build docker-run clean

install: $(DEPS_MARKER) ## Create the virtualenv and install dependencies

$(DEPS_MARKER): venv
	@$(PIP) install --upgrade pip setuptools wheel
	@if [ -f requirements.txt ]; then \
		$(PIP) install -r requirements.txt; \
	else \
		$(PIP) install fastapi "uvicorn[standard]" httpx; \
	fi
	@touch $(DEPS_MARKER)

venv: ## Create a local virtual environment if missing
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)

run: install ## Run the API without reload (prod-like)
	@if [ -f $(ENV_FILE) ]; then \
		ENV_FLAG="--env-file $(ENV_FILE)"; \
	else \
		ENV_FLAG=""; \
	fi; \
	$(PYTHON_CMD) -m uvicorn $(APP_MODULE) --host $(HOST) --port $(PORT) $$ENV_FLAG $(UVICORN_ARGS)

run-dev: UVICORN_ARGS += --reload
run-dev: install ## Run the API with reload for development
	@if [ -f $(ENV_FILE) ]; then \
		ENV_FLAG="--env-file $(ENV_FILE)"; \
	else \
		ENV_FLAG=""; \
	fi; \
	$(PYTHON_CMD) -m uvicorn $(APP_MODULE) --host $(HOST) --port $(PORT) $$ENV_FLAG $(UVICORN_ARGS)

test: install ## Run the fast tests
	@$(PYTHON_CMD) -m pytest $(PYTEST_ARGS)

lint: install ## Run ruff linting if available
	@if $(PYTHON_CMD) -m pip show ruff >/dev/null 2>&1; then \
		$(PYTHON_CMD) -m ruff check ..; \
	else \
		echo "ruff not installed; run '$(PIP) install ruff'"; \
	fi

docker-build: ## Build the Docker image for deployment
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f $(DOCKERFILE) $(DOCKER_CONTEXT)

docker-run: ## Run the service using the built Docker image
	docker run --rm -p $(PORT):$(PORT) $(DOCKER_IMAGE):$(DOCKER_TAG)

clean: ## Remove build artefacts and caches
	rm -rf $(VENV)
	rm -f $(DEPS_MARKER)
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
