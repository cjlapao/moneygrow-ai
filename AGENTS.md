# Repository Guidelines
## Project Structure & Module Organization
The FastAPI service lives in `sentiment_api/main.py`, where the FinBERT sentiment pipeline is exposed through the `/analyze` endpoint. Keep additional routers, validators, and utilities inside `sentiment_api/` as separate modules to avoid overloading `main.py`. Place integration fixtures or sample payloads under `sentiment_api/fixtures/` if you add them. New automated tests should sit in a top-level `tests/` package mirroring the runtime module layout.

## Environment & Setup
Target Python 3.10+ and create an isolated environment before installing dependencies. Typical setup:
`python -m venv .venv && source .venv/bin/activate`
`pip install fastapi uvicorn[standard] transformers torch`
The first model download uses the Hugging Face Hub; allow the cache directory (`~/.cache/huggingface`) to persist between runs to avoid repeated downloads.

## Build, Test, and Development Commands
`uvicorn sentiment_api.main:app --reload` — launch the API locally with hot reloading.
`pytest` — execute the test suite; use `pytest -k "<pattern>"` to target specific cases when diagnosing failures.
`ruff check .` — optional lint pass; install with `pip install ruff` and run `ruff format .` before committing if formatting drift occurs.

## Coding Style & Naming Conventions
Follow PEP 8, with four-space indentation and descriptive snake_case for functions, CamelCase for classes, and uppercase module-level constants. Type hints are required on public call surfaces, especially request/response models. Keep FastAPI routers lightweight and push data-processing helpers into dedicated modules. Document any non-obvious tensor operations with concise comments.

## Testing Guidelines
Prefer `pytest` for unit and integration tests, co-locating fixtures in `tests/conftest.py`. Mock transformer outputs when possible to keep tests fast and deterministic. For real-model smoke checks, mark them with `@pytest.mark.slow` so CI can skip by default. Aim to cover new routes, validation errors, and sentiment-score edge cases.

## Commit & Pull Request Guidelines
Use short, imperative commit messages similar to the existing `first commit` history. Reference linked issues with `Closes #123` when applicable. Pull requests should describe the behavior change, note model or dependency updates, and include before/after screenshots for API contract changes (e.g., new response fields). Ensure lint and tests pass before requesting review.
