# moneygrow-ai

MoneyGrowAI bundles a set of FastAPI-powered microservices that turn raw market chatter and news into actionable signals. The services focus on extracting articles, classifying sentiment with FinBERT, surfacing emerging tickers, and computing price and risk metrics to support the broader MoneyGrowAI research and content pipeline.

## What It Does
- Pulls clean article bodies (including Reddit threads) for further processing.
- Scores news headlines with FinBERT sentiment to track bullish/bearish tone over time.
- Flags ticker symbols across stories and comments, optionally verifying them with Yahoo Finance.
- Calculates technical and risk metrics (returns, RSI, Sharpe, drawdowns, bubble score, etc.) from historical prices.

## Repository Layout
- `article_extractor/` — FastAPI service that normalizes article and Reddit content.
- `sentiment_api/` — FinBERT-based sentiment scoring service with batch support.
- `stock_metrics/` — Metrics API that downloads daily prices via yfinance and derives technical indicators.
- `ticker_discovery/` — Ticker detection and verification pipeline shaped for n8n workflows.
- `compose.yaml` — Development compose file for spinning services up together.

## Getting Started
1. Create a virtual environment: `python -m venv .venv && source .venv/bin/activate`.
2. Install shared dependencies: `pip install fastapi uvicorn[standard] transformers torch httpx trafilatura pandas numpy yfinance`.
3. Launch any service locally with uvicorn, e.g. `uvicorn article_extractor.article_extractor:app --reload` (each folder ships a `Makefile` and `compose.yaml` if you prefer Docker).
4. Run tests with `pytest`; lint and format with `ruff check .` and `ruff format .`.

## Stay Connected
- Blog: http://www.carloslapao.com/#/blog — deep dives and project updates.
- Substack: https://moneygrowai.substack.com — weekly market intelligence powered by MoneyGrowAI.

Contributions and ideas are welcome—open an issue or share feedback through the blog or Substack community.
