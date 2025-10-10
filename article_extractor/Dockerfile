FROM python:3.11-slim

# Install system libs that trafilatura/lxml rely on
RUN apt-get update && \
    apt-get install -y --no-install-recommends libxml2 libxslt1.1 && \
    rm -rf /var/lib/apt/lists/*

# Install runtime dependencies
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" httpx trafilatura

WORKDIR /app
COPY article_extractor.py /app/

ENV PYTHONUNBUFFERED=1 \
    PORT=5003

EXPOSE 5003
CMD ["uvicorn", "article_extractor:app", "--host", "0.0.0.0", "--port", "5003"]
