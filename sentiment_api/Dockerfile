FROM python:3.11-slim

# Optional: faster wheels for PyTorch (CPU)
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
    fastapi uvicorn[standard] transformers==4.44.2 torch==2.3.1

# Hugging Face cache to persist model across restarts (optional volume)
ENV HF_HOME=/models
WORKDIR /app
COPY sentiment_service.py /app/

# Security/perf niceties
ENV PYTHONUNBUFFERED=1 \
    UVICORN_WORKERS=2 \
    PORT=5002

EXPOSE 5002
CMD ["uvicorn","sentiment_service:app","--host","0.0.0.0","--port","5002","--workers","2"]
