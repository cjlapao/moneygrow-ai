FROM python:3.11-slim

# Install service dependencies
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" numpy pandas yfinance

WORKDIR /app
COPY stock_metrics_service.py /app/

ENV PYTHONUNBUFFERED=1 \
    PORT=5004

EXPOSE 5004
CMD ["uvicorn", "stock_metrics_service:app", "--host", "0.0.0.0", "--port", "5004"]
