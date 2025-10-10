FROM python:3.11-slim

# Install required Python dependencies
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" httpx

WORKDIR /app
COPY ticker_discovery_service.py /app/

ENV PYTHONUNBUFFERED=1 \
    PORT=5005

EXPOSE 5005
CMD ["uvicorn", "ticker_discovery_service:app", "--host", "0.0.0.0", "--port", "5005"]
