FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir requests

# Copy application
COPY api.py .

# Cloud Run expects PORT env var
ENV PORT=8080

CMD ["python", "api.py"]
