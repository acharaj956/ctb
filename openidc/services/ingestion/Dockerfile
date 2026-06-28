# Build context is the repo root (see docker-compose.yml) so we can copy `common`.
FROM python:3.11-slim

WORKDIR /app
COPY services/ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY services/ingestion/ /app/

CMD ["python", "-u", "producer.py"]
