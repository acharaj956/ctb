# Build context is the repo root (see docker-compose.yml) so we can copy `common`.
FROM python:3.11-slim

WORKDIR /app
COPY services/dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY services/dashboard/ /app/

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
