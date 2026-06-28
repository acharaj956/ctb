# Build context is the repo root (see docker-compose.yml) so we can copy `common`.
FROM python:3.11-slim

# gcc/cc is needed to compile the STA/LTA C kernel into libstalta.so.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY services/station-processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY services/station-processing/ /app/

# Compile the detector kernel; the Python wrapper falls back to NumPy if absent.
RUN cc -O2 -fPIC -shared -o libstalta.so stalta.c

CMD ["python", "-u", "processor.py"]
