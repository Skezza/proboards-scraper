FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

VOLUME ["/data"]

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import json,urllib.request;data=json.load(urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3));assert data.get('status')=='ok'"

CMD ["python", "scraper.py", "worker", "run", "--db-path", "/data/archive.db", "--output-dir", "/data/output", "--export-dir", "/data/exports", "--lock-file", "/data/worker.lock", "--health-port", "8080", "--import-json-bootstrap", "--bootstrap-json-dir", "/data/output"]
