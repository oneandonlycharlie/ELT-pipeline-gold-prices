# base image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# copy project
COPY . .

RUN adduser --disabled-password --uid 1001 appuser

# allow user access to create logs
RUN chown -R appuser:appuser /app

USER appuser

# ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestor.main", "--full"]