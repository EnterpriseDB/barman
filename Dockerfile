FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /src
RUN pip install --no-cache-dir --no-deps /src && \
    pip install --no-cache-dir boto3 python-dateutil python-snappy cramjam setuptools psycopg2-binary

FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/0xsend/barman"
LABEL org.opencontainers.image.description="Barman cloud tools with R2-compatible multipart uploads (snappy+gzip)"
LABEL org.opencontainers.image.licenses="GPL-3.0-or-later"

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/barman-cloud-* /usr/local/bin/

ENTRYPOINT ["barman-cloud-backup"]
