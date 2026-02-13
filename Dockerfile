FROM python:3.12-slim-bullseye AS builder

WORKDIR /app

COPY requirements.txt ./

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev libssl-dev zlib1g-dev binutils \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY main.py file_seeker.py dsbulk_reader.py cassandra_row_inferno.py row_analyzer.py ./

RUN pyinstaller --onefile main.py

RUN apt-get remove -y gcc libffi-dev libssl-dev zlib1g-dev binutils \
    && apt-get autoremove -y --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ARG DSBULK_VERSION=1.11.0
ADD https://github.com/datastax/dsbulk/releases/download/${DSBULK_VERSION}/dsbulk-${DSBULK_VERSION}.tar.gz \
    /tmp/dsbulk.tar.gz

RUN tar -xzf /tmp/dsbulk.tar.gz \
    && mv dsbulk-${DSBULK_VERSION} /opt/dsbulk \
    && ln -s /opt/dsbulk/bin/dsbulk /usr/local/bin/dsbulk \
    && rm /tmp/dsbulk.tar.gz

COPY --from=builder /app/dist/main ./main

ENTRYPOINT ["/app/main", "--dry-run"]