FROM rust:bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=builder /app/target/release/serverless-db /usr/local/bin/serverless-db
COPY handler.py /app/handler.py

ENV SERVERLESS_DB_INTERNAL_PORT=8080
ENV SERVERLESS_DB_DATA_DIR=/data

CMD ["python", "-u", "/app/handler.py"]
