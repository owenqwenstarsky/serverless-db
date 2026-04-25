FROM rust:bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/serverless-db /usr/local/bin/serverless-db

ENV PORT=8080
ENV SERVERLESS_DB_DATA_DIR=/data

EXPOSE 8080

CMD ["serverless-db"]
