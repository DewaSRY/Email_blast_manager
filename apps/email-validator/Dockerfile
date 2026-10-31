FROM rust:bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrate ./migrate
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home validator \
    && mkdir -p /app/data/uploads \
    && chown -R validator:validator /app

WORKDIR /app
COPY --from=builder /app/target/release/email-validator /usr/local/bin/email-validator

USER validator
EXPOSE 3000
CMD ["email-validator"]