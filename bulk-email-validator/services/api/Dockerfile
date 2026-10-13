FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY src src
COPY migrations migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/email-validator-api /usr/local/bin/email-validator-api
COPY migrations /app/migrations
ENV RUST_LOG=info
EXPOSE 8080
CMD ["email-validator-api"]
