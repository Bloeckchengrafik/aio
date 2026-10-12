FROM node:22-bookworm-slim AS web
WORKDIR /web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run check && npm run build

FROM rust:1.99-bookworm AS rust
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=rust /app/target/release/aio-agent /app/target/release/aio-dashboard /app/target/release/aio-probe /usr/local/bin/
COPY --from=web /web/dist /app/web
ENV AIO_WEB_DIR=/app/web
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/aio-agent"]
