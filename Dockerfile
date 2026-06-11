# ==============================================================================
# Base Stage
# ==============================================================================
FROM rust:slim-bookworm AS base

WORKDIR /usr/src/surreal-dbops

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-chef for dependency graph planning/cooking
RUN cargo install cargo-chef --locked

# ==============================================================================
# Planner Stage
# ==============================================================================
FROM base AS planner

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo chef prepare --recipe-path recipe.json

# ==============================================================================
# Cacher Stage
# ==============================================================================
FROM base AS cacher

COPY --from=planner /usr/src/surreal-dbops/recipe.json recipe.json

RUN cargo chef cook --recipe-path recipe.json --no-default-features --jobs 1

# ==============================================================================
# Builder Stage
# ==============================================================================
FROM cacher AS builder

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --no-default-features --jobs 1 && \
    cp target/debug/surreal-dbops /usr/src/surreal-dbops/surreal-dbops

# ==============================================================================
# Runtime Stage
# ==============================================================================
FROM gcr.io/distroless/cc-debian12:latest

# Copy binary from builder
COPY --from=builder /usr/src/surreal-dbops/surreal-dbops /usr/local/bin/surreal-dbops

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/surreal-dbops"]
