# ==============================================================================
# Builder Stage
# ==============================================================================
FROM rust:slim-bookworm AS builder

WORKDIR /usr/src/surreal-dbops

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./

# Create dummy files to cache dependencies build
RUN mkdir src && echo "pub fn dummy() {}" > src/lib.rs && echo "fn main() {}" > src/main.rs
RUN cargo build --no-default-features --jobs 1
RUN rm -rf src

# Copy real source code
COPY src/ ./src/

# Build real binary
RUN touch src/lib.rs src/main.rs && cargo build --no-default-features --jobs 1

# ==============================================================================
# Runtime Stage
# ==============================================================================
FROM gcr.io/distroless/cc-debian12:latest

# Copy binary from builder
COPY --from=builder /usr/src/surreal-dbops/target/debug/surreal-dbops /usr/local/bin/surreal-dbops

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/surreal-dbops"]
