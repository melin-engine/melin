# syntax=docker/dockerfile:1
# Multi-stage build for the melin all-in-one image.
#
# Contains: melin-server, melin-oe-gateway, melin-md-gateway, melin-keygen.
# Does NOT include melin-tui-fix-client — run that on the host.
#
# Build (requires SSH agent for private git deps):
#   docker build --ssh default -t melin .
#
# Run:
#   docker run --rm -p 9000:9000 -p 9001:9001 melin
#
# Connect TUI (from host):
#   cargo run -p melin-tui-fix-client -- \
#     --oe-addr localhost:9000 --md-addr localhost:9001 \
#     --sender TRADER --oe-target MELIN-OE --md-target MELIN-MD

# --- Builder stage ---
FROM rust:1.88-bookworm AS builder

# Install git + openssh for private GitHub dependencies (astenn, fastcp).
# nasm is required for blake3 to compile its AVX-512 compress_in_place
# assembly stubs (blake3_avx512_ffi); without it blake3 falls back to the
# SSE4.1 path even on AVX-512-capable CPUs (e.g., EPYC 9255 / Zen 4).
RUN apt-get update && apt-get install -y --no-install-recommends \
    git openssh-client nasm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for dependency caching.
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY crates crates

# Remove target-cpu=native — the Docker build host may differ from the
# runtime host. Use a portable baseline instead.
RUN mkdir -p .cargo && printf '[build]\nrustflags = []\n' > .cargo/config.toml

# Build with SSH agent forwarding for private git deps (astenn, fastcp).
# Requires: docker build --ssh default .
RUN --mount=type=ssh \
    mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null && \
    cargo build --release \
    -p melin-server \
    -p melin-oe-gateway \
    -p melin-md-gateway \
    -p melin-admin

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries from builder.
COPY --from=builder /build/target/release/melin-server /usr/local/bin/
COPY --from=builder /build/target/release/melin-oe-gateway /usr/local/bin/
COPY --from=builder /build/target/release/melin-md-gateway /usr/local/bin/
COPY --from=builder /build/target/release/melin-keygen /usr/local/bin/

# Copy entrypoint.
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Persistent data (journal, keys, configs).
VOLUME /data
ENV DATA_DIR=/data

# FIX gateway ports.
EXPOSE 9000 9001

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
