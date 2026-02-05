# Stage 1: Build the Rust binary
FROM rust:bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release

# Stage 2: Runtime with ZFS + age + mc
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      zfsutils-linux \
      kmod \
      e2fsprogs \
      age \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install MinIO client (mc) for bucket creation
RUN arch=$(dpkg --print-architecture) && \
    curl -fsSL "https://dl.min.io/client/mc/release/linux-${arch}/mc" -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

COPY --from=builder /build/target/release/zfs-cloud-backup /usr/local/bin/zfs-cloud-backup
COPY tests/integration-test.sh /integration-test.sh
RUN chmod +x /integration-test.sh

CMD ["/integration-test.sh"]
