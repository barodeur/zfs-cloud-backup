FROM ubuntu:24.04

ARG BINARY=bin/zfs-cloud-backup

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

COPY ${BINARY} /usr/local/bin/zfs-cloud-backup
COPY tests/integration-test.sh /integration-test.sh
RUN chmod +x /integration-test.sh

CMD ["/integration-test.sh"]
