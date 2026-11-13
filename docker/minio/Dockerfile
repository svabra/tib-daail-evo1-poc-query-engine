FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# The former Docker Hub repositories are unavailable. Keep the existing test
# versions using upstream release assets and their published SHA-256 digests.
FROM runtime AS server
ARG TARGETARCH
RUN case "$TARGETARCH" in \
        amd64) checksum=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ;; \
        arm64) checksum=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ;; \
        *) echo "Unsupported MinIO architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --retry 3 \
        "https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-${TARGETARCH}.RELEASE.2025-09-07T16-13-09Z" \
        --output /usr/local/bin/minio \
    && printf '%s  /usr/local/bin/minio\n' "$checksum" | sha256sum --check - \
    && chmod +x /usr/local/bin/minio \
    && mkdir -p /usr/share/doc/minio \
    && curl --fail --location --retry 3 \
        https://raw.githubusercontent.com/minio/minio/RELEASE.2025-09-07T16-13-09Z/LICENSE \
        --output /usr/share/doc/minio/LICENSE
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]

FROM runtime AS client
ARG TARGETARCH
RUN case "$TARGETARCH" in \
        amd64) checksum=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891 ;; \
        arm64) checksum=14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c ;; \
        *) echo "Unsupported MinIO client architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --retry 3 \
        "https://github.com/minio/mc/releases/download/RELEASE.2025-08-13T08-35-41Z/mc.linux-${TARGETARCH}.RELEASE.2025-08-13T08-35-41Z" \
        --output /usr/local/bin/mc \
    && printf '%s  /usr/local/bin/mc\n' "$checksum" | sha256sum --check - \
    && chmod +x /usr/local/bin/mc \
    && mkdir -p /usr/share/doc/minio-client \
    && curl --fail --location --retry 3 \
        https://raw.githubusercontent.com/minio/mc/RELEASE.2025-08-13T08-35-41Z/LICENSE \
        --output /usr/share/doc/minio-client/LICENSE
ENTRYPOINT ["/usr/local/bin/mc"]
