# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:984d31d4bd6e71bb9469353d93e73c68f7b3c17142b2aec45f2a960489095b22 AS ca-certs

FROM golang:1.24.0 as builder
# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && for f in /etc/apt/sources.list.d/*; do sed 's|http://|https://|g' "$f" > "$f.tmp" && mv "$f.tmp" "$f"; done

COPY no_fsync.patch /
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git patch \
    && git clone --depth=1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git \
    && cd minio \
    && patch -p1 < /no_fsync.patch \
    && go build -o /out/minio .

MZFROM debian-base
COPY --from=builder /out/minio /usr/local/bin/minio
RUN useradd -m minio \
    && apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data \
    && chown -R minio:minio /data
WORKDIR /data
USER minio
