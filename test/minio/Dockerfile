# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:bdb634669ffbf44cdfef9495238b03ce33a02e9ef631fc718b7dd6d2531f4325 AS ca-certs

FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS builder
# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

# renovate: datasource=github-tags depName=minio/minio versioning=loose extractVersion=^RELEASE\.(?<version>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)$
ARG MINIO_RELEASE=2025-10-15T17-29-55Z

COPY no_fsync.patch /
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git patch \
    && git clone --depth=1 --branch RELEASE.$MINIO_RELEASE https://github.com/minio/minio.git \
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
