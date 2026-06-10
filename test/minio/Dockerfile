# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM golang:1.24.0 as builder
COPY no_fsync.patch /
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git patch \
    && git clone --depth=1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git \
    && cd minio \
    && patch -p1 < /no_fsync.patch \
    && go build -o /out/minio .

MZFROM ubuntu-base
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
