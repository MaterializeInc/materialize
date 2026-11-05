# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The golang image ships git, so the build needs no apt.
FROM golang:1.27.3-trixie@sha256:d9fa6e7ba43678ee1e5d1617007db9a134612032a74e2d86b44a9f582f1564cc AS builder
COPY no_fsync.patch /
RUN git clone --depth=1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git \
    && cd minio \
    && git apply /no_fsync.patch \
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
