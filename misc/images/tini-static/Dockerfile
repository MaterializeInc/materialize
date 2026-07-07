# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build a statically-linked `tini` init binary. tini is the PID 1 we run in
# every production image: it forwards signals to the child and reaps orphaned
# zombies (e.g. ssh tunnel subprocesses). The static binary (~600KB, glibc)
# lets distroless images, which ship no init and no apt, keep using tini
# without a package manager. This replaces the apt-installed tini from the
# Debian-based prod-base.
#
# tini's CMake build emits both a dynamic `tini` and a static `tini-static`
# target. We keep only the static one.
#
# Usage:
#   docker build -t tini-static .
#   docker create --name extract tini-static
#   docker cp extract:/output/tini ./tini
#   docker rm extract

FROM ubuntu:noble-20260610 AS builder

ARG TINI_VERSION=v0.19.0

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/tini
RUN git clone --depth 1 --branch ${TINI_VERSION} https://github.com/krallin/tini.git . \
    && cmake . \
    && make tini-static \
    && strip tini-static

# Verify the binary is not dynamically linked and is functional.
RUN ! ldd tini-static 2>/dev/null \
    && ./tini-static --version

# Output stage: just the binary.
FROM scratch
COPY --from=builder /build/tini/tini-static /output/tini
