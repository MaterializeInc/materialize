# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# MinIO deleted `minio/mc` from Docker Hub and archived the source repository,
# so RELEASE.2025-08-13T08-35-41Z is the final release and there is no newer
# one to move to. Build it from source, as test/minio already does for the
# server, so CI does not depend on a third-party registry continuing to serve a
# dead project's images.

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:bdb634669ffbf44cdfef9495238b03ce33a02e9ef631fc718b7dd6d2531f4325 AS ca-certs

FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS builder
# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

# renovate: datasource=github-tags depName=minio/mc versioning=loose extractVersion=^RELEASE\.(?<version>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)$
ARG MC_RELEASE=2025-08-13T08-35-41Z

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git \
    && git clone --depth=1 --branch RELEASE.$MC_RELEASE https://github.com/minio/mc.git \
    && cd mc \
    && CGO_ENABLED=0 go build -trimpath -o /out/mc .
# Upstream's Makefile stamps a release version through ldflags; this build does
# not, so `ReleaseTag` stays `DEVELOPMENT.GOGET`. That is what we want in CI:
# `mc` only checks dl.min.io for updates when the tag starts with `RELEASE.`,
# so this build never makes that call. (`mc version enable`, which the tests
# use, sets bucket versioning and is unrelated to the binary's version.)

MZFROM debian-base
COPY --from=builder /out/mc /usr/local/bin/mc
ENTRYPOINT ["mc"]
