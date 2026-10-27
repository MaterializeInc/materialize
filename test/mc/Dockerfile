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

FROM golang:1.24.0 AS builder
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git \
    && git clone --depth=1 --branch RELEASE.2025-08-13T08-35-41Z https://github.com/minio/mc.git \
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
