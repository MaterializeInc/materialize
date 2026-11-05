# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The CA bundle comes from this pinned image, so certificate updates arrive
# only through Dependabot's digest bumps of it. Only the bundle is copied, since
# the `ca-certificates` package depends on the `openssl` CLI.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:8b33d821344902366f6ac0fd2cabf4c39d5b98abe37ebc9eb8c35943f8ede923 AS ca-certs

FROM debian:13-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1
# Disable backtrace collection in Rust libraries, such as `anyhow`. Collecting
# backtraces at runtime can be expensive and is known to cause deadlocks
# (database-issues#9159).
ENV RUST_LIB_BACKTRACE=0

ARG BUILD_PROFILE=optimize

LABEL build.profile=$BUILD_PROFILE

# Rust binaries using the workspace reqwest verify TLS against the system trust
# store and fail to build an HTTP client if it is missing, even when they never
# make a TLS connection. apt needs it too, since it fetches over HTTPS only.
# Descendant images inherit both.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends eatmydata \
    && rm -rf /var/lib/apt/lists/*
