# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# `ca-certificates` depends on the `openssl` CLI, so install it in a separate
# stage and copy only the bundle into the final image.
FROM debian:13-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a AS ca-certs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM debian:13-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1
# Disable backtrace collection in Rust libraries, such as `anyhow`. Collecting
# backtraces at runtime can be expensive and is known to cause deadlocks
# (database-issues#9159).
ENV RUST_LIB_BACKTRACE=0

ARG BUILD_PROFILE=optimize

LABEL build.profile=$BUILD_PROFILE

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends eatmydata \
    && rm -rf /var/lib/apt/lists/*

# Rust binaries using the workspace reqwest verify TLS against the system trust
# store and fail to build an HTTP client if it is missing, even when they never
# make a TLS connection.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
