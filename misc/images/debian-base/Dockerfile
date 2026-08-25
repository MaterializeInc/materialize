# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM debian:13-slim@sha256:3a39a0592364683e6bab97937b72cad5a8fa6dcbbee90edb3bb48c7f8e94f258

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

# FoundationDB client library, dynamically linked by the Rust binaries.
# NOTE: keep this reference identical to the COPY in distroless-prod-base and
# materialized-base, and to the `foundationdb` crate's `fdb-7_3` API feature in
# the root Cargo.toml. All three images share one FoundationDB cluster during a
# rolling upgrade, so a bump in one that is not matched in the others is a bug.
COPY --from=foundationdb/foundationdb:7.3.71@sha256:1c988226e25ecd953475d46aef11fa2133e9e5eefda67e0a463196a4caab2d5d /usr/lib/libfdb_c.so /usr/lib/
