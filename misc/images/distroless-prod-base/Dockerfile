# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a separate mzimage so that we don't have to re-install the apt things
# every time we get a CI builder with a cold cache.

# Extract libeatmydata from a Debian image for CI use. eatmydata disables
# fsync for faster test execution. Activated via LD_PRELOAD=libeatmydata.so
# set as a container environment variable (must be set before process start).
FROM debian:13-slim AS eatmydata
RUN apt-get update && apt-get install -y --no-install-recommends eatmydata \
    && rm -rf /var/lib/apt/lists/*

# Statically-linked `tini`, the PID 1 init for all distroless prod images.
# Distroless ships no init, so without this each leaf binary would run as PID 1
# and have to forward signals and reap zombies (e.g. ssh tunnel subprocesses)
# itself.
MZFROM tini-static AS tini

FROM gcr.io/distroless/cc-debian13:nonroot-28078d2e5e77671d2046dcc9e2c75334e31efa4d

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1
# Disable backtrace collection in Rust libraries, such as `anyhow`. Collecting
# backtraces at runtime can be expensive and is known to cause deadlocks
# (database-issues#9159).
ENV RUST_LIB_BACKTRACE=0

# Copy libeatmydata for CI performance optimization (no-op unless
# LD_PRELOAD=libeatmydata.so is set as a container env var).
COPY --from=eatmydata /usr/lib/*/libeatmydata.so /usr/lib/
COPY --from=tini /output/tini /usr/bin/tini

# FoundationDB client library, dynamically linked by environmentd and clusterd.
# NOTE: keep the version in sync with ubuntu-base.
COPY --from=foundationdb/foundationdb:7.3.71 /usr/lib/libfdb_c.so /usr/lib/

USER nonroot
ENV HOME=/home/nonroot
ENTRYPOINT []
