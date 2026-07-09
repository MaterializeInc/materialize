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

# The `materialize` user (uid/gid 999), built here because the distroless final
# image has no shell to run `useradd`. uid 999 matches the orchestrator's
# hardcoded runAsUser/fsGroup, so no UID gate is needed.
FROM debian:13-slim AS useradd
RUN groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize

# `debug` variant of the same distroless build id as the plain `nonroot` pin. It
# adds a busybox shell (`/busybox/sh`), kept intentionally so entrypoint.sh and
# `kubectl exec` keep working. A later step can drop to the shell-less variant.
FROM gcr.io/distroless/cc-debian13:debug-nonroot-28078d2e5e77671d2046dcc9e2c75334e31efa4d

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
# NOTE: this tag is coupled to two other pins and all three must move
# together: the same COPY in `ubuntu-base` (both prod bases must ship an
# identical libfdb_c.so, since Ubuntu and distroless environmentd/clusterd
# share one FoundationDB cluster during a rolling upgrade) and the
# `foundationdb` crate's `fdb-7_3` API feature in the root Cargo.toml. A
# dependabot bump here that is not matched in the other two is a bug, not a
# routine update.
COPY --from=foundationdb/foundationdb:7.3.71 /usr/lib/libfdb_c.so /usr/lib/

# Replace the base image's glibc example nsswitch.conf, whose `compat`
# backend segfaults the statically-linked `ssh` (see comments in the file).
COPY nsswitch.conf /etc/nsswitch.conf

# Run as `materialize` (uid/gid 999). The static `ssh` resolves the uid via the
# `files` nsswitch backend against the copied /etc/passwd.
COPY --from=useradd /etc/passwd /etc/passwd
COPY --from=useradd /etc/group /etc/group
COPY --from=useradd --chown=999:999 /home/materialize /home/materialize

USER materialize
ENV HOME=/home/materialize
ENTRYPOINT []
