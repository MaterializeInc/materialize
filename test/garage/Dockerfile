# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The upstream image is built FROM scratch, with no shell, so it cannot run
# the cluster setup that a fresh garage node needs before it serves S3
# requests. Take its statically linked binary and give it a shell.
FROM dxflrs/garage:v2.4.1 AS garage

MZFROM debian-base

COPY --from=garage /garage /usr/local/bin/garage
COPY garage.toml /etc/garage.toml
COPY entrypoint.sh /usr/local/bin/garage-entrypoint
RUN useradd -m garage \
    && mkdir -p /var/lib/garage/meta /var/lib/garage/data \
    && chown -R garage:garage /var/lib/garage

# Per-connection INFO lines from the RPC layer would otherwise log every
# healthcheck.
ENV RUST_LOG=garage=warn

USER garage
ENTRYPOINT ["garage-entrypoint"]
