# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build the dbbench Go binary from https://github.com/MaterializeInc/dbbench
# We use the standard Materialize mzbuild base image.

MZFROM ubuntu-base

WORKDIR /workdir

# Install build dependencies for Go and Git
RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    golang-go \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

ENV CGO_ENABLED=0

# Pin to specific commit for reproducibility
# TODO: Fork it under the MaterializeInc organization (like e.g. sqlsmith).
ADD https://api.github.com/repos/MaterializeInc/dbbench/git/commits/10b2a0b5159f06945646fa1179bda4be51fe02b4 version.json

# Clone and build dbbench. Some forks place main under ./cmd/dbbench; handle both.
RUN git clone https://github.com/MaterializeInc/dbbench /workdir/dbbench \
    && cd /workdir/dbbench \
    && git checkout 10b2a0b5159f06945646fa1179bda4be51fe02b4 \
    && go mod download \
    && if [ -d cmd/dbbench ]; then \
         go build -trimpath -ldflags="-s -w" -o /usr/local/bin/dbbench ./cmd/dbbench; \
       else \
         go build -trimpath -ldflags="-s -w" -o /usr/local/bin/dbbench .; \
       fi \
    && cd /workdir \
    && rm -rf /workdir/dbbench

# (No default dbbench.ini; scenarios generate an INI at runtime.)

# Default command shows dbbench help if executed without arguments
ENTRYPOINT ["/usr/local/bin/dbbench"]
CMD ["--help"]
