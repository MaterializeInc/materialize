# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build and run SQLsmith, a random query generator, ported for Materialize

MZFROM ubuntu-base

# Git dependencies
RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    libdbi-perl \
    libdbd-pg-perl \
    libparse-yapp-perl \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN perl -MCPAN -e 'install DBIx::MyParsePP'

RUN git clone --single-branch https://github.com/MaterializeInc/RQG.git \
    && cd RQG \
    && git checkout 4fe4e6f09f9599ce9f8ffd3f75d593b20518cc50 \
    && rm -rf .git

ENTRYPOINT ["/usr/bin/perl"]

WORKDIR RQG
