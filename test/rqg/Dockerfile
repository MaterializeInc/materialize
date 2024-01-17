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
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    libdbi-perl \
    libdbd-pg-perl \
    libparse-yapp-perl \
    postgresql-client

RUN perl -MCPAN -e 'install DBIx::MyParsePP'

# Rebuild if the RQG repo has changed
ADD https://api.github.com/repos/MaterializeInc/RQG/git/refs/heads/main version.json

RUN git clone --single-branch https://github.com/MaterializeInc/RQG.git \
    && cd RQG \
    && git checkout 69a0e7b910d2d0ee22e7f25edd16648d7ea4fcb4

ENTRYPOINT ["/usr/bin/perl"]

WORKDIR RQG
