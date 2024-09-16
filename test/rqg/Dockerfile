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
    && git checkout bb026d4bb1a4e6d1fd70b9adc9d1f31e6ec76770

ENTRYPOINT ["/usr/bin/perl"]

WORKDIR RQG
