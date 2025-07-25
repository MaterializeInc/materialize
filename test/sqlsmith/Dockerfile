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

WORKDIR /workdir

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    cmake \
    make \
    g++ \
    libpqxx-dev \
    nlohmann-json3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Rebuild since SQLsmith repo might have changed
ADD https://api.github.com/repos/MaterializeInc/sqlsmith/git/refs/heads/master version.json

# Build SQLsmith
RUN git clone --single-branch --branch=master https://github.com/MaterializeInc/sqlsmith \
    && cd sqlsmith \
    && git checkout 5b0448c3c8c9f87654302d12c893fe173f97cbb9 \
    && rm -rf .git \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ . \
    && cmake --build . -j `nproc`

ENTRYPOINT ["/usr/bin/eatmydata", "sqlsmith/sqlsmith"]
