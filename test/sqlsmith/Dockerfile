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
RUN git clone https://github.com/def-/sqlsmith \
    && cd sqlsmith \
    && git checkout a0d1d30f157327aae0ad3316318484328a64e3c7 \
    && rm -rf .git \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ . \
    && cmake --build . -j `nproc`

ENTRYPOINT ["/usr/bin/eatmydata", "sqlsmith/sqlsmith"]
