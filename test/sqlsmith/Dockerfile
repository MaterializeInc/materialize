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

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    cmake \
    make \
    g++ \
    libpqxx-dev \
    nlohmann-json3-dev

# Rebuild since SQLsmith repo might have changed
ADD https://api.github.com/repos/MaterializeInc/sqlsmith/git/refs/heads/master version.json

# Build SQLsmith
RUN git clone --single-branch --branch=master https://github.com/MaterializeInc/sqlsmith \
    && cd sqlsmith \
    && git checkout 85e887a38662b015a1d7147979e5ef3a241d24ba \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ . \
    && cmake --build . -j `nproc`

ENTRYPOINT ["sqlsmith/sqlsmith"]
